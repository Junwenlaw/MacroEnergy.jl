# ============================================================================
# Cross-scenario Time Domain Reduction for Stochastic Benders
# ============================================================================
#
# Clusters subperiods ACROSS all scenarios jointly so that a single set of
# K groups spans the full stochastic time-series space.  This is the TDR
# counterpart to `CrossScenarioGrouping=true` in the Benders settings.
#
# Pipeline (reuses existing TDR functions):
#   1. _load_and_normalize_scenario  — load + normalize one scenario's CSVs
#   2. pre_clustering_tdr_cross_scenario  — concatenate with global column labels
#   3. clustering_tdr  (existing)         — cluster the combined data
#   4. post_clustering_tdr  (existing)    — build PeriodMap with global indices
#   5. run_cross_scenario_tdr             — orchestrate 1–4 + write output
#   6. write_benders_fixed_group_map_cross_scenario  — read Period_map → fixed_group_map
#
# Notes:
#   • Normalization is per-scenario (each scenario's time series is independently
#     scaled to [0,1] or z-scored before concatenation).  This makes clustering
#     scale-invariant across scenarios — subperiods are compared by pattern shape.
#   • Extreme periods and ClusterSubperiodResults are not supported for
#     cross-scenario clustering (UseExtremePeriods is ignored).
#   • The output fixed_group_map has the structure required by
#     benders_fixed_group_disaggregated:  Dict{1 => Dict{g => [global_w...]}}
# ============================================================================


"""
    _load_and_normalize_scenario(system_path, mysetup; v=false)
        -> (ModifiedDataNormalized::DataFrame, W::Int)

Load time-series CSVs from `system_path`, normalize, and reshape into the
`ModifiedDataNormalized` format used by the TDR clustering pipeline:
  - Rows: stacked feature-timestep vector (length = TimestepsPerRepPeriod × n_features)
  - Columns: one column per subperiod, named "1", "2", …, "W"

Files read (plain names, no period suffix — each scenario has its own folder):
  - availability.csv  (if ClusterAvailability == 1)
  - demand.csv        (if ClusterDemand == 1)
  - fuel_prices.csv   (if ClusterFuelPrices == 1)
"""
function _load_and_normalize_scenario(system_path::String, mysetup::Dict; v::Bool=false)

    TimestepsPerRepPeriod = mysetup["TimestepsPerRepPeriod"]
    ScalingMethod         = mysetup["ScalingMethod"]

    availability = DataFrame()
    demand       = DataFrame()
    fuel_prices  = DataFrame()

    # ── Load CSVs (always plain names — each scenario has its own system folder) ──
    if mysetup["ClusterAvailability"] == 1
        p = joinpath(system_path, mysetup["AvailabilityFileName"] * ".csv")
        isfile(p) || error("Cross-scenario TDR: availability file not found: $p")
        availability = DataFrame(CSV.File(p))
        Nh = (nrow(availability) ÷ TimestepsPerRepPeriod) * TimestepsPerRepPeriod
        availability = availability[1:Nh, :]
        v && println("  Loaded availability $(size(availability)) from $p")
    end

    if mysetup["ClusterDemand"] == 1
        p = joinpath(system_path, mysetup["DemandFileName"] * ".csv")
        isfile(p) || error("Cross-scenario TDR: demand file not found: $p")
        demand = DataFrame(CSV.File(p))
        Nh = (nrow(demand) ÷ TimestepsPerRepPeriod) * TimestepsPerRepPeriod
        demand = demand[1:Nh, :]
        v && println("  Loaded demand $(size(demand)) from $p")
    end

    if mysetup["ClusterFuelPrices"] == 1
        p = joinpath(system_path, mysetup["FuelPricesFileName"] * ".csv")
        isfile(p) || error("Cross-scenario TDR: fuel prices file not found: $p")
        fuel_prices = DataFrame(CSV.File(p))
        Nh = (nrow(fuel_prices) ÷ TimestepsPerRepPeriod) * TimestepsPerRepPeriod
        fuel_prices = fuel_prices[1:Nh, :]
        v && println("  Loaded fuel_prices $(size(fuel_prices)) from $p")
    end

    drop_time_columns!(availability)
    drop_time_columns!(demand)
    drop_time_columns!(fuel_prices)

    dfs_to_combine = DataFrame[]
    mysetup["ClusterAvailability"] == 1  && push!(dfs_to_combine, availability)
    mysetup["ClusterDemand"] == 1        && push!(dfs_to_combine, demand)
    mysetup["ClusterFuelPrices"] == 1    && push!(dfs_to_combine, fuel_prices)

    isempty(dfs_to_combine) &&
        error("Cross-scenario TDR: no active time series. " *
              "Enable ClusterAvailability, ClusterDemand, or ClusterFuelPrices.")

    full_df = hcat(dfs_to_combine...)

    # ── Remove constant columns ──────────────────────────────────────────────
    non_const = [c for c in names(full_df) if std(skipmissing(full_df[!, c])) > 1e-6]
    df_filtered = full_df[:, non_const]
    ColNames = names(df_filtered)

    # Force Float64
    InputData = DataFrame(Dict(c => Float64.(df_filtered[!, c]) for c in ColNames))
    Nhours = nrow(InputData)

    # ── Per-scenario normalization ───────────────────────────────────────────
    if ScalingMethod == "S"
        normProfiles = [
            StatsBase.transform(
                fit(ZScoreTransform, InputData[!, c]; dims=1, center=true, scale=true),
                InputData[!, c])
            for c in ColNames]
    else   # default: min-max normalization
        normProfiles = [
            StatsBase.transform(
                fit(UnitRangeTransform, InputData[!, c]; dims=1, unit=true),
                InputData[!, c])
            for c in ColNames]
    end

    AnnualNorm = DataFrame(Dict(ColNames[i] => normProfiles[i] for i in eachindex(ColNames)))

    # ── Reshape: (Nhours × n_features) → (feature-timestep × W_subperiods) ─
    # Each column of ModifiedDataNormalized = one subperiod's stacked feature vector.
    W = Nhours ÷ TimestepsPerRepPeriod
    AnnualNorm[!, :Group] = (1:Nhours) .÷ (TimestepsPerRepPeriod + 0.0001) .+ 1
    cols_stacked = [
        stack(AnnualNorm[isequal.(AnnualNorm.Group, w), :], ColNames)[!, :value]
        for w in 1:W
    ]
    ModifiedDataNormalized = DataFrame(Dict(Symbol(i) => cols_stacked[i] for i in 1:W))

    v && println("  ModifiedDataNormalized shape: $(size(ModifiedDataNormalized))  (W=$W subperiods)")

    return ModifiedDataNormalized, W
end


"""
    pre_clustering_tdr_cross_scenario(scenario_system_paths, mysetup; v=false)
        -> (ClusteringInputDF, ModifiedDataNormalized, NClusters, ExtremeWksList)

Load and normalize all scenarios' time series, then concatenate into a single
`ModifiedDataNormalized` with globally-indexed columns.

Scenario s contributes subperiod columns offset so that global index =
local_index + (s-1) * W_per_scenario (all scenarios assumed same W).

Extreme periods are not supported for cross-scenario clustering
(UseExtremePeriods is silently ignored; ExtremeWksList = []).
"""
function pre_clustering_tdr_cross_scenario(
    scenario_system_paths::Vector{String},
    mysetup::Dict;
    v::Bool = false,
)
    all_modified = DataFrame[]
    global_offset = 0

    for (s_idx, sys_path) in enumerate(scenario_system_paths)
        @info "Cross-scenario TDR: loading scenario $s_idx from $sys_path"

        ModData_s, W_s = _load_and_normalize_scenario(sys_path, mysetup; v=v)

        # Rename local columns "1".."W_s" to global "offset+1".."offset+W_s"
        global_names = [string(global_offset + i) for i in 1:W_s]
        rename!(ModData_s, global_names)
        push!(all_modified, ModData_s)
        global_offset += W_s
    end

    W_total = global_offset
    @info "Cross-scenario TDR: $W_total total subproblems across $(length(scenario_system_paths)) scenarios"

    combined = hcat(all_modified...)
    NClusters = mysetup["NumberOfSubperiods"]

    if NClusters > W_total
        error("CrossScenarioGrouping: BendersNumGroups ($NClusters) exceeds total " *
              "subproblems across all scenarios ($W_total). Reduce BendersNumGroups.")
    end

    # Extreme periods not supported for cross-scenario — skip silently
    ExtremeWksList = Int[]

    return (combined, combined, NClusters, ExtremeWksList)
end


"""
    run_cross_scenario_tdr(output_path, scenario_system_paths, mysetup; v=false)
        -> period_map_path::String

Run the full cross-scenario TDR pipeline:
  1. Load + normalize all scenarios' time series (per-scenario normalization)
  2. Cluster subperiods jointly across all scenarios
  3. Write `Period_map_cross_scenario.csv` to `output_path`

Returns the path to the written Period_map CSV.

Arguments:
  - `output_path`            : directory where Period_map_cross_scenario.csv is written
  - `scenario_system_paths`  : vector of system folder paths, one per scenario
                               (e.g. ["scenarios/scenario_1/system", ...])
  - `mysetup`                : TDR settings dict; `NumberOfSubperiods` = K groups
"""
function run_cross_scenario_tdr(
    output_path::String,
    scenario_system_paths::Vector{String},
    mysetup::Dict;
    v::Bool = false,
)
    mkpath(output_path)

    # ── 1. Pre-clustering: load + normalize + concatenate ───────────────────
    (ClusteringInputDF, ModifiedDataNormalized, NClusters, ExtremeWksList) =
        pre_clustering_tdr_cross_scenario(scenario_system_paths, mysetup; v=v)

    # ── 2. Clustering ────────────────────────────────────────────────────────
    (A, M, W, autoencoder_time, cluster_time) =
        clustering_tdr(output_path, mysetup, ClusteringInputDF, NClusters;
                       period_idx=1, v=v)

    # ── 3. Post-clustering: build PeriodMap with global indices ──────────────
    (A, W, M, PeriodMap) =
        post_clustering_tdr(mysetup, ClusteringInputDF, NClusters, A, W, M,
                            ExtremeWksList, ModifiedDataNormalized; v=v)

    # ── 4. Write Period_map_cross_scenario.csv ───────────────────────────────
    period_map_path = joinpath(output_path, "Period_map_cross_scenario.csv")
    CSV.write(period_map_path, PeriodMap)
    @info "Cross-scenario Period_map written to $period_map_path"

    runtime_df = DataFrame(
        ClusterMethod            = [mysetup["ClusterMethod"]],
        Autoencoder_Training_Time = [autoencoder_time],
        Clustering_Time          = [cluster_time],
    )
    CSV.write(joinpath(output_path, "clustering_runtime_cross_scenario.csv"), runtime_df)

    return period_map_path
end


"""
    write_benders_fixed_group_map_cross_scenario(period_map_path) -> Dict

Read a cross-scenario `Period_map_cross_scenario.csv` (with globally-indexed
`Period_Index` and cluster `Rep_Period_Index` columns) and return a fixed group map
in the format expected by `benders_fixed_group_disaggregated`:

    Dict{1 => Dict{group_id => Vector{global_w_indices}}}

The outer key is always 1 (single pseudo-period spanning all scenarios).
"""
function write_benders_fixed_group_map_cross_scenario(period_map_path::String)
    isfile(period_map_path) ||
        error("Cross-scenario Period_map not found: $period_map_path")

    df = CSV.read(period_map_path, DataFrame)

    group_map = Dict{Int, Vector{Int}}()
    for row in eachrow(df)
        rep = row.Rep_Period_Index   # group id (1..K)
        idx = row.Period_Index       # global subproblem index (1..W_total)
        push!(get!(group_map, rep, Int[]), idx)
    end
    for v in values(group_map)
        sort!(v)
    end

    return Dict(1 => group_map)
end
