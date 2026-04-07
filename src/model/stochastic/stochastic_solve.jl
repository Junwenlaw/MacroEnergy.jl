# ============================================================================
# solve_case dispatch for StochasticCase
# ============================================================================

"""
    solve_case(sc::StochasticCase, opt, algorithm)

Top-level dispatch for solving a stochastic capacity expansion case.
"""
function solve_case(sc::StochasticCase, opt::O) where O <: Union{Optimizer, Dict{Symbol, Dict{Symbol, Any}}}
    solve_case(sc, opt, solution_algorithm(sc))
end

# ── Monolithic ────────────────────────────────────────────────────────────────

function solve_case(sc::StochasticCase, opt::Optimizer, ::Monolithic)
    @info("*** Running stochastic case with monolithic solver ***")
    model = generate_stochastic_model(sc)
    set_optimizer(model, opt)

    if investment_system(sc).settings.ConstraintScaling
        @info "Scaling constraints and RHS"
        scale_constraints!(model)
    end

    start_optimization = time()
    optimize!(model)
    cpu_optimization = time() - start_optimization

    return (sc, model, cpu_optimization)
end

# ── Benders ───────────────────────────────────────────────────────────────────

"""
    solve_case(sc::StochasticCase, opt::Dict, ::Benders; kwargs...)

Benders decomposition for a stochastic case.  The interface mirrors the
deterministic `solve_case(case::Case, opt, ::Benders)`:

- `opt[:planning]`    → optimizer settings for the master problem
- `opt[:subproblems]` → optimizer settings for the subproblems

The `scenario_to_subproblem_map` (scenario id → global subproblem indices) is
passed to the MacroEnergySolvers algorithm in place of `period_to_subproblem_map`.
All existing algorithm variants (multi, single, group, group_disaggregated,
fixed/adaptive) work without modification because they only depend on the map
structure, not on the period/scenario semantics.
"""
function solve_case(
    sc::StochasticCase,
    opt::Dict{Symbol, Dict{Symbol, Any}},
    ::Benders;
    case_path::Union{Nothing, String} = nothing,
    fixed_group_map::Union{Dict{Int, Dict{Int, Vector{Int}}}, Nothing} = nothing,
    v::Bool = false,
)
    @info("*** Running stochastic case with Benders decomposition ***")

    case_path === nothing &&
        error("solve_case (Stochastic Benders): `case_path` must be provided.")

    bd_setup = sc.settings.BendersSettings
    partial_subproblem_solve = Bool(get(bd_setup, :PartialSubproblemSolve, false))
    cut_trimming             = Bool(get(bd_setup, :CutTrimming, false))
    scale_CO2_dual           = Bool(get(bd_setup, :ScaleCO2Dual, false))
    cluster_co2_duals        = Bool(get(bd_setup, :ClusterOnlyCO2Duals, false))

    # ── Build decomposed subproblem systems ──────────────────────────────────
    system_decomp, scenario_to_subproblem_map =
        generate_stochastic_decomposed_system(sc)

    # ── Build master problem ─────────────────────────────────────────────────
    planning_problem, _ =
        initialize_stochastic_planning_problem!(sc, opt[:planning])

    # ── Build subproblems ────────────────────────────────────────────────────
    subproblems, linking_variables_sub =
        initialize_subproblems!(
            system_decomp,
            opt[:subproblems],
            bd_setup[:Distributed],
            bd_setup[:IncludeSubproblemSlacksAutomatically],
        )

    start_optimization = time()

    # ── Dispatch to MacroEnergySolvers algorithm ─────────────────────────────
    # scenario_to_subproblem_map is structurally identical to period_to_subproblem_map
    # so all existing solver variants work unchanged.
    results = _dispatch_benders_algorithm(
        planning_problem, subproblems, linking_variables_sub,
        bd_setup, scenario_to_subproblem_map;
        partial_subproblem_solve, cut_trimming, scale_CO2_dual,
        cluster_co2_duals, fixed_group_map, case_path, v,
    )

    cpu_optimization = time() - start_optimization

    # ── Write planning solution back into scenario systems ───────────────────
    _update_stochastic_with_planning_solution!(sc, results.planning_sol.values)

    return (sc, BendersResults(results, subproblems), cpu_optimization)
end

"""
    _dispatch_benders_algorithm(...)

Select and call the correct MacroEnergySolvers Benders variant.
Mirrors the logic in `solver.jl` `solve_case(case::Case, ..., ::Benders)`.
The `subproblem_map` argument is `scenario_to_subproblem_map` for stochastic
and `period_to_subproblem_map` for deterministic — both are `Dict{Int,Vector{Int}}`.
"""
function _dispatch_benders_algorithm(
    planning_problem, subproblems, linking_variables_sub,
    bd_setup, subproblem_map;
    partial_subproblem_solve, cut_trimming, scale_CO2_dual,
    cluster_co2_duals, fixed_group_map, case_path, v,
)
    cut  = bd_setup[:BendersCut]
    gtype = get(bd_setup, :GroupType, "fixed")
    cross_scenario = Bool(get(bd_setup, :CrossScenarioGrouping, false))

    # Cross-scenario grouping validity:
    # - group_disaggregated: fully supported (disaggregated theta vTHETA[w] per subproblem).
    # - multi + PartialSubproblemSolve: supported for medoid selection only — K global
    #   medoids are chosen across all scenarios; cuts remain per-subproblem (multi-cut).
    # - group: NOT supported. Aggregated theta vTHETA[scenario_id, g] is keyed by scenario;
    #   a cross-scenario outer key=1 would leave scenario 2+ theta variables without cuts.
    multi_partial = (cut == "multi" && partial_subproblem_solve)
    if cross_scenario && cut != "group_disaggregated" && !multi_partial
        error("CrossScenarioGrouping=true is only supported with " *
              "BendersCut=\"group_disaggregated\" (for grouped cuts) or " *
              "BendersCut=\"multi\" + PartialSubproblemSolve=true (for medoid selection). " *
              "Got BendersCut=$(repr(cut)). " *
              "Use one of these combinations or set CrossScenarioGrouping=false.")
    end

    # ── BendersNumGroups semantics (important distinction) ───────────────────
    # CrossScenarioGrouping=false (within-scenario):
    #   BendersNumGroups = K groups PER SCENARIO.
    #   Total cuts = K × number_of_scenarios.
    #   e.g. K=64, 2 scenarios → 128 total group cuts.
    #
    # CrossScenarioGrouping=true (cross-scenario, group_disaggregated only):
    #   BendersNumGroups = K groups TOTAL across all scenarios.
    #   e.g. K=64, 2 scenarios → 64 total group cuts.
    K = get(bd_setup, :BendersNumGroups, nothing)
    R = length(subproblem_map)
    if cut in ("group", "group_disaggregated") && K !== nothing
        if cross_scenario
            @info("BendersNumGroups=$K = TOTAL groups across all $R scenarios " *
                  "(cross-scenario grouping)")
        else
            @info("BendersNumGroups=$K = groups PER SCENARIO × $R scenarios " *
                  "= $(K*R) total group cuts (within-scenario grouping)")
        end
    elseif multi_partial && K !== nothing
        if cross_scenario
            @info("BendersNumGroups=$K = TOTAL medoids across all $R scenarios " *
                  "(cross-scenario medoid selection; cuts remain per-subproblem)")
        else
            @info("BendersNumGroups=$K = medoids PER SCENARIO × $R scenarios " *
                  "(within-scenario medoid selection)")
        end
    end

    # For stochastic cases, cross-scenario grouping flattens all subproblems from
    # all scenarios into a single pseudo-period so the clustering algorithm can
    # form groups spanning across scenarios.  Ignored for non-adaptive group types.
    effective_map = cross_scenario ? _cross_scenario_map(subproblem_map) : subproblem_map

    if cut == "multi" && !partial_subproblem_solve && !cut_trimming
        @info("Running Multi-cut Benders (stochastic)")
        return MacroEnergySolvers.benders_multi(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), subproblem_map;
            case_path = case_path, v = v)

    elseif cut == "multi" && !partial_subproblem_solve && cut_trimming
        @info("Running Multi-cut Benders with Cut Trimming (stochastic)")
        return MacroEnergySolvers.benders_multi_trimming(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), subproblem_map;
            case_path = case_path, v = v)

    elseif cut == "single"
        @info("Running Single-cut Benders (stochastic)")
        return MacroEnergySolvers.benders_single(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), subproblem_map;
            case_path = case_path, v = v)

    elseif cut == "group" && gtype == "fixed"
        @info("Running Fixed Group-cut Benders (stochastic)")
        return MacroEnergySolvers.benders_fixed_group(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), subproblem_map;
            fixed_group_map = fixed_group_map, case_path = case_path, v = v)

    elseif cut == "group_disaggregated" && gtype == "fixed"
        @info("Running Disaggregated Fixed Group-cut Benders (stochastic)")
        return MacroEnergySolvers.benders_fixed_group_disaggregated(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), subproblem_map;
            fixed_group_map = fixed_group_map, case_path = case_path, v = v)

    elseif cut == "group_disaggregated" && gtype == "adaptive" && !partial_subproblem_solve && cluster_co2_duals
        @info("Running Disaggregated Adaptive Group-cut Benders with CO2 Dual-Only Clustering (stochastic)")
        return MacroEnergySolvers.benders_adaptive_group_disaggregated_cluster_co2_duals(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), effective_map;
            get_flow_func = MacroEnergy.get_optimal_flow,
            reshape_wide_func = MacroEnergy.reshape_wide,
            case_path = case_path, v = v)

    elseif cut == "group_disaggregated" && gtype == "adaptive" && !partial_subproblem_solve && scale_CO2_dual
        @info("Running Disaggregated Adaptive Group-cut Benders with CO2 Dual Scaling (stochastic)")
        return MacroEnergySolvers.benders_adaptive_group_disaggregated_weight_co2_duals(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), effective_map;
            get_flow_func = MacroEnergy.get_optimal_flow,
            reshape_wide_func = MacroEnergy.reshape_wide,
            case_path = case_path, v = v)

    elseif cut == "group_disaggregated" && gtype == "adaptive" && !partial_subproblem_solve
        @info("Running Disaggregated Adaptive Group-cut Benders (stochastic)")
        return MacroEnergySolvers.benders_adaptive_group_disaggregated(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), effective_map;
            get_flow_func = MacroEnergy.get_optimal_flow,
            reshape_wide_func = MacroEnergy.reshape_wide,
            case_path = case_path, v = v)

    elseif cut == "group" && gtype == "adaptive" && !partial_subproblem_solve && cluster_co2_duals
        @info("Running Adaptive Group-cut Benders with CO2 Dual-Only Clustering (stochastic)")
        return MacroEnergySolvers.benders_adaptive_group_cluster_co2_duals(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), effective_map;
            get_flow_func = MacroEnergy.get_optimal_flow,
            reshape_wide_func = MacroEnergy.reshape_wide,
            case_path = case_path, v = v)

    elseif cut == "group" && gtype == "adaptive" && !partial_subproblem_solve && scale_CO2_dual
        @info("Running Adaptive Group-cut Benders with CO2 Dual Scaling (stochastic)")
        return MacroEnergySolvers.benders_adaptive_group_weight_co2_duals(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), effective_map;
            get_flow_func = MacroEnergy.get_optimal_flow,
            reshape_wide_func = MacroEnergy.reshape_wide,
            case_path = case_path, v = v)

    elseif cut == "group" && gtype == "adaptive" && !partial_subproblem_solve
        @info("Running Adaptive Group-cut Benders (stochastic)")
        return MacroEnergySolvers.benders_adaptive_group(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), effective_map;
            get_flow_func = MacroEnergy.get_optimal_flow,
            reshape_wide_func = MacroEnergy.reshape_wide,
            case_path = case_path, v = v)

    elseif cut == "multi" && partial_subproblem_solve
        @info("Running Multi-cut Benders with Partial Subproblem Solves (stochastic)" *
              (cross_scenario ? " [cross-scenario medoid selection]" : ""))
        return MacroEnergySolvers.benders_multi_partial_SP(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), effective_map;
            get_flow_func = MacroEnergy.get_optimal_flow,
            reshape_wide_func = MacroEnergy.reshape_wide,
            case_path = case_path, v = v)

    elseif cut == "group" && gtype == "adaptive" && partial_subproblem_solve && !scale_CO2_dual
        @info("Running Adaptive Group-cut Benders with Partial Subproblem Solves (stochastic)")
        return MacroEnergySolvers.benders_adaptive_group_partial_SP(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), effective_map;
            get_flow_func = MacroEnergy.get_optimal_flow,
            reshape_wide_func = MacroEnergy.reshape_wide,
            case_path = case_path, v = v)

    else
        error("Benders method unspecified — use multi, single, or group (fixed/adaptive) cuts. " *
              "Got BendersCut=$(repr(cut)), GroupType=$(repr(gtype))")
    end
end

"""
    _cross_scenario_map(scenario_to_subproblem_map) -> Dict{Int, Vector{Int}}

Flatten a `scenario_to_subproblem_map` into a single pseudo-period containing
all global subproblem indices across all scenarios.  This is passed to adaptive
group-cut algorithms so they cluster subproblems across scenarios rather than
within each scenario independently.

Example:
    {1 => [1,2,3], 2 => [4,5,6]}  →  {1 => [1,2,3,4,5,6]}
"""
function _cross_scenario_map(scenario_to_subproblem_map::Dict{Int, Vector{Int}})
    all_w = sort(collect(Iterators.flatten(values(scenario_to_subproblem_map))))
    return Dict(1 => all_w)
end

"""
    _update_stochastic_with_planning_solution!(sc, sol_values)

Write the Benders planning solution back into each scenario's asset structures,
mirroring `update_with_planning_solution!` for the deterministic case.
Since all scenarios share the same investment topology, the solution is applied
to each scenario's system.
"""
function _update_stochastic_with_planning_solution!(sc::StochasticCase, sol_values::Dict)
    inv_sys = investment_system(sc)
    # Pass 1: share VariableRefs from inv_sys to all other scenarios BEFORE any update.
    # update_with_planning_solution! mutates capacity from VariableRef → Float64 in-place,
    # so sharing must happen while inv_sys still holds VariableRefs.
    for sc_r in sc.scenarios
        sc_r.system !== inv_sys && _share_capacity_with_scenario!(sc_r.system, inv_sys)
    end
    # Pass 2: update all scenarios (now every scenario has VariableRefs that can be named).
    for sc_r in sc.scenarios
        update_with_planning_solution!(sc_r.system, sol_values)
    end
end
