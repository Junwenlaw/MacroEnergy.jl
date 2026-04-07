
function write_benders_fixed_group_map(
    case_path::String,
    mysetup::Dict,
    num_periods::Int
)
    system_path = joinpath(case_path, "system")
    output_path = joinpath(system_path, mysetup["OutputFolder"])

    # --------------------------------------------------
    # 1. Read local (period-level) group maps
    # --------------------------------------------------
    local_group_map = Dict{Int, Dict{Int, Vector{Int}}}()

    for s in 1:num_periods
        fname = num_periods == 1 ?
            joinpath(output_path, "Period_map.csv") :
            joinpath(output_path, "Period_map_$(s).csv")

        df = CSV.read(fname, DataFrame)

        period_map = Dict{Int, Vector{Int}}()

        for row in eachrow(df)
            rep = row.Rep_Period_Index    # group ID (representative)
            idx = row.Period_Index        # local subproblem index

            if !haskey(period_map, rep)
                period_map[rep] = Int[]
            end
            push!(period_map[rep], idx)
        end

        # Sort group members for determinism
        for v in values(period_map)
            sort!(v)
        end

        local_group_map[s] = period_map
    end

    # --------------------------------------------------
    # 2. Infer W_per_period from the local map
    # --------------------------------------------------
    W_per_period = maximum(
        maximum(ws) for s in keys(local_group_map)
        for ws in values(local_group_map[s])
    )

    # Safety check: all indices must be local
    @assert all(
        all(w <= W_per_period for w in ws)
        for s in keys(local_group_map)
        for ws in values(local_group_map[s])
    ) "Group map contains inconsistent local indices"

    # Optional: ensure all periods use the same local index space
    ref_ws = sort(unique(vcat(values(local_group_map[1])...)))
    for s in keys(local_group_map)
        ws = sort(unique(vcat(values(local_group_map[s])...)))
        @assert ws == ref_ws "Local group map differs across periods (s = $s)"
    end

    # --------------------------------------------------
    # 3. Expand to global subproblem indices
    # --------------------------------------------------
    global_group_map = Dict{Int, Dict{Int, Vector{Int}}}()

    for s in sort(collect(keys(local_group_map)))
        offset = (s - 1) * W_per_period
        global_group_map[s] = Dict(
            g => [offset + w for w in ws]
            for (g, ws) in local_group_map[s]
        )
    end

    return global_group_map
end


function print_global_group_map(group_map::Dict{Int, Dict{Int, Vector{Int}}})
    println("\n========== Global Group Map Summary ==========")

    if isempty(group_map)
        println("(Group map is empty.)")
        return
    end

    for period in sort(collect(keys(group_map)))
        println("\nPeriod $period:")
        local_map = group_map[period]

        if isempty(local_map)
            println("  (No groups found)")
            continue
        end

        for rep in sort(collect(keys(local_map)))
            members = sort(local_map[rep])
            println("  Group $rep → Members: $(members)")
        end
    end

    println("\n========== End Global Group Map ==========\n")
end

