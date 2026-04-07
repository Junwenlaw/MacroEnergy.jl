function generate_decomposed_system(periods_full::Vector{System})
    
    number_of_subperiods = sum(length(system.time_data[:Electricity].subperiods) for system in periods_full);

    system_decomp = Vector{System}(undef,number_of_subperiods)
    subperiod_count = 0;

    for system in periods_full
        period_index = system.time_data[:Electricity].period_index;
        number_of_subperiods_per_period = length(system.time_data[:Electricity].subperiods);
        for i in 1:number_of_subperiods_per_period
            subperiod_count = subperiod_count + 1;
            system_decomp[subperiod_count] = deepcopy(system)
            w = system.time_data[:Electricity].subperiod_indices[i];
            subperiod_w = system.time_data[:Electricity].subperiods[i];
            weight_w = system.time_data[:Electricity].subperiod_weights[w];
            subperiod_map = system.time_data[:Electricity].subperiod_map;
            modeled_subperiods_all = collect(keys(subperiod_map));
            for c in keys(system.time_data)
                system_decomp[subperiod_count].time_data[c].time_interval = subperiod_w
                system_decomp[subperiod_count].time_data[c].subperiod_weights = Dict(w => weight_w)
                system_decomp[subperiod_count].time_data[c].subperiods = [subperiod_w]
                system_decomp[subperiod_count].time_data[c].subperiod_indices = [w]
                system_decomp[subperiod_count].time_data[c].period_index = period_index
                modeled_subperiods = modeled_subperiods_all[findall(subperiod_map[x]==w for x in modeled_subperiods_all)] 
                system_decomp[subperiod_count].time_data[c].subperiod_map = Dict(n => w for n in modeled_subperiods) 
            end
        end
    end


    return system_decomp
end

function get_period_to_subproblem_mapping(periods::Vector{System})
    period_to_subproblem_map = Dict{Int64,Vector{Int64}}()
    subperiod_count = 0;
    for system in periods
        period_index = system.time_data[:Electricity].period_index;
        number_of_subperiods_per_period = length(system.time_data[:Electricity].subperiods);       
        for i in 1:number_of_subperiods_per_period
            subperiod_count = subperiod_count + 1; 
            if haskey(period_to_subproblem_map, period_index)
                push!(period_to_subproblem_map[period_index], subperiod_count)
            else
                period_to_subproblem_map[period_index] = [subperiod_count]
            end
        end
    end
    return period_to_subproblem_map, collect(1:subperiod_count)
    
end

function start_distributed_processes!(number_of_processes::Int64,case_path::AbstractString)

    # rmprocs.(workers())

    if haskey(ENV,"SLURM_NTASKS")
        ntasks = min(number_of_processes,parse(Int, ENV["SLURM_NTASKS"]));
        cpus_per_task = parse(Int, ENV["SLURM_CPUS_PER_TASK"]);
        addprocs(ClusterManagers.SlurmManager(ntasks);exeflags=["-t $cpus_per_task"])
    else
        ntasks = min(number_of_processes,Sys.CPU_THREADS)
        cpus_per_task = 1;
        addprocs(ntasks)
    end

    project = Pkg.project().path

    @sync for p in workers()
        @async create_worker_process(p,project,case_path) # add a check
    end

    @info("Number of procs: $(nprocs())")
    @info("Number of workers: $(nworkers())")
end

function solver_available(solver_name::Symbol)::Bool
    return isdefined(Main, solver_name)
end

function create_worker_process(pid,project,case_path::AbstractString)

    Distributed.remotecall_eval(Main, pid,:(using Pkg))

    Distributed.remotecall_eval(Main, pid,:(Pkg.activate($(project))))

    Distributed.remotecall_eval(Main, pid, :(using MacroEnergy))

    optional_solvers = [:Gurobi,]
    for solver in optional_solvers
        if solver_available(solver)
            Distributed.remotecall_eval(Main, pid, :(using $solver))
            @debug("Loaded $solver on worker $pid")
        end
    end

    Distributed.remotecall_eval(Main, pid, :(using MacroEnergySolvers))

    additions_path = user_additions_module_path(case_path)
    Distributed.remotecall_eval(MacroEnergy, pid, :(MacroEnergy.load_user_additions($additions_path)))

end


## Utility functions for MP valid inequalities
"""
Return asset type Symbols enabled (=1) in the valid inequalities settings for a commodity.
"""
function enabled_asset_types(asset_cfg)::Vector{Symbol}
    enabled = Symbol[]
    for (a, flag) in pairs(asset_cfg)
        flag == 1 && push!(enabled, a)
    end
    return enabled
end

"""
Compute average demand for a commodity in a given system (period).
Works with full-year or TDR because it uses the model's time representation.
"""
function average_demand_for_commodity(system::System,
                                      commodity::Symbol)

    ctype = commodity_types()[commodity]

    # Collect all nodes of this commodity
    nodes = Node[]
    for obj in get_nodes(system)
        if obj isa Node && commodity_type(obj) == ctype
            push!(nodes, obj)
        elseif obj isa Location
            for n in values(obj.nodes)
                commodity_type(n) == ctype && push!(nodes, n)
            end
        end
    end

    isempty(nodes) && return 0.0

    total = 0.0
    hours = 0.0

    # Use one reference node for the time axis
    ref = nodes[1]

    for t in time_interval(ref)
        w  = current_subperiod(ref, t)
        wt = subperiod_weight(ref, w)

        system_demand_t = 0.0
        for n in nodes
            system_demand_t += demand(n, t)
        end

        total += wt * system_demand_t
        hours += wt
    end

    avgD = hours > 0 ? total / hours : 0.0

    return avgD
end


function average_availability(e::AbstractEdge)::Float64
    total = 0.0
    hours = 0.0

    # If somehow no time steps exist, treat as always available
    ts = collect(time_interval(e))
    isempty(ts) && return 1.0

    for t in ts
        w  = current_subperiod(e, t)
        wt = subperiod_weight(e, w)
        total += wt * availability(e, t)
        hours += wt
    end

    return hours > 0 ? total / hours : 1.0
end


function effective_capacity_expr_for_commodity(
    system::System,
    commodity::Symbol,
    asset_cfg;
    include_availability::Bool = true,
)

    asset_types = enabled_asset_types(asset_cfg)
    isempty(asset_types) && return AffExpr(0.0)

    edges, edge_asset_map =
        edges_with_capacity_variables(system.assets; return_ids_map=true)

    (found_comm, _) = search_commodities(
        string(commodity),
        string.(unique(MacroEnergy.commodity_type.(edges)))
    )
    filter_edges_by_commodity!(edges, found_comm, edge_asset_map)

    available_types = string.(unique(get_type(asset) for asset in values(edge_asset_map)))
    (found_assets, _) = search_assets(string.(asset_types), available_types)
    filter_edges_by_asset_type!(edges, found_assets, edge_asset_map)

    expr = AffExpr(0.0)
    for e in edges
        α = include_availability ? average_availability(e) : 1.0
        expr += α * capacity(e)
    end

    return expr
end


