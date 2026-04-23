"""
    generate_stochastic_planning_problem(sc::StochasticCase)
        -> (model::Model, scenario_to_subproblem_map::Dict{Int,Vector{Int}})

Build the Benders master (planning) problem for a stochastic case.

Investment variables are created once from the first scenario's asset topology.
Policy budget variables (`vBudget`) are created per scenario via `add_linking_variables!`
on each scenario's locations; their names are unique because each scenario's
`period_index` was stamped with the scenario id by `set_scenario_id!`.

Policy constraints are dispatched via `_add_stochastic_policy_constraints!` according
to `PolicyMode`.

Theta variables approximate the weighted operational cost. They are weighted by
`p[r]` only — `opexmult` is already included in each subproblem's objective by
`generate_operation_subproblem`, so including it here would double-count.
Supported `BendersCut` modes: `"multi"` (one theta per subproblem) and `"single"`
(one theta per scenario).
"""
function generate_stochastic_planning_problem(sc::StochasticCase)

    @info("Generating stochastic planning problem")
    start_time = time()

    inv_sys          = investment_system(sc)
    settings         = sc.settings
    benders_settings = settings.BendersSettings

    model = Model()
    @variable(model, vREF == 1)

    model[:eInvestmentFixedCost] = AffExpr(0.0)
    model[:eOMFixedCost]         = AffExpr(0.0)

    @info(" -- Adding investment linking variables (assets)")
    add_linking_variables!.(inv_sys.assets, Ref(model))

    @info(" -- Defining available capacity")
    define_available_capacity!(inv_sys, model)

    @info(" -- Generating investment planning model (assets only)")
    planning_model!.(inv_sys.assets, Ref(model))
    add_age_based_retirements!.(inv_sys.assets, model)
    add_constraints_by_type!(inv_sys, model, PlanningConstraint)

    model[:eFixedCost] = model[:eInvestmentFixedCost] + model[:eOMFixedCost]

    model[:eAvailableCapacity] = get_available_capacity([inv_sys])

    @expression(model, eFixedCostByPeriod[s in 1:1],           model[:eFixedCost])
    @expression(model, eInvestmentFixedCostByPeriod[s in 1:1], model[:eInvestmentFixedCost])
    @expression(model, eOMFixedCostByPeriod[s in 1:1],         model[:eOMFixedCost])

    @info(" -- Adding policy budget linking variables per scenario")
    for sc_r in sc.scenarios
        add_linking_variables!.(sc_r.system.locations, Ref(model))
    end

    @info(" -- Adding stochastic policy constraints (mode=$(sc.stochastic_settings.PolicyMode))")
    _add_stochastic_policy_constraints!(sc, model)

    scenario_to_subproblem_map, subproblem_indices =
        get_period_to_subproblem_mapping(map(s -> s.system, sc.scenarios))

    R = [s.id for s in sc.scenarios]
    p = Dict(s.id => s.probability for s in sc.scenarios)

    cut_type = benders_settings[:BendersCut]

    if cut_type == "multi"
        @variable(model, vTHETA[w in subproblem_indices] >= 0)
        @expression(model, eVariableCostByScenario[r in R],
            p[r] * sum(vTHETA[w] for w in scenario_to_subproblem_map[r])
        )
    elseif cut_type == "single"
        @variable(model, vTHETA[r in R] >= 0)
        @expression(model, eVariableCostByScenario[r in R],
            p[r] * vTHETA[r]
        )
    else
        error("BendersCut must be \"multi\" or \"single\" for stochastic cases. " *
              "Got: \"$cut_type\"")
    end

    @expression(model, eApproximateVariableCost,
        sum(eVariableCostByScenario[r] for r in R))

    @objective(model, Min, model[:eFixedCost] + model[:eApproximateVariableCost])

    @info("Stochastic planning problem generated in $(round(time()-start_time, digits=2)) s")

    return model, scenario_to_subproblem_map
end


"""
    _add_stochastic_policy_constraints!(sc::StochasticCase, model::Model)

Add policy planning constraints to the master problem, dispatching on `PolicyMode`:

- `"per_realization"`: calls `planning_model!` on each scenario's locations so each
  scenario independently satisfies every policy constraint.
- `"expected"`: adds a single probability-weighted constraint per node for CO2Cap
  and CO2Storage via `_add_expected_budget_constraint!`.
"""
function _add_stochastic_policy_constraints!(sc::StochasticCase, model::Model)
    mode = sc.stochastic_settings.PolicyMode
    if mode == "per_realization"
        for sc_r in sc.scenarios
            planning_model!.(sc_r.system.locations, Ref(model))
        end
        @info("   Per-realization policy constraints added")
    elseif mode == "expected"
        _add_expected_budget_constraint!(CO2CapConstraint,     sc, model)
        _add_expected_budget_constraint!(CO2StorageConstraint, sc, model)
        @info("   Expected policy constraints added")
    end
end

"""
    _add_expected_budget_constraint!(ct_type, sc, model)

Add a probability-weighted planning constraint for a budget-linked policy type
(CO2Cap, CO2Storage):

    Σ_r p_r · Σ_w vBudget_r[node][w]  ≤  rhs_policy(node, ct_type)   per node
"""
function _add_expected_budget_constraint!(ct_type::DataType, sc::StochasticCase, model::Model)
    budget_key = Symbol(string(ct_type) * "_Budget")
    p = Dict(s.id => s.probability for s in sc.scenarios)

    budget_vars = Dict{Tuple{Int,Any}, AffExpr}()
    node_cap    = Dict{Any, Float64}()

    for sc_r in sc.scenarios
        for loc in sc_r.system.locations
            for node in _all_nodes(loc)
                _has_budget_vars(node, budget_key) || continue
                nid = id(node)
                if !haskey(node_cap, nid) && haskey(node.rhs_policy, ct_type)
                    node_cap[nid] = rhs_policy(node, ct_type)
                end
                expr = AffExpr(0.0)
                for var in node.policy_budgeting_vars[budget_key]
                    add_to_expression!(expr, var)
                end
                budget_vars[(sc_r.id, nid)] = expr
            end
        end
    end

    isempty(budget_vars) && return

    for (nid, cap) in node_cap
        @constraint(model,
            sum(p[r] * budget_vars[(r, nid)]
                for r in keys(p) if haskey(budget_vars, (r, nid))) <= cap
        )
    end
end

function _has_budget_vars(n, key::Symbol)
    hasproperty(n, :policy_budgeting_vars) || return false
    return haskey(n.policy_budgeting_vars, key)
end

function _all_nodes(loc)
    if loc isa Node
        return [loc]
    elseif hasproperty(loc, :nodes)
        return collect(values(loc.nodes))
    else
        return []
    end
end


"""
    generate_stochastic_decomposed_system(sc::StochasticCase)
        -> (system_decomp::Vector{System}, scenario_to_subproblem_map::Dict{Int,Vector{Int}})

Decompose each scenario's system into per-subperiod systems using the existing
`generate_decomposed_system`. Subproblem indices are assigned sequentially across
scenarios; `scenario_to_subproblem_map[r]` gives the global indices for scenario `r`.

Two `period_index` resets are applied after decomposition:

1. `_reset_investment_period_index!` — sets `period_index = 1` on every edge/storage
   so investment variable names match the planning problem (which always uses period 1).

2. System-level `time_data` reset — replaces `sys.time_data[c]` with a private copy
   at `period_index = 1` so `generate_operation_subproblem` can safely index
   `discount_factor[1]`. The node's `timedata` field (which shared the same object)
   is left unchanged so budget variable names remain scenario-specific.
"""
function generate_stochastic_decomposed_system(sc::StochasticCase)
    system_decomp = System[]
    scenario_to_subproblem_map = Dict{Int, Vector{Int}}()
    global_w = 0

    for sc_r in sc.scenarios
        sc_decomp = generate_decomposed_system([sc_r.system])

        indices_for_r = Int[]
        for sys in sc_decomp
            global_w += 1

            _reset_investment_period_index!(sys)

            for c in keys(sys.time_data)
                private_td = deepcopy(sys.time_data[c])
                private_td.period_index = 1
                sys.time_data[c] = private_td
            end

            push!(system_decomp, sys)
            push!(indices_for_r, global_w)
        end
        scenario_to_subproblem_map[sc_r.id] = indices_for_r
    end

    @info("Stochastic decomposed system: $(length(system_decomp)) total subproblems " *
          "across $(length(sc.scenarios)) scenarios")

    return system_decomp, scenario_to_subproblem_map
end

"""
    _reset_investment_period_index!(system::System)

Set `timedata.period_index = 1` on all edges and storages in the system.

Investment variables are created from scenario 1's system (period_index=1). Subproblem
systems for other scenarios inherit their scenario's period_index, which would generate
mismatched investment variable names. This corrects them without touching node timedata
(which must retain the scenario id for correct budget variable naming).
"""
function _reset_investment_period_index!(system::System)
    for asset in system.assets
        _reset_investment_period_index!(asset)
    end
end

function _reset_investment_period_index!(asset::AbstractAsset)
    for field in fieldnames(typeof(asset))
        val = getfield(asset, field)
        if val isa AbstractEdge || val isa AbstractStorage
            private_td = deepcopy(val.timedata)
            private_td.period_index = 1
            val.timedata = private_td
        end
    end
end


"""
    initialize_stochastic_planning_problem!(sc::StochasticCase, opt::Dict)
        -> (model::Model, scenario_to_subproblem_map::Dict{Int,Vector{Int}})

Generate and configure the stochastic Benders planning problem with its optimizer.
"""
function initialize_stochastic_planning_problem!(sc::StochasticCase, opt::Dict)
    planning_problem, scenario_to_subproblem_map =
        generate_stochastic_planning_problem(sc)

    optimizer = create_optimizer(opt[:solver], opt_env(opt[:solver]), opt[:attributes])
    set_optimizer(planning_problem, optimizer)
    set_silent(planning_problem)

    if investment_system(sc).settings.ConstraintScaling
        @info "Scaling stochastic planning problem constraints"
        scale_constraints!(planning_problem)
    end

    return planning_problem, scenario_to_subproblem_map
end
