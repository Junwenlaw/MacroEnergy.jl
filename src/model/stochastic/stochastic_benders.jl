# ============================================================================
# Stochastic Benders decomposition: planning problem + decomposed system
# ============================================================================
#
# Key structural differences from multi-period Benders:
#
#   Multi-period                       Stochastic
#   ─────────────────────────────      ──────────────────────────────────────
#   Investment varies per period       Single investment decision (shared)
#   Period index = planning period     Period index repurposed as scenario id
#   discount_factor × opexmult         probability × opexmult (uniform across r)
#   vBudget[s][w] per period           vBudget[r][w] per scenario r
#   Σ_w vBudget[s][w] ≤ cap (per s)   Σ_r p_r · Σ_w vBudget_r[w] ≤ cap (expected)
#
# ============================================================================

"""
    generate_stochastic_planning_problem(sc::StochasticCase)
        -> (Model, scenario_to_subproblem_map::Dict{Int,Vector{Int}})

Build the Benders master (planning) problem for a stochastic case.

Investment structure (single period):
  - Capacity investment variables created ONCE from the first scenario's asset topology.
  - Age-based retirements and min/max capacity planning constraints added once.
  - Fixed costs (investment + OM) discounted at the case DiscountRate, single period.

Policy constraint structure (one set of budget vars per scenario per subperiod):
  - `add_linking_variables!` is called on each scenario's locations independently so
    that each policy node creates budget variables with unique names (the
    `period_index` field carries the scenario id).
  - Planning constraints are added via `_add_stochastic_policy_constraints!` which
    dispatches on `PolicyMode` ("per_realization" or "expected") and handles all
    PolicyConstraint types: CO2Cap, CO2Storage, RenewableShare.
  - CO2Price is an OperationConstraint with no linking variables; its cost is
    automatically probability-weighted in the objective.

Theta variables and objective:
  - Theta weighted by `p_r * opexmult` (uniform opexmult, probability varies by scenario).
  - All BendersCut modes (multi, single, group, group_disaggregated) are supported.
"""
function generate_stochastic_planning_problem(sc::StochasticCase)

    @info("Generating stochastic planning problem")
    start_time = time()

    inv_sys  = investment_system(sc)   # first scenario — shared topology
    settings = sc.settings
    benders_settings = settings.BendersSettings

    model = Model()
    @variable(model, vREF == 1)

    # ── 1. Investment variables (capacity expansion) — from shared topology ──────
    # Initialise cost accumulators that planning_model! appends to
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

    # Single-period fixed cost: discount_factor = 1 / (1+r)^0 = 1
    # (investment happens at the start of the single planning period)
    discount_rate   = settings.DiscountRate
    period_length   = first(settings.PeriodLengths)   # should be 1 for stochastic

    model[:eFixedCost] = model[:eInvestmentFixedCost] + model[:eOMFixedCost]

    # Required by MacroEnergySolvers.process_planning_sol to check for negative capacities
    model[:eAvailableCapacity] = get_available_capacity([inv_sys])

    @expression(model, eFixedCostByPeriod[s in 1:1], model[:eFixedCost])
    @expression(model, eInvestmentFixedCostByPeriod[s in 1:1], model[:eInvestmentFixedCost])
    @expression(model, eOMFixedCostByPeriod[s in 1:1], model[:eOMFixedCost])

    # ── 2. CO2 budget linking variables — one set per scenario ───────────────────
    # Each scenario's CO2 node carries period_index = scenario_id, so variable
    # base names are unique: vCO2CapConstraint_Budget_<node>_period<r>[w]
    #
    # For the FIRST scenario the budget vars were already created implicitly when
    # we called add_linking_variables!.(inv_sys.assets, ...) above? No — budget
    # vars live on nodes (locations), not on assets. We call them explicitly here.
    @info(" -- Adding CO2 budget linking variables per scenario")
    for sc_r in sc.scenarios
        add_linking_variables!.(sc_r.system.locations, Ref(model))
    end

    # ── 3. Policy planning constraints (CO2Cap, CO2Storage, RenewableShare) ──────
    @info(" -- Adding stochastic policy constraints (mode=$(sc.stochastic_settings.PolicyMode))")
    _add_stochastic_policy_constraints!(sc, model)

    # ── 4. Scenario ↔ subproblem mapping ─────────────────────────────────────────
    # get_period_to_subproblem_mapping uses period_index (= scenario_id) from TimeData
    scenario_to_subproblem_map, subproblem_indices =
        get_period_to_subproblem_mapping(map(s -> s.system, sc.scenarios))

    # ── 5. Theta variables and approximate variable cost ─────────────────────────
    # opexmult is uniform across scenarios (same period length, same discount rate)
    opexmult = sum(1.0 / (1.0 + discount_rate)^i for i in 1:period_length)

    R = [s.id for s in sc.scenarios]
    p = Dict(s.id => s.probability for s in sc.scenarios)

    cut_type = benders_settings[:BendersCut]

    if cut_type == "multi" || cut_type == "group_disaggregated"
        @variable(model, vTHETA[w in subproblem_indices] >= 0)
        @expression(model, eVariableCostByScenario[r in R],
            p[r] * opexmult *
            sum(vTHETA[w] for w in scenario_to_subproblem_map[r])
        )

    elseif cut_type == "single"
        @variable(model, vTHETA[r in R] >= 0)
        @expression(model, eVariableCostByScenario[r in R],
            p[r] * opexmult * vTHETA[r]
        )

    elseif cut_type == "group"
        K = benders_settings[:BendersNumGroups]
        @variable(model, vTHETA[r in R, g in 1:K] >= 0)
        @expression(model, eVariableCostByScenario[r in R],
            p[r] * opexmult * sum(vTHETA[r, g] for g in 1:K)
        )

    else
        error("BendersCut must be \"multi\", \"single\", \"group\", or " *
              "\"group_disaggregated\" in benders_settings.json. Got: \"$cut_type\"")
    end

    @expression(model, eApproximateVariableCost,
        sum(eVariableCostByScenario[r] for r in R))

    # ── 6. Valid inequalities (optional, reuse existing logic) ───────────────────
    vi_cfg = get(benders_settings, :ValidInequalities, nothing)
    if vi_cfg !== nothing && get(vi_cfg, :Enabled, false)
        _add_stochastic_valid_inequalities!(sc, model, vi_cfg, R)
    end

    # ── 7. Objective ─────────────────────────────────────────────────────────────
    @objective(model, Min, model[:eFixedCost] + model[:eApproximateVariableCost])

    @info("Stochastic planning problem generated in $(round(time()-start_time, digits=2)) s")

    return model, scenario_to_subproblem_map
end


"""
    _add_stochastic_policy_constraints!(sc::StochasticCase, model::Model)

Add planning-level policy constraints to the stochastic master problem.
Dispatches on `sc.stochastic_settings.PolicyMode`:

- `"per_realization"`: each scenario satisfies each constraint independently.
  Calls `planning_model!` on each scenario's locations (CO2Cap, CO2Storage) and
  `add_global_renewable_share_constraint!` per scenario (RenewableShare).

- `"expected"`: probability-weighted constraints coupling all scenarios.
  Constraint forms (per constraint type, per node):
  - CO2Cap/CO2Storage:   `Σ_r p_r · Σ_w vBudget_r[w]    ≤ rhs_policy(node, ct)`
  - RenewableShare:      `Σ_r p_r · Σ_w vVREBudget_r[w]  ≥ X · E[annual_demand]`

CO2Price has no linking variables and is always probability-weighted in the objective.
"""
function _add_stochastic_policy_constraints!(sc::StochasticCase, model::Model)
    mode = sc.stochastic_settings.PolicyMode
    if mode == "per_realization"
        for sc_r in sc.scenarios
            planning_model!.(sc_r.system.locations, Ref(model))
            add_global_renewable_share_constraint!(sc_r.system, model)
        end
        @info("   Per-realization policy constraints added")
    elseif mode == "expected"
        _add_expected_budget_constraint!(CO2CapConstraint,     sc, model)
        _add_expected_budget_constraint!(CO2StorageConstraint, sc, model)
        _add_expected_renewable_share_constraint!(sc, model)
        @info("   Expected policy constraints added")
    end
end

"""
    _add_expected_budget_constraint!(ct_type, sc, model)

Add a probability-weighted planning constraint for any `PolicyConstraint` type that
uses a `_Budget` linking variable (CO2Cap, CO2Storage):

    `Σ_r p_r · Σ_w vBudget_r[node][w] ≤ rhs_policy(node, ct_type)`  per node

The RHS is read from `rhs_policy(node, ct_type)` in `nodes.json`.
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

"""
    _add_expected_renewable_share_constraint!(sc, model)

Add the probability-weighted RenewableShare planning constraint:

    `Σ_r p_r · Σ_nodes Σ_w vVREBudget_r[n][w]  ≥  X · Σ_r p_r · annual_demand_r`

The share X is read from `rhs_policy(node, RenewableShareConstraint)` in `nodes.json`.
The RHS uses the expected (probability-weighted) annual demand across scenarios.
"""
function _add_expected_renewable_share_constraint!(sc::StochasticCase, model::Model)
    ct_type    = RenewableShareConstraint
    budget_key = Symbol(string(ct_type) * "_VREBudget")
    p = Dict(s.id => s.probability for s in sc.scenarios)

    total_vre_expr       = AffExpr(0.0)
    total_expected_demand = 0.0
    X = nothing
    found_any = false

    for sc_r in sc.scenarios
        for loc in sc_r.system.locations
            for node in _all_nodes(loc)
                node isa Node{Electricity} || continue
                _has_budget_vars(node, budget_key) || continue
                found_any = true
                X === nothing && (X = rhs_policy(node, ct_type))
                for var in node.policy_budgeting_vars[budget_key]
                    add_to_expression!(total_vre_expr, p[sc_r.id], var)
                end
                total_expected_demand += p[sc_r.id] * sum(
                    subperiod_weight(node, current_subperiod(node, t)) * demand(node, t)
                    for t in time_interval(node)
                )
            end
        end
    end

    !found_any && return
    @constraint(model, total_vre_expr >= X * total_expected_demand)
end

# Helper: check if a node has a given policy budget variable key
function _has_budget_vars(n, key::Symbol)
    hasproperty(n, :policy_budgeting_vars) || return false
    return haskey(n.policy_budgeting_vars, key)
end

# Helper: iterate all Node objects inside a location (handles both Node and Location)
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
    _add_stochastic_valid_inequalities!(sc, model, vi_cfg, R)

Add average-supply/demand valid inequality constraints across all scenarios.
The demand lower-bound uses the probability-weighted average across scenarios.
"""
function _add_stochastic_valid_inequalities!(sc, model, vi_cfg, R)
    commodities = Symbol[]
    for (c, cfg) in pairs(vi_cfg.Commodities)
        cfg.Enabled && push!(commodities, c)
    end
    isempty(commodities) && return

    @info "Adding stochastic Benders valid inequalities for: $commodities"

    disable_after = get(vi_cfg, :DisableAfterIter, 0)
    model.ext[:VI] = Dict{Symbol,Any}(:disable_after => disable_after,
                                      :cons => JuMP.ConstraintRef[])

    @variable(model, vUAVG[r in R, c in commodities] >= 0)

    for (r, sc_r) in zip(R, sc.scenarios)
        sys = sc_r.system
        for (c, cfg) in pairs(vi_cfg.Commodities)
            cfg.Enabled || continue
            capE = effective_capacity_expr_for_commodity(sys, c, cfg.Assets;
                                                          include_availability = true)
            con_ub = @constraint(model, vUAVG[r, c] <= capE)
            push!(model.ext[:VI][:cons], con_ub)

            avgD = average_demand_for_commodity(sys, c)
            frac = get(cfg, :DemandFraction, 1.0)
            if avgD > 0
                con_lb = @constraint(model, vUAVG[r, c] >= frac * avgD)
                push!(model.ext[:VI][:cons], con_lb)
            end
        end
    end
    @info "Stochastic VI constraints added (DisableAfterIter = $disable_after)"
end


# ── Decomposed system ─────────────────────────────────────────────────────────

"""
    generate_stochastic_decomposed_system(sc::StochasticCase)
        -> (system_decomp::Vector{System}, scenario_to_subproblem_map::Dict{Int,Vector{Int}})

Build the per-subproblem decomposed system vector for a stochastic case.

Each scenario's system is decomposed into per-subperiod systems via the existing
`generate_decomposed_system`. The global subproblem index is assigned sequentially
across scenarios (scenario 1 subperiods first, then scenario 2, etc.).

`scenario_to_subproblem_map[r]` gives the global subproblem indices for scenario r.
This is equivalent to the multi-period `period_to_subproblem_map` with scenarios
playing the role of periods.
"""
function generate_stochastic_decomposed_system(sc::StochasticCase)
    system_decomp = System[]
    scenario_to_subproblem_map = Dict{Int, Vector{Int}}()
    global_w = 0

    for sc_r in sc.scenarios
        # Reuse existing single-period decomposition — zero changes to that function
        sc_decomp = generate_decomposed_system([sc_r.system])

        indices_for_r = Int[]
        for sys in sc_decomp
            global_w += 1
            # Investment capacity variables are shared (created from inv_sys with period_index=1).
            # Reset edge/storage timedata period_index to 1 so that `add_linking_variables!`
            # in the subproblem generates names like vCAP_edgeid_period1 that match the
            # planning problem, regardless of which scenario this subproblem belongs to.
            _reset_investment_period_index!(sys)
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

Reset the `timedata.period_index` of all edges and storages in `system` to 1.

In stochastic Benders, investment capacity variables are created once from the first
scenario's system (period_index=1), named e.g. `vCAP_edgeid_period1`.  Subproblem
systems for scenarios 2, 3, ... inherit period_index=scenario_id from `set_scenario_id!`,
which would generate mismatched names `vCAP_edgeid_period2`, etc.  This function corrects
that so all subproblems reference the same shared investment variable names.

Node timedata (used for budget variable naming) is left unchanged so that budget variables
remain scenario-specific (`vCO2CapConstraint_Budget_nodeid_period{scenario_id}[w]`).
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
            # Give the edge/storage its own private TimeData copy so that resetting
            # period_index to 1 does NOT also reset the co-located node's period_index
            # (edges and nodes share the same system.time_data[commodity] object).
            private_td = deepcopy(val.timedata)
            private_td.period_index = 1
            val.timedata = private_td
        end
    end
end


# ── Planning problem initializer (mirrors initialize_planning_problem!) ────────

"""
    initialize_stochastic_planning_problem!(sc::StochasticCase, opt::Dict)
        -> (planning_problem::Model, scenario_to_subproblem_map::Dict{Int,Vector{Int}})

Generate and configure the stochastic Benders planning problem with optimizer.
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
