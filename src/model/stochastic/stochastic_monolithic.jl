# ============================================================================
# Stochastic monolithic (extensive form) model
# ============================================================================

"""
    generate_stochastic_model(sc::StochasticCase) -> Model

Build the monolithic (extensive form) JuMP model for a stochastic capacity
expansion problem.

## Mathematical structure

```
min  C_invest(x) + Σ_r p_r · opexmult · C_op(x, ξ_r)
s.t. planning constraints  (shared investment x)
     operational constraints per scenario r
     CO2 cap per scenario (per_realization) or in expectation (expected mode)
```

## Implementation note

Investment variables (`vCAP`, `vNEWUNIT`, `vRETUNIT`, …) are created once
from the first scenario's asset topology (`investment_system(sc)`).  Before
each scenario's `operation_model!` is called, the shared capacity
`VariableRef` / `AffExpr` objects are copied into that scenario's asset
objects so that capacity constraints in the operational model reference the
correct (shared) JuMP variables.  Flow variables (`vFLOW_…_period{r}`) are
created fresh per scenario and are therefore unique across scenarios.
"""
function generate_stochastic_model(sc::StochasticCase)

    @info("Generating stochastic monolithic model")
    start_time = time()

    inv_sys  = investment_system(sc)   # first scenario — shared investment topology
    settings = sc.settings

    model = Model()
    @variable(model, vREF == 1)

    # ── 1. Investment variables (capacity expansion) — from shared topology ──
    model[:eInvestmentFixedCost] = AffExpr(0.0)
    model[:eOMFixedCost]         = AffExpr(0.0)

    @info(" -- Adding investment linking variables")
    add_linking_variables!.(inv_sys.assets, Ref(model))

    @info(" -- Defining available capacity")
    define_available_capacity!(inv_sys, model)

    @info(" -- Generating investment planning model (assets)")
    planning_model!.(inv_sys.assets, Ref(model))
    add_age_based_retirements!.(inv_sys.assets, model)
    add_constraints_by_type!(inv_sys, model, PlanningConstraint)

    discount_rate = settings.DiscountRate
    period_length = first(settings.PeriodLengths)
    opexmult      = sum(1.0 / (1.0 + discount_rate)^i for i in 1:period_length)

    model[:eFixedCost] = model[:eInvestmentFixedCost] + model[:eOMFixedCost]

    @expression(model, eFixedCostByPeriod[s in 1:1],           model[:eFixedCost])
    @expression(model, eInvestmentFixedCostByPeriod[s in 1:1], model[:eInvestmentFixedCost])
    @expression(model, eOMFixedCostByPeriod[s in 1:1],         model[:eOMFixedCost])

    # ── 2. CO2 budget linking variables — one set per scenario ───────────────
    # period_index = scenario_id ensures unique JuMP variable names.
    @info(" -- Adding CO2 budget linking variables per scenario")
    for sc_r in sc.scenarios
        add_linking_variables!.(sc_r.system.locations, Ref(model))
    end

    # ── 3. Policy planning constraints ───────────────────────────────────────
    # PolicyMode applies to all PolicyConstraint types with linking variables:
    # CO2Cap, CO2Storage (budget vars), RenewableShare (VRE budget vars).
    # CO2Price is a cost-only OperationConstraint and needs no planning constraint.
    mode = sc.stochastic_settings.PolicyMode
    if mode == "per_realization"
        @info(" -- Adding per-realization policy constraints")
        for sc_r in sc.scenarios
            planning_model!.(sc_r.system.locations, Ref(model))
            add_global_renewable_share_constraint!(sc_r.system, model)
        end
    elseif mode == "expected"
        @info(" -- Adding expected policy constraints")
        _add_stochastic_policy_constraints!(sc, model)
    end

    # ── 4. Operational variables and constraints — one set per scenario ──────
    R = [s.id for s in sc.scenarios]
    p = Dict(s.id => s.probability for s in sc.scenarios)

    variable_cost  = Dict{Int, AffExpr}()
    co2_price_cost = Dict{Int, AffExpr}()

    for sc_r in sc.scenarios
        @info(" -- Scenario $(sc_r.id): sharing investment capacity with scenario assets")
        _share_capacity_with_scenario!(sc_r.system, inv_sys)

        @info(" -- Scenario $(sc_r.id): initializing VRE balance data")
        initialize_vre_balance_data!(sc_r.system)

        @info(" -- Scenario $(sc_r.id): generating operational model")
        model[:eVariableCost]  = AffExpr(0.0)
        model[:eCO2PriceCost]  = AffExpr(0.0)

        operation_model!(sc_r.system, model)

        # CO2 price merges into variable cost for the objective
        add_to_expression!(model[:eVariableCost], model[:eCO2PriceCost])

        variable_cost[sc_r.id]  = model[:eVariableCost]
        co2_price_cost[sc_r.id] = model[:eCO2PriceCost]
        unregister(model, :eVariableCost)
        unregister(model, :eCO2PriceCost)
    end

    @expression(model, eVariableCostByScenario[r in R],
        p[r] * opexmult * variable_cost[r])
    @expression(model, eVariableCost,
        sum(eVariableCostByScenario[r] for r in R))

    # Per-scenario raw (unweighted) costs — registered for output writing
    @expression(model, eRawVariableCostByScenario[r in R],  variable_cost[r])
    @expression(model, eRawCO2PriceCostByScenario[r in R],  co2_price_cost[r])
    @expression(model, eExpectedCO2PriceCost,
        sum(p[r] * opexmult * co2_price_cost[r] for r in R))

    # Store scenario metadata in model.ext for output writing
    model.ext[:scenario_ids]    = R
    model.ext[:scenario_probs]  = p
    model.ext[:opexmult]        = opexmult

    # ── 5. Objective ─────────────────────────────────────────────────────────
    @objective(model, Min, model[:eFixedCost] + model[:eVariableCost])

    @info("Stochastic monolithic model generated in $(round(time()-start_time, digits=2)) s")

    return model
end


# ── Capacity sharing helpers ──────────────────────────────────────────────────

"""
    _share_capacity_with_scenario!(sc_system, inv_sys)

Copy JuMP capacity variable references (`VariableRef` / `AffExpr`) from the
investment system's asset edges and storages into the matching assets of the
scenario system.

Investment variables are created once (from `inv_sys`) but `operation_model!`
is called on each scenario's system.  Without this step scenario assets retain
the default `capacity = AffExpr(0.0)` and all capacity constraints become
trivially zero.

Matching is by asset `id`.  Assets present in the scenario but absent from
`inv_sys` are skipped.
"""
function _share_capacity_with_scenario!(sc_system::System, inv_sys::System)
    inv_map = Dict(id(a) => a for a in inv_sys.assets)
    for sc_asset in sc_system.assets
        inv_asset = get(inv_map, id(sc_asset), nothing)
        inv_asset === nothing && continue
        _copy_investment_fields!(sc_asset, inv_asset)
    end
end

function _copy_investment_fields!(sc_asset::AbstractAsset, inv_asset::AbstractAsset)
    for field in fieldnames(typeof(sc_asset))
        sc_val  = getfield(sc_asset, field)
        inv_val = getfield(inv_asset, field)
        if sc_val isa AbstractEdge && inv_val isa AbstractEdge
            _copy_edge_investment!(sc_val, inv_val)
        elseif sc_val isa AbstractStorage && inv_val isa AbstractStorage
            _copy_storage_investment!(sc_val, inv_val)
        end
    end
end

function _copy_edge_investment!(sc_e::AbstractEdge, inv_e::AbstractEdge)
    has_capacity(inv_e) || return
    sc_e.capacity               = inv_e.capacity
    sc_e.new_capacity           = inv_e.new_capacity
    sc_e.new_units              = inv_e.new_units
    sc_e.retired_capacity       = inv_e.retired_capacity
    sc_e.retired_units          = inv_e.retired_units
    sc_e.new_capacity_track     = inv_e.new_capacity_track
    sc_e.retired_capacity_track = inv_e.retired_capacity_track
    if can_retrofit(inv_e)
        sc_e.retrofitted_capacity       = inv_e.retrofitted_capacity
        sc_e.retrofitted_units          = inv_e.retrofitted_units
        sc_e.retrofitted_capacity_track = inv_e.retrofitted_capacity_track
    end
end

function _copy_storage_investment!(sc_g::AbstractStorage, inv_g::AbstractStorage)
    has_capacity(inv_g) || return
    sc_g.capacity               = inv_g.capacity
    sc_g.new_capacity           = inv_g.new_capacity
    sc_g.new_units              = inv_g.new_units
    sc_g.retired_capacity       = inv_g.retired_capacity
    sc_g.retired_units          = inv_g.retired_units
    sc_g.new_capacity_track     = inv_g.new_capacity_track
    sc_g.retired_capacity_track = inv_g.retired_capacity_track
end
