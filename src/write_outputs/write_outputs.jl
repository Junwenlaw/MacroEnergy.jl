"""
Write results when using Monolithic as solution algorithm.
"""
function write_outputs(case_path::AbstractString, case::Case, model::Model)
    num_periods = number_of_periods(case)
    periods = get_periods(case)
    for (period_idx,period) in enumerate(periods)
        @info("Writing results for period $period_idx")
        
        create_discounted_cost_expressions!(model, period, get_settings(case))

        compute_undiscounted_costs!(model, period, get_settings(case))

        ## Create results directory to store the results
        if num_periods > 1
            # Create a directory for each period
            results_dir = joinpath(case_path, "results_period_$period_idx")
        else
            # Create a directory for the single period
            results_dir = joinpath(case_path, "results")
        end
        mkpath(results_dir)

        # Scaling factor for variable cost portion of objective function
        discount_scaling = compute_variable_cost_discount_scaling(period_idx, get_settings(case))

        write_outputs(results_dir, period, model, discount_scaling)
    end
    write_settings(case, joinpath(case_path, "settings.json"))
    return nothing
end

"""
Write results when using Myopic as solution algorithm. 
"""
function write_outputs(case_path::AbstractString, case::Case, myopic_results::MyopicResults)
    @debug("Outputs were already written during iteration.")
    return nothing
end

"""
Write results when using Benders as solution algorithm.
"""
function write_outputs(case_path::AbstractString, case::Case, bd_results::BendersResults)

    settings = get_settings(case);
    num_periods = number_of_periods(case);
    periods = get_periods(case);

    period_to_subproblem_map, _ = get_period_to_subproblem_mapping(periods)

    # get the flow results from the operational subproblems
    flow_df = collect_flow_results(case, bd_results)
    
    # get the policy slack variables from the operational subproblems
    slack_vars = collect_distributed_policy_slack_vars(bd_results)

    # get the constraint duals from the operational subproblems
    # for now, only balance constraints are exported
    balance_duals = collect_distributed_constraint_duals(bd_results, BalanceConstraint)

    for (period_idx, period) in enumerate(periods)
        @info("Writing results for period $period_idx")
        ## Create results directory to store the results
        if num_periods > 1
            # Create a directory for each period
            results_dir = joinpath(case_path, "results_period_$period_idx")
        else
            # Create a directory for the single period
            results_dir = joinpath(case_path, "results")
        end
        mkpath(results_dir)

        # subproblem indices for the current period
        subop_indices_period = period_to_subproblem_map[period_idx]

        # Note: period has been updated with the capacity values in planning_solution at the end of function solve_case
        # Capacity results
        write_capacity(joinpath(results_dir, "capacity.csv"), period)

        # Flow results
        write_flows(joinpath(results_dir, "flows.csv"), period, flow_df[subop_indices_period])
        
        # Cost results
        costs = prepare_costs_benders(period, bd_results, subop_indices_period, settings, period_to_subproblem_map)
        write_costs(joinpath(results_dir, "costs.csv"), period, costs)
        write_undiscounted_costs(joinpath(results_dir, "undiscounted_costs.csv"), period, costs)

        write_settings(case, joinpath(case_path, "settings.json"))

        # Write dual values (if enabled)
        if period.settings.DualExportsEnabled
            # Move slack variables from subproblems to planning problem
            if haskey(slack_vars, period_idx)
                populate_slack_vars_from_subproblems!(period, slack_vars[period_idx])
            else
                @debug "No slack variables found for period $period_idx"
            end
            
            # Calculate and store constraint duals from subproblems to planning problem
            if haskey(balance_duals, period_idx)
                populate_constraint_duals_from_subproblems!(period, balance_duals[period_idx], BalanceConstraint)
            else
                @debug "No balance constraint duals found for period $period_idx"
            end
            
            # Scaling factor to account for discounting in multi-period models
            discount_scaling = compute_variable_cost_discount_scaling(period_idx, settings)
            write_duals_benders(results_dir, period, discount_scaling)
        end
    end
    return nothing
end

"""
    write_outputs(case_path, sc::StochasticCase, model::Model)

Write results for a stochastic monolithic solve:
- `results/capacity.csv`              — shared investment capacity
- `results/scenario_{id}/flows.csv`   — per-scenario operational flows
- `results/costs.csv`                 — aggregate expected costs
- `results/scenario_costs.csv`        — per-scenario cost breakdown
- `settings.json`                     — case + system settings
"""
function write_outputs(case_path::AbstractString, sc::StochasticCase, model::Model)
    inv_sys = investment_system(sc)

    # Use the same output path logic as the non-stochastic case
    # (respects OverwriteResults and OutputDir settings → results_001, results_002, …)
    results_dir = create_output_path(inv_sys, case_path)

    # ── Capacity (shared investment decision) ──────────────────────────────
    write_capacity(joinpath(results_dir, "capacity.csv"), inv_sys)

    # ── Per-scenario flows ─────────────────────────────────────────────────
    for sc_r in sc.scenarios
        sc_dir = joinpath(results_dir, "scenario_$(sc_r.id)")
        mkpath(sc_dir)
        write_flow(joinpath(sc_dir, "flows.csv"), sc_r.system)
    end

    # ── Costs ──────────────────────────────────────────────────────────────
    _write_stochastic_costs(results_dir, model)

    # ── Settings ───────────────────────────────────────────────────────────
    write_settings(sc, joinpath(results_dir, "settings.json"))

    return results_dir
end

"""
    write_outputs(case_path, sc::StochasticCase, bd_results::BendersResults)

Write results for a stochastic Benders solve:
- `results/capacity.csv`              — shared investment capacity
- `results/scenario_{id}/flows.csv`   — per-scenario operational flows
- `results/costs.csv`                 — aggregate expected costs
- `results/scenario_costs.csv`        — per-scenario cost breakdown
- `settings.json`                     — case + system settings
"""
function write_outputs(case_path::AbstractString, sc::StochasticCase, bd_results::BendersResults)
    inv_sys  = investment_system(sc)
    settings = get_settings(sc)

    results_dir = create_output_path(inv_sys, case_path)

    # Reconstruct scenario → subproblem mapping (same logic as in stochastic_benders.jl)
    scenario_to_subproblem_map, _ =
        get_period_to_subproblem_mapping(map(s -> s.system, sc.scenarios))

    # Collect flow results from subproblems
    is_distributed = settings.BendersSettings[:Distributed]
    flow_df = is_distributed ? collect_distributed_flows(bd_results) : collect_local_flows(bd_results)

    # ── Capacity ───────────────────────────────────────────────────────────────
    write_capacity(joinpath(results_dir, "capacity.csv"), inv_sys)

    # ── Per-scenario flows ─────────────────────────────────────────────────────
    for sc_r in sc.scenarios
        sc_dir = joinpath(results_dir, "scenario_$(sc_r.id)")
        mkpath(sc_dir)
        subop_indices = scenario_to_subproblem_map[sc_r.id]
        write_flows(joinpath(sc_dir, "flows.csv"), sc_r.system, flow_df[subop_indices])
    end

    # ── Costs ──────────────────────────────────────────────────────────────────
    _write_stochastic_benders_costs(results_dir, sc, bd_results, scenario_to_subproblem_map, settings)

    # ── Settings ───────────────────────────────────────────────────────────────
    write_settings(sc, joinpath(results_dir, "settings.json"))

    return results_dir
end

"""Helper: write aggregate + per-scenario cost CSVs for a stochastic Benders solve."""
function _write_stochastic_benders_costs(
    results_dir::AbstractString,
    sc::StochasticCase,
    bd_results::BendersResults,
    scenario_to_subproblem_map::Dict{Int,Vector{Int}},
    settings::NamedTuple,
)
    planning      = bd_results.planning_problem
    R             = [s.id for s in sc.scenarios]
    p             = Dict(s.id => s.probability for s in sc.scenarios)
    discount_rate = settings.DiscountRate
    period_length = first(settings.PeriodLengths)
    opexmult      = sum(1.0 / (1.0 + discount_rate)^i for i in 1:period_length)

    subop_sol = bd_results.subop_sol
    inv_sys   = investment_system(sc)

    # Rebuild eFixedCost with undiscounted annual costs, mirroring prepare_costs_benders for
    # the non-stochastic case. The planning model bakes opexmult into OM costs and annuity
    # factors into investment costs. undo_discount_fixed_costs! + compute_fixed_costs! rebuilds
    # from true annual values using inv_sys capacity (Float64 after _update_stochastic_with_planning_solution!).
    undo_discount_fixed_costs!(inv_sys, settings)
    unregister(planning, :eFixedCost)
    unregister(planning, :eOMFixedCost)
    unregister(planning, :eInvestmentFixedCost)
    planning[:eFixedCost]           = AffExpr(0.0)
    planning[:eOMFixedCost]         = AffExpr(0.0)
    planning[:eInvestmentFixedCost] = AffExpr(0.0)
    compute_fixed_costs!(inv_sys, planning)
    planning[:eFixedCost] = planning[:eInvestmentFixedCost] + planning[:eOMFixedCost]
    fixed_cost = value(planning[:eFixedCost])

    # Compute actual expected variable cost from final subproblem solutions (UB-based).
    # Using theta variable values from the planning master would give the LB approximation.
    expected_var_cost = sum(
        p[r] * opexmult * sum(subop_sol[w].op_cost for w in scenario_to_subproblem_map[r])
        for r in R
    )

    # CO2 price cost from actual final subproblem solutions
    expected_co2_cost = sum(
        p[r] * opexmult * collect_co2_price_cost_benders(bd_results.op_subproblem, scenario_to_subproblem_map[r])
        for r in R
    )
    expected_op_cost = expected_var_cost - expected_co2_cost

    agg_df = DataFrame(
        FixedCost            = [fixed_cost],
        ExpectedVariableCost = [expected_op_cost],
        ExpectedCO2PriceCost = [expected_co2_cost],
        TotalExpectedCost    = [fixed_cost + expected_var_cost],
    )
    write_dataframe(joinpath(results_dir, "costs.csv"), agg_df)

    sc_rows = map(R) do r
        subop_indices = scenario_to_subproblem_map[r]
        # Actual weighted variable cost from subproblem solutions (not theta approximation)
        weighted_var_total = p[r] * opexmult * sum(subop_sol[w].op_cost for w in subop_indices)
        raw_co2            = collect_co2_price_cost_benders(bd_results.op_subproblem, subop_indices)
        weighted_co2       = p[r] * opexmult * raw_co2
        weighted_op        = weighted_var_total - weighted_co2
        (
            scenario_id          = r,
            probability          = p[r],
            WeightedVariableCost = weighted_op,
            WeightedCO2PriceCost = weighted_co2,
            WeightedTotalOpCost  = weighted_var_total,
        )
    end
    write_dataframe(joinpath(results_dir, "scenario_costs.csv"), DataFrame(sc_rows))

    return nothing
end

"""Helper: write aggregate + per-scenario cost CSVs for a stochastic monolithic solve."""
function _write_stochastic_costs(results_dir::AbstractString, model::Model)
    R        = model.ext[:scenario_ids]
    p        = model.ext[:scenario_probs]
    opexmult = model.ext[:opexmult]

    fixed_cost        = value(model[:eFixedCost])
    expected_var_cost = value(model[:eVariableCost])
    expected_co2_cost = haskey(model, :eExpectedCO2PriceCost) ?
                        value(model[:eExpectedCO2PriceCost]) : 0.0
    expected_op_cost  = expected_var_cost - expected_co2_cost

    agg_df = DataFrame(
        FixedCost            = [fixed_cost],
        ExpectedVariableCost = [expected_op_cost],
        ExpectedCO2PriceCost = [expected_co2_cost],
        TotalExpectedCost    = [fixed_cost + expected_var_cost],
    )
    write_dataframe(joinpath(results_dir, "costs.csv"), agg_df)

    sc_rows = map(R) do r
        raw_var = value(model[:eRawVariableCostByScenario][r])
        raw_co2 = haskey(model, :eRawCO2PriceCostByScenario) ?
                  value(model[:eRawCO2PriceCostByScenario][r]) : 0.0
        raw_op  = raw_var - raw_co2
        (scenario_id           = r,
         probability           = p[r],
         RawVariableCost       = raw_op,
         RawCO2PriceCost       = raw_co2,
         RawTotalOpCost        = raw_var,
         WeightedVariableCost  = p[r] * opexmult * raw_op,
         WeightedCO2PriceCost  = p[r] * opexmult * raw_co2,
         WeightedTotalOpCost   = p[r] * opexmult * raw_var)
    end
    write_dataframe(joinpath(results_dir, "scenario_costs.csv"), DataFrame(sc_rows))

    return nothing
end

"""Write settings JSON for a StochasticCase."""
function write_settings(sc::StochasticCase, filepath::AbstractString)
    settings = Dict{Symbol, Any}(
        :case_settings       => sc.settings,
        :stochastic_settings => sc.stochastic_settings,
        :system_settings     => investment_system(sc).settings,
    )
    write_json(filepath, settings)
end

"""
    Fallback function to write outputs for a single period.
"""
function write_outputs(results_dir::AbstractString, 
    system::System, 
    model::Model, 
    scaling::Float64=1.0
)
    
    # Capacity results
    write_capacity(joinpath(results_dir, "capacity.csv"), system)
    
    # Cost results
    write_costs(joinpath(results_dir, "costs.csv"), system, model)
    write_undiscounted_costs(joinpath(results_dir, "undiscounted_costs.csv"), system, model)

    # Flow results
    write_flow(joinpath(results_dir, "flows.csv"), system)

    write_time_weights(results_dir, system)

    # Write dual values (if enabled)
    if system.settings.DualExportsEnabled
        ensure_duals_available!(model)        
        write_duals(results_dir, system, scaling)
    end

    return nothing
end