"""
    solve_case(sc::StochasticCase, opt) -> (sc, solution, cpu_time)

Top-level dispatch for solving a stochastic case. `opt` is either an `Optimizer`
(Monolithic) or a `Dict{Symbol, Dict{Symbol, Any}}` (Benders). The algorithm is
determined by `SolutionAlgorithm` in `case_settings.json`.
"""
function solve_case(sc::StochasticCase, opt::O) where O <: Union{Optimizer, Dict{Symbol, Dict{Symbol, Any}}}
    solve_case(sc, opt, solution_algorithm(sc))
end

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

"""
    solve_case(sc::StochasticCase, opt::Dict, ::Benders; case_path, v) -> (sc, BendersResults, cpu_time)

Benders decomposition for a stochastic case.

`opt[:planning]` and `opt[:subproblems]` are optimizer config dicts with `:solver`
and `:attributes` keys. `case_path` is required (used by MacroEnergySolvers for cut
file output). Supported `BendersCut` values: `"multi"` and `"single"`.
"""
function solve_case(
    sc::StochasticCase,
    opt::Dict{Symbol, Dict{Symbol, Any}},
    ::Benders;
    case_path::Union{Nothing, String} = nothing,
    v::Bool = false,
)
    @info("*** Running stochastic case with Benders decomposition ***")

    case_path === nothing &&
        error("solve_case (Stochastic Benders): `case_path` must be provided.")

    bd_setup = sc.settings.BendersSettings

    system_decomp, scenario_to_subproblem_map =
        generate_stochastic_decomposed_system(sc)

    planning_problem, _ =
        initialize_stochastic_planning_problem!(sc, opt[:planning])

    # sc.settings provides PeriodLengths = [1] and DiscountRate for the subproblem
    # objective (discount_factor[1] * opexmult[1]), matching the single planning period.
    subproblems, linking_variables_sub =
        initialize_subproblems!(
            system_decomp,
            opt[:subproblems],
            sc.settings,
            bd_setup[:Distributed],
            bd_setup[:IncludeSubproblemSlacksAutomatically],
        )

    start_optimization = time()

    cut_type = bd_setup[:BendersCut]

    if cut_type == "multi"
        @info("Running Multi-cut Benders (stochastic)")
        results = MacroEnergySolvers.benders_multi(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), scenario_to_subproblem_map;
            case_path = case_path, v = v)

    elseif cut_type == "single"
        @info("Running Single-cut Benders (stochastic)")
        results = MacroEnergySolvers.benders_single(
            planning_problem, subproblems, linking_variables_sub,
            Dict(pairs(bd_setup)), scenario_to_subproblem_map;
            case_path = case_path, v = v)

    else
        error("BendersCut must be \"multi\" or \"single\" for stochastic cases. " *
              "Got: $(repr(cut_type))")
    end

    cpu_optimization = time() - start_optimization

    _update_stochastic_with_planning_solution!(sc, results.planning_sol.values)

    return (sc, BendersResults(results, subproblems), cpu_optimization)
end

"""
    _update_stochastic_with_planning_solution!(sc, sol_values)

Write the Benders planning solution back into every scenario's asset structures.
Investment VariableRefs are first shared from the investment system to all other
scenarios, then `update_with_planning_solution!` is called on each.
"""
function _update_stochastic_with_planning_solution!(sc::StochasticCase, sol_values::Dict)
    inv_sys = investment_system(sc)
    for sc_r in sc.scenarios
        sc_r.system !== inv_sys && _share_capacity_with_scenario!(sc_r.system, inv_sys)
    end
    for sc_r in sc.scenarios
        update_with_planning_solution!(sc_r.system, sol_values)
    end
end
