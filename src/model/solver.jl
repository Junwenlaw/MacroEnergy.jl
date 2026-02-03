function solve_case(case::Case, opt::O) where O <: Union{Optimizer, Dict{Symbol, Dict{Symbol, Any}}}
    solve_case(case, opt, solution_algorithm(case))
end

function solve_case(case::Case, opt::Optimizer, ::Monolithic)

    @info("*** Running simulation with monolithic solver ***")
    
    
    model = generate_model(case)
    set_optimizer(model, opt)

    # For monolithic solution there is only one model
    # scale constraints if the flag is true in the first system
    if case.systems[1].settings.ConstraintScaling
        @info "Scaling constraints and RHS"
        scale_constraints!(model)
    end

    start_optimization = time()
    optimize!(model)
    cpu_optimization = time() - start_optimization

    return (case, model, cpu_optimization)
end

####### myopic expansion #######
function solve_case(case::Case, opt::Optimizer, ::Myopic)

    @info("*** Running simulation with myopic iteration ***")
    
    myopic_results, cpu_optimization = run_myopic_iteration!(case,opt)

    return (case, myopic_results, cpu_optimization)
end

####### Benders decomposition algorithm #######
function solve_case(case::Case, opt::Dict{Symbol, Dict{Symbol, Any}}, ::Benders; case_path::Union{Nothing,String}=nothing, fixed_group_map::Union{Dict{Int,Dict{Int,Vector{Int}}},Nothing} = nothing, v::Bool=false)

    @info("*** Running simulation with Benders decomposition ***")

    if case_path === nothing
        error("solve_case (Benders): `case_path` must be provided when running Benders decomposition.")
    end

    bd_setup = get_settings(case).BendersSettings
    periods = get_periods(case);
    partial_subproblem_solve = get(bd_setup, :PartialSubproblemSolve, false)
    cut_trimming = get(bd_setup, :CutTrimming, false)

    # Decomposed system
    periods_decomp = generate_decomposed_system(periods);

    planning_problem, period_to_subproblem_map = initialize_planning_problem!(case,opt[:planning])

    subproblems, linking_variables_sub = initialize_subproblems!(periods_decomp,opt[:subproblems],bd_setup[:Distributed],bd_setup[:IncludeSubproblemSlacksAutomatically])

    start_optimization = time()
    #results = MacroEnergySolvers.benders(planning_problem, subproblems, linking_variables_sub, Dict(pairs(bd_setup)), period_to_subproblem_map; get_flow_func = MacroEnergy.get_optimal_flow, reshape_wide_func = MacroEnergy.reshape_wide, fixed_group_map = fixed_group_map, case_path = case_path, v=v)
    if bd_setup[:BendersCut] == "multi" && !partial_subproblem_solve && !cut_trimming
        @info("Running Multi-cut Benders")
        results = MacroEnergySolvers.benders_multi(planning_problem, subproblems, linking_variables_sub, Dict(pairs(bd_setup)), period_to_subproblem_map; case_path = case_path, v=v)

    elseif bd_setup[:BendersCut] == "multi" && !partial_subproblem_solve && cut_trimming
        @info("Running Multi-cut Benders with Cut Trim")
        results = MacroEnergySolvers.benders_multi_trimming(planning_problem, subproblems, linking_variables_sub, Dict(pairs(bd_setup)), period_to_subproblem_map; case_path = case_path, v=v)

    elseif bd_setup[:BendersCut] == "single"
        @info("Running Single-cut Benders")
        results = MacroEnergySolvers.benders_single(planning_problem, subproblems, linking_variables_sub, Dict(pairs(bd_setup)), period_to_subproblem_map; case_path = case_path, v=v)

    elseif bd_setup[:BendersCut] == "group" && bd_setup[:GroupType] == "fixed"
        @info("Running Fixed Group-cut Benders")
        results = MacroEnergySolvers.benders_fixed_group(planning_problem, subproblems, linking_variables_sub, Dict(pairs(bd_setup)), period_to_subproblem_map; fixed_group_map = fixed_group_map, case_path = case_path, v=v)
    
    elseif bd_setup[:BendersCut] == "group" && bd_setup[:GroupType] == "adaptive" && !partial_subproblem_solve
        @info("Running Adaptive Group-cut Benders")
        results = MacroEnergySolvers.benders_adaptive_group(planning_problem, subproblems, linking_variables_sub, Dict(pairs(bd_setup)), period_to_subproblem_map; get_flow_func = MacroEnergy.get_optimal_flow, reshape_wide_func = MacroEnergy.reshape_wide, case_path = case_path, v=v)
    
    # Partial SP solving algorithms
    elseif bd_setup[:BendersCut] == "multi" && partial_subproblem_solve
        @info("Running Multi-cut Benders with Partial Subproblem Solves from intermediate clustering")
        results = MacroEnergySolvers.benders_multi_partial_SP(planning_problem, subproblems, linking_variables_sub, Dict(pairs(bd_setup)), period_to_subproblem_map; get_flow_func = MacroEnergy.get_optimal_flow, reshape_wide_func = MacroEnergy.reshape_wide, case_path = case_path, v=v)

    elseif bd_setup[:BendersCut] == "group" && bd_setup[:GroupType] == "adaptive" && partial_subproblem_solve
        @info("Running Adaptive Group-cut Benders with Partial Subproblem Solves")
        results = MacroEnergySolvers.benders_adaptive_group_partial_SP(planning_problem, subproblems, linking_variables_sub, Dict(pairs(bd_setup)), period_to_subproblem_map; get_flow_func = MacroEnergy.get_optimal_flow, reshape_wide_func = MacroEnergy.reshape_wide, case_path = case_path, v=v)
    
    else
        error("Benders method unspecified - Use multi, single, or group (fixed or adaptive) cuts")
    end
    cpu_optimization = time() - start_optimization

    update_with_planning_solution!(case, results.planning_sol.values)

    return (case, BendersResults(results, subproblems), cpu_optimization)
end

"""
    ensure_duals_available!(model::Model)

Ensure that dual values are available in the model. If the model has integer variables
and duals are not available, fixes the integer variables and re-solves the LP model to 
compute duals.

# Arguments
- `model::Model`: The JuMP model to ensure duals for

# Throws
- `ErrorException`: If the model is not solved and feasible or if the dual values are not 
available after linearization

# Notes
- This function modifies the model in-place by fixing integer and binary variables to their 
current values.
- The model is solved again in silent mode to avoid redundant output
"""
function ensure_duals_available!(model::Model)
    if has_duals(model)
        @debug "Dual values available in the model"
        return nothing
    end

    assert_is_solved_and_feasible(model)
    
    @info "Dual values not available in the model. Linearizing model and re-solving to compute duals."
    
    # Fix integer and binary variables to their current values
    fix_discrete_variables(model);
    
    # Re-solve the LP model silently
    was_silent = get_attribute(model, MOI.Silent())
    set_silent(model)
    optimize!(model)
    
    # Restore original silent setting if it was not already set
    was_silent || unset_silent(model)
    
    # Verify that duals are now available
    assert_is_solved_and_feasible(model)
    if dual_status(model) != MOI.FEASIBLE_POINT
        error("Model is not feasible after linearization.")
    end
    
    @info "Linearization successful, dual values now available."
    
    return nothing
end