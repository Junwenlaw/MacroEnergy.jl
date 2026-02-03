const GLOBAL_TDR_FLAG = Ref(0) 
const GLOBAL_NUM_PERIODS = Ref(1)

function run_case(
    case_path::AbstractString=@__DIR__;
    lazy_load::Bool=true,
    # Logging
    log_level::LogLevel=Logging.Info,
    log_to_console::Bool=true,
    log_to_file::Bool=true,
    log_file_path::AbstractString=joinpath(case_path, "$(basename(case_path)).log"),
    log_file_attribution::Bool=true,
    # Monolithic or Myopic
    optimizer::DataType=HiGHS.Optimizer,
    optimizer_env::Any=nothing,
    optimizer_attributes::Tuple=("BarConvTol" => 1e-3, "Crossover" => 0, "Method" => 2),
    # Benders
    planning_optimizer::DataType=HiGHS.Optimizer,
    subproblem_optimizer::DataType=HiGHS.Optimizer,
    planning_optimizer_attributes::Tuple=("BarConvTol" => 1e-3, "Crossover" => 0, "Method" => 2),
    subproblem_optimizer_attributes::Tuple=("BarConvTol" => 1e-3, "Crossover" => 0, "Method" => 2)
)
    # This will run when the Julia process closes. 
    # It may be overfill with the try-catch
    atexit(() -> try case_cleanup() catch; end)

    set_logger(log_to_console, log_to_file, log_level, log_file_path, log_file_attribution)

    # Wrapping the work in a try-catch to all for cleanup after errors
    try 
        @info("Running case at $(case_path)")
        start_case = time()

        create_user_additions_module(case_path)
        additions_path = user_additions_module_path(case_path)
        load_user_additions(additions_path)

        # Identify multi-stage or period models for TDR and Benders group clustering purposes
        case_settings_path = joinpath(case_path, "settings", "case_settings.json")
        case_settings_setup = to_string_keys(JSON3.read(read(case_settings_path, String)))

        if haskey(case_settings_setup, "PeriodLengths")
            num_periods = length(case_settings_setup["PeriodLengths"])
        else
            num_periods = 1
        end

        # Time Domain Reduction
        tdr_settings_file = joinpath(case_path, "settings", "TDR_settings.json")
        cpu_subperiod_cases = "NA"
        cpu_time_domain_reduction = "NA"

        if isfile(tdr_settings_file)
            @info "Detected TDR_settings.json"
            try
                @info "Detected $num_periods planning periods"
                myTDRsetup = to_string_keys(JSON3.read(read(tdr_settings_file, String)))
                TDR_flag = myTDRsetup["TimeDomainReduction"]

                GLOBAL_TDR_FLAG[] = TDR_flag 
                GLOBAL_NUM_PERIODS[] = num_periods

                if TDR_flag == 1
                    @info "Time Domain Reduction Enabled"
                    # Cluster Subperiod Results: Incorporate Output-based TDR by solving individual subperiod level CEM
                    if myTDRsetup["ClusterSubperiodResults"] == 1
                        start_subperiod_cases = time()
                        run_subperiod_cases(case_path, optimizer,optimizer_env, optimizer_attributes, myTDRsetup; num_periods = num_periods, v = false)
                        cpu_subperiod_cases = time() - start_subperiod_cases
                    end
                    start_time_domain_reduction = time()
                    run_time_domain_reduction(case_path, myTDRsetup; num_periods = num_periods, v = false)
                    cpu_time_domain_reduction = time() - start_time_domain_reduction
                else
                    @info "Time Domain Reduction Disabled"
                end
            catch e
                @error "TDR Failed, possible issue with TDR_settings.json: $(e)"
                rethrow(e)
            end
        else
            @debug "No TDR_settings.json found — skipping time domain reduction"
            GLOBAL_TDR_FLAG[] = 0
        end

        case = load_case(case_path; lazy_load=lazy_load)

        # Create optimizer based on solution algorithm
        optimizer = if isa(solution_algorithm(case), Monolithic) || isa(solution_algorithm(case), Myopic)
            create_optimizer(optimizer, optimizer_env, optimizer_attributes)
        elseif isa(solution_algorithm(case), Benders)
            create_optimizer_benders(planning_optimizer, subproblem_optimizer,
                planning_optimizer_attributes, subproblem_optimizer_attributes)
        else
            error("The solution algorithm is not Monolithic, Myopic, or Benders. Please double check the `SolutionAlgorithm` in the `settings/case_settings.json` file.")
        end

        # If Benders, create processes for subproblems optimization
        if isa(solution_algorithm(case), Benders)
            if case.settings.BendersSettings[:Distributed]
                number_of_subproblems = sum(length(system.time_data[:Electricity].subperiods) for system in case.systems)
                start_distributed_processes!(number_of_subproblems, case_path)
            end
        end

        cpu_benders_group_subproblem_cases = "NA"
        cpu_benders_group_clustering = "NA"

        if isa(solution_algorithm(case), Benders)

            bd_setup = get_settings(case).BendersSettings
            fixed_group_map = nothing

            # Cluster Subperiod Results: Incorporate Output-based TDR by solving individual subperiod level CEM
            if bd_setup[:BendersCut] == "group" && bd_setup[:GroupType] == "fixed"
                @info("Running fixed Bender group clustering based on input time-series similar to TDR")
                @info("Utilizing TDR codes to obtain group maps")

                bd_fixed_group_settings_file = joinpath(case_path, "settings", "benders_fixed_group_settings.json")
                myBDfixedgroupsetup = to_string_keys(JSON3.read(read(bd_fixed_group_settings_file, String)))
                myBDfixedgroupsetup["NumberOfSubperiods"] = bd_setup[:BendersNumGroups]

                if myBDfixedgroupsetup["ClusterSubperiodResults"] == 1
                    start_benders_group_subproblem_cases = time()
                    run_subperiod_cases(case_path, planning_optimizer, optimizer_env, planning_optimizer_attributes, myBDfixedgroupsetup; num_periods = num_periods, v = false)
                    cpu_benders_group_subproblem_cases = time() - start_benders_group_subproblem_cases
                end

                bd_fixed_group_settings_file = joinpath(case_path, "settings", "benders_fixed_group_settings.json")
                myBDfixedgroupsetup = to_string_keys(JSON3.read(read(bd_fixed_group_settings_file, String)))
                myBDfixedgroupsetup["NumberOfSubperiods"] = bd_setup[:BendersNumGroups]

                #Run TDR to obtain period map
                start_benders_group_clustering = time()
                run_time_domain_reduction(case_path, myBDfixedgroupsetup; num_periods = num_periods, v = false)
                cpu_benders_group_clustering = time() - start_benders_group_clustering

                #Utilize period map to build Benders group map 
                fixed_group_map = write_benders_fixed_group_map(case_path, myBDfixedgroupsetup, num_periods)
                print_global_group_map(fixed_group_map)

                @info("Pre-Benders clustering complete — fixed grouping will be used")
            end

            start_solve_total = time()
            (case, solution, cpu_optimization) = solve_case(case, optimizer, Benders(); case_path=case_path, fixed_group_map=fixed_group_map, v = false)
            cpu_solve_total = time() - start_solve_total
        else
            start_solve_total = time()
            (case, solution, cpu_optimization) = solve_case(case, optimizer)
            cpu_solve_total = time() - start_solve_total
        end 

        cpu_case = time() - start_case

        # Myopic outputs are written during iteration, so we don't need to write them here
        if !isa(solution_algorithm(case), Myopic)
            if length(case.systems) ≥ 1
                case_path = create_output_path(case.systems[1], case_path)
            end
            write_outputs(case_path, case, solution)
        end

        # If Benders, delete processes
        if isa(solution_algorithm(case), Benders)
            if case.settings.BendersSettings[:Distributed] && length(workers()) > 1
                rmprocs.(workers())
            end
        end

        # Write CPU runtime summary
        runtime_df = DataFrame(
            TDR_Subperiod_Cases_sec            = [cpu_subperiod_cases],
            TDR_Clustering_sec                 = [cpu_time_domain_reduction],
            Benders_Subperiod_Cases_sec        = [cpu_benders_group_subproblem_cases],
            Benders_Group_Clustering_sec       = [cpu_benders_group_clustering],
            Optimization_CPU_sec               = [cpu_optimization],
            Solve_Total_CPU_sec                = [cpu_solve_total],
            Case_Total_CPU_sec                 = [cpu_case],
        )

        runtime_csv_path = joinpath(case_path, "Case_Runtime_Summary.csv")
        CSV.write(runtime_csv_path, runtime_df)
        @info("Case runtime summary written to $runtime_csv_path")


        return case.systems, solution
    catch e
        rethrow(e)
    finally
        case_cleanup()  # Ensure all processes are removed
    end
end

function case_cleanup()
    rmprocs(workers())  # Ensure all processes are removed
end