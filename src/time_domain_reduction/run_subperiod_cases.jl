function run_subperiod_cases(
    case_path::String,
    optimizer::DataType,
    optimizer_env,
    optimizer_attributes,
    mysetup::Dict;
    num_periods::Int = 1,
    v::Bool = false
)
    println()
    println("=== Output-based TDR: Running subperiod CEMs ===")

    # ============================================================
    # 1. Load settings
    # ============================================================
    TimestepsPerRepPeriod = mysetup["TimestepsPerRepPeriod"]
    SubperiodPolicyConstraints = mysetup["SubperiodPolicyConstraints"]
    sub_cfg = mysetup["SubperiodResults"]["Commodities"]

    # Validate that at least one commodity+asset is included
    has_valid_output = false

    for (commodity, cfg) in sub_cfg
        if cfg["Include"] == 1
            asset_cfg = cfg["Assets"]

            # Check if at least ONE asset is included
            for (_, toggle) in asset_cfg
                if toggle == 1
                    has_valid_output = true
                    break
                end
            end
        end

        if has_valid_output
            break   # no need to continue checking
        end
    end

    if !has_valid_output
        error("""
        No subperiod outputs selected!
        In TDR_settings.json, at least one commodity must have `"Include": 1`
        AND at least one of its assets must also have `"Include": 1`.

        Example:
        "Electricity" : {
            "Include": 1,
            "Assets": { "VRE": 1 }
        }
        """)
    end

    # ============================================================
    # 2. Solve subperiods
    # ============================================================
    GLOBAL_TDR_FLAG[] = 0

    @info "Include subperiod cases results in TDR"
    system_path = joinpath(case_path, "system")

    for s in 1:num_periods
        # Path to subperiod results
        base_name = mysetup["ClusterSubperiodFileName"]
        
        if GLOBAL_NUM_PERIODS[] == 1
            file = base_name * ".csv"
        else
            file = base_name * "_" * string(s) * ".csv"
        end

        subperiod_results_file = joinpath(system_path, file)

        # If file exists, skip subperiod runs
        if isfile(subperiod_results_file)
            @info "Subperiod results already exist for period $s, skipping subperiod runs"
            continue
        else
            @info "Generating subperiod results for period $s"
        end
            
        T_full = get_original_T_full(case_path)
        ranges = make_subperiod_ranges(T_full, TimestepsPerRepPeriod)

        num_sub = length(ranges)
        println("Running total of $(length(ranges)) subperiods")

        # 3. Create output folder structure
        if GLOBAL_NUM_PERIODS[] == 1
            sub_results_dir = joinpath(case_path, "subperiod_results")
        else
            sub_results_dir = joinpath(case_path, "subperiod_results_$(s)")
        end            

        mkpath(sub_results_dir)

        #Turn off logging
    

        # Load the original case once
        full_case = load_case(case_path; lazy_load=true)

        # Build solver
        opt = create_optimizer(optimizer, optimizer_env, optimizer_attributes)

        # Apply full-year scaling manually
        weight_per_hour = T_full / TimestepsPerRepPeriod

        # Run subperiods of period s in multi-period system
        if num_periods > 1
            case_s = Case([full_case.systems[s]], full_case.settings)
        else
            case_s = full_case
        end

        # Loop over subperiods (Can add distributed here)
        for sp = 1:num_sub
            t_range = ranges[sp]
            println("Running $sp / $(length(ranges)) subperiods t = $t_range")
            println()

            # Deep copy the case for this subperiod run
            case_sub = deepcopy(case_s)

            if SubperiodPolicyConstraints == 0
                println("Disabling all policy constraints for subperiod cases.")
                println()

                for sys in case_sub.systems
                    disable_policy_constraints!(sys)
                end
            end
        
            # Restrict time_data for each commodity
            for sys in case_sub.systems
                for c in keys(sys.time_data)
                    td = sys.time_data[c]
                    td.time_interval = t_range
                    td.subperiods = [t_range]
                    td.subperiod_indices = [1]
                    td.subperiod_weights = Dict(1 => weight_per_hour)
                    td.subperiod_map = Dict(t => 1 for t in t_range)
                    td.period_index = s
                end
            end

            # Solve this subperiod
            solve_start = time()
            (_, sol_sub) = solve_case(case_sub, opt, Monolithic())
            solve_time = time() - solve_start
            println("Subperiod $sp solved in $(round(solve_time; digits=2)) seconds")

            # Prepare subperiod folder
            sp_folder = joinpath(sub_results_dir, "sub_$(lpad(string(sp), 3, '0'))")
            mkpath(sp_folder)

            # Raw results
            write_subperiod_results(case_sub, sp_folder)

            # Extract time series results of specified commodities/assets in TDR_settings.json file for TDR input
            write_subperiod_results(case_sub, sp_folder, sub_cfg, mysetup)

            # Debugging outputs (availability + demand)
            if v
                system_sub = case_sub.systems[1]
                write_subperiod_availability(system_sub, sp_folder)
                write_subperiod_demand(system_sub, sp_folder)
            end
        end

        #Merge individual subperiod time series results into a combined dataframe for TDR input
        merge_subperiod_results_to_timeseries(case_path, mysetup; period_idx = s)
        
        @info "Finished generating subperiod flow matrices period $s"
    end

    GLOBAL_TDR_FLAG[] = 1
    println("=== Subperiod output-based TDR completed for $num_periods subperiods ===")
    println()

    return nothing
end
