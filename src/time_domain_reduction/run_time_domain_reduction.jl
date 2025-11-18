function run_time_domain_reduction(case_path::String; v::Bool=false)

    # Load settings
    system_path = joinpath(case_path, "system")
    tdr_settings_path = joinpath(system_path, "TDR_settings.json")
    
    if !isfile(tdr_settings_path)
        error("Missing TDR_settings.json at: $tdr_settings_path")
    end

    # Read TDR_settings.json as text and parse
    json_text = read(tdr_settings_path, String)
    raw_setup = JSON3.read(json_text)
    myTDRsetup = to_string_keys(raw_setup)

    # Check for existing files to skip running TDR

    tdr_output_path = joinpath(system_path, myTDRsetup["TimeDomainReductionFolder"])
    required_files = String[]

    # Add required files based on user settings
    if myTDRsetup["ClusterAvailability"] == 1
        push!(required_files, myTDRsetup["AvailabilityFileName"])
    end

    if myTDRsetup["ClusterDemand"] == 1
        push!(required_files, myTDRsetup["DemandFileName"])
    end

    if myTDRsetup["ClusterFuelPrices"] == 1
        push!(required_files, myTDRsetup["FuelPricesFileName"])
    end

    # Check each required file inside the TDR folder
    all_exist = true
    for fname in required_files
        fpath = joinpath(tdr_output_path, fname)
        if !isfile(fpath)
            @info "Missing TDR input file: $fpath"
            all_exist = false
        end
    end

    if all_exist && isdir(tdr_output_path)
        @info "All required TDR files exist in: $tdr_output_path"
        @info "Skipping TDR computation and using existing files"
        return nothing
    else
        @info "Initializing full TDR"
        mkpath(tdr_output_path)
    end

    if v
        println("TDR Settings loaded from: ", tdr_settings_path)
        println("    TimestepsPerRepPeriod       : ", myTDRsetup["TimestepsPerRepPeriod"])
        println("    NumberOfSubperiods          : ", myTDRsetup["NumberOfSubperiods"])
        println("    ClusterMethod               : ", myTDRsetup["ClusterMethod"])
        println("    ScalingMethod               : ", myTDRsetup["ScalingMethod"])
        println("    UseExtremePeriods           : ", myTDRsetup["UseExtremePeriods"])
        println("    ClusterSubperiodResults     : ", myTDRsetup["ClusterSubperiodResults"])
        println("    TotalHoursModeled           : ", myTDRsetup["TotalHoursModeled"])
        println("    Output folder               : ", tdr_output_path)
        println()
    end

    # Pre-clustering processing
    (ClusteringInputDF, ModifiedDataNormalized, NClusters, ExtremeWksList) = pre_clustering_tdr(case_path, myTDRsetup; v=v)

    # Clustering
    (A, M, W) = clustering_tdr(tdr_output_path, myTDRsetup, ClusteringInputDF, NClusters; v=v)

    # Post-clustering processing
    (A, W, M, PeriodMap) = post_clustering_tdr(myTDRsetup, ClusteringInputDF, NClusters, A, W, M, ExtremeWksList, ModifiedDataNormalized; v=v)

    # Write outputs
    write_outputs_tdr(case_path, myTDRsetup, PeriodMap, M; v=v)

    @info "TDR Completed"
    
end
