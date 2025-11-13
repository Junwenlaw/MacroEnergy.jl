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

    # Folder path for outputs
    tdr_output_path = joinpath(system_path, myTDRsetup["TimeDomainReductionFolder"])
    
    # If TDR output folder already exists, skip running TDR
    if isdir(tdr_output_path)
        @info "TDR results folder already exists at: $tdr_output_path"
        @info "Using existing TDR results"
        return nothing
    else
        mkpath(tdr_output_path)
        @info "Initializing TDR"
    end

    if v
        println("TDR Settings loaded from: ", tdr_settings_path)
        println("    TimestepsPerRepPeriod : ", myTDRsetup["TimestepsPerRepPeriod"])
        println("    NumberOfSubperiods    : ", myTDRsetup["NumberOfSubperiods"])
        println("    ClusterMethod         : ", myTDRsetup["ClusterMethod"])
        println("    ScalingMethod         : ", myTDRsetup["ScalingMethod"])
        println("    UseExtremePeriods     : ", myTDRsetup["UseExtremePeriods"])
        println("    TotalHoursModeled     : ", myTDRsetup["TotalHoursModeled"])
        println("    Output folder         : ", tdr_output_path)
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
