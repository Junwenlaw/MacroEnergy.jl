function write_outputs_tdr(case_path::String, myTDRsetup::Dict, PeriodMap::DataFrame, M::Vector{Int}; v::Bool=false)
    
    println("=== Write TDR Outputs ===")

    system_path = joinpath(case_path, "system")
    tdr_output_path = joinpath(system_path, myTDRsetup["TimeDomainReductionFolder"])

    # Write PeriodMap
    period_map_path = joinpath(tdr_output_path, "Period_map.csv")
    CSV.write(period_map_path, PeriodMap)
    println("Period_map.csv written to $period_map_path")

    # Generate Reduced Input CSVs
    TimestepsPerRepPeriod = myTDRsetup["TimestepsPerRepPeriod"]
    file_keys = ["AvailabilityFileName", "DemandFileName", "FuelPricesFileName"]

    for key in file_keys
        full_path = joinpath(system_path, myTDRsetup[key])
        reduced_path = joinpath(tdr_output_path, basename(myTDRsetup[key]))

        if isfile(full_path)
            create_reduced_csv(full_path, reduced_path, M, TimestepsPerRepPeriod; v=v)
        else
            println("Missing file: ", full_path, " — skipping reduction.")
        end
    end

    # --- Write time_data.json ---
    write_time_data_json(
        system_path,
        tdr_output_path,
        "Period_map.csv",
        myTDRsetup["TimestepsPerRepPeriod"],
        myTDRsetup["NumberOfSubperiods"],
        myTDRsetup["TotalHoursModeled"];
        v=v
    )

    println("=== Completed ===")
    println()
end