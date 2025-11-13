
# Used in pre_clustering_tdr.jl
function get_extreme_period(DF, GDF, profKey, typeKey, statKey, demand_col_names, solar_col_names, wind_col_names; v=false)
    if v println(profKey," ", typeKey," ", statKey) end
    if typeKey == "Integral"
        if profKey == "Load"
            (stat, group_idx) = get_integral_extreme(GDF, statKey, demand_col_names)
        elseif profKey == "PV"
            (stat, group_idx) = get_integral_extreme(GDF, statKey, solar_col_names)
        elseif profKey == "Wind"
            (stat, group_idx) = get_integral_extreme(GDF, statKey, wind_col_names)
        else
            println(" -- Error: Profile Key ", profKey, " is invalid. Choose `Load', `PV' or `Wind'.")
        end
    elseif typeKey == "Absolute"
        if profKey == "Load"
            (stat, group_idx) = get_absolute_extreme(DF, statKey, demand_col_names)
        elseif profKey == "PV"
            (stat, group_idx) = get_absolute_extreme(DF, statKey, solar_col_names)
        elseif profKey == "Wind"
            (stat, group_idx) = get_absolute_extreme(DF, statKey, wind_col_names)
        else
            println(" -- Error: Profile Key ", profKey, " is invalid. Choose `Load', `PV' or `Wind'.")
        end
   else
       println(" -- Error: Type Key ", typeKey, " is invalid. Choose `Absolute' or `Integral'.")
       stat = 0
       group_idx = 0
   end
    return (stat, group_idx)
end

function get_integral_extreme(GDF, statKey, col_names)
    #println(" → Columns being summed: ", col_names)

    if statKey == "Max"
        (stat, stat_idx) = findmax( sum([GDF[!, Symbol(c)] for c in col_names ]) )
    elseif statKey == "Min"
        (stat, stat_idx) = findmin( sum([GDF[!, Symbol(c)] for c in col_names ]) )
    else
        println(" -- Error: Statistic Key ", statKey, " is invalid. Choose `Max' or `Min'.")
    end
    return (stat, stat_idx)
end

function get_absolute_extreme(DF, statKey, col_names)
    #println(" → Columns being summed: ", col_names)

    if statKey == "Max"
        (stat, stat_idx) = findmax( sum([DF[!, Symbol(c)] for c in col_names ]) )
        group_idx = DF.Group[stat_idx]
    elseif statKey == "Min"
        (stat, stat_idx) = findmin( sum([DF[!, Symbol(c)] for c in col_names ]) )
        group_idx = DF.Group[stat_idx]
    else
        println(" -- Error: Statistic Key ", statKey, " is invalid. Choose `Max' or `Min'.")
    end
    return (stat, group_idx)
end


# Used in run_time_domain_reduction.jl
function to_string_keys(x)
    if x isa Dict
        return Dict(string(k) => to_string_keys(v) for (k,v) in x)
    elseif x isa Vector
        return [to_string_keys(v) for v in x]
    elseif x isa JSON3.Object
        return Dict(string(k) => to_string_keys(v) for (k,v) in pairs(x))
    else
        return x
    end
end

# Used in pre_clustering_tdr.jl
function drop_time_columns!(df)
    if isempty(df)
        return
    end
    dropcols = intersect(names(df), ["Time", "Index", "Time_Index", "Hour", "Datetime"])
    if !isempty(dropcols)
        select!(df, Not(Symbol.(dropcols)))
    end
end

# Used in write_outputs_tdr.jl
function create_reduced_csv(full_path::String, reduced_path::String, M::Vector{Int}, hours_per_period::Int; v=false)
    # Load full-year data
    df_full = CSV.read(full_path, DataFrame)
    total_hours = nrow(df_full)
    ncols = ncol(df_full)

    # Determine file type label based on output name
    filename_lower = lowercase(basename(reduced_path))
    file_type =
        if occursin("demand", filename_lower)
            "Demand"
        elseif occursin("fuel", filename_lower)
            "Fuel Prices"
        elseif occursin("avail", filename_lower)
            "Availability"
        else
            "Time Series"
        end

    # Extract representative periods
    df_reduced_list = DataFrame[]
    for m in M
        start_idx = (m - 1) * hours_per_period + 1
        end_idx   = min(m * hours_per_period, total_hours)
        push!(df_reduced_list, df_full[start_idx:end_idx, :])
    end

    # Concatenate vertically
    df_reduced = vcat(df_reduced_list...)

    # Robustly relabel Time_Index (if present)
    time_col = findfirst(x -> lowercase(string(x)) == "time_index", names(df_reduced))
    if time_col !== nothing
        df_reduced[!, names(df_reduced)[time_col]] = collect(1:nrow(df_reduced))
    elseif "Time" in names(df_reduced)
        df_reduced[!, :Time] = collect(1:nrow(df_reduced))
    else
        println("No recognizable time column found in $(basename(full_path)); leaving indices unchanged.")
    end

    # Write reduced CSV
    CSV.write(reduced_path, df_reduced)

    # Log success
    println("Reduced $file_type file written to:", reduced_path)

    if v
        println("   Original hours: ", total_hours,
                " | Reduced hours: ", nrow(df_reduced),
                " | Columns: ", ncols)
        println()
    end
end

# Used in write_outputs_tdr.jl
function write_time_data_json(system_path::String,
                              tdr_output_path::String,
                              period_map_file::String,
                              TimestepsPerRepPeriod::Int,
                              NumberOfSubperiods::Int,
                              TotalHoursModeled::Int;
                              v::Bool=false)

    # Load commodities file
    commodities_path = joinpath(system_path, "commodities.json")
    if !isfile(commodities_path)
        error("commodities.json not found at: $commodities_path")
    end

    commodities_json = JSON3.read(open(commodities_path, "r"))
    commodities = String[]

    for entry in commodities_json["commodities"]
        if entry isa String
            push!(commodities, entry)
        elseif haskey(entry, "acts_like")
            push!(commodities, String(entry["acts_like"]))
        elseif haskey(entry, "name")
            push!(commodities, String(entry["name"]))
        end
    end

    commodities = unique(commodities)

    if v
        println("Found commodities: ", commodities)
    end

    # Build time_data dictionary
    HoursPerSubperiod_dict = Dict(c => TimestepsPerRepPeriod for c in commodities)
    HoursPerTimeStep_dict  = Dict(c => 1 for c in commodities)

    time_data = Dict(
        "HoursPerSubperiod"  => HoursPerSubperiod_dict,
        "HoursPerTimeStep"   => HoursPerTimeStep_dict,
        "NumberOfSubperiods" => NumberOfSubperiods,
        "SubPeriodMap"       => Dict(
            "path" => joinpath("system", basename(tdr_output_path), period_map_file)
        ),
        "TotalHoursModeled"  => TotalHoursModeled
    )
    

    # Write JSON
    time_data_out_path = joinpath(tdr_output_path, "time_data.json")
    open(time_data_out_path, "w") do io
        JSON3.write(io, time_data; indent=4)
    end

    println("time_data.json written to ", time_data_out_path)
    if v
        println("   Number of commodities: ", length(commodities))
        println("   Subperiods: ", NumberOfSubperiods, 
                " | HoursPerRepPeriod: ", TimestepsPerRepPeriod)
        println("   Linked PeriodMap: ", joinpath("system", basename(tdr_output_path), period_map_file))
    end
end
