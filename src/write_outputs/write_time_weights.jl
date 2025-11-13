"""
    write_time_weights(results_dir::AbstractString, system::System)

Write the subperiod weight for each modeled timestep to a CSV file.

This uses the same logic as `balance_duals` to ensure the weights correspond exactly
to the modeled timesteps after time-domain reduction (e.g., 840 hours).

# Output
Creates `time_weights.csv` with columns:
- `Time`: timestep index (1:N)
- `Weight`: subperiod weight for that timestep
"""
function write_time_weights(results_dir::AbstractString, system::System)
    @info "Writing time weights to $results_dir"

    filename = "time_weights.csv"
    file_path = joinpath(results_dir, filename)

    # Pick a reference node (just to get the modeled time structure)
    ref_node = first(filter(n -> n isa Node, system.locations))

    # Extract weights per modeled timestep
    weights = [
        subperiod_weight(ref_node, current_subperiod(ref_node, t))
        for t in time_interval(ref_node)
    ]

    df = DataFrame(Time = collect(1:length(weights)), Weight = weights)
    CSV.write(file_path, df)

    return nothing
end
