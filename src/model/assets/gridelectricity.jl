struct GridElectricity <: AbstractAsset
    id::AssetId
    gridelectricity_transform::Transformation
    grid_elec_edge::Edge{Electricity}
    elec_edge::Edge{Electricity}
    co2_edge::Edge{CO2}
end

GridElectricity(id, tr, grid_edge, elec_edge, co2_edge) =
    GridElectricity(id, tr, grid_edge, elec_edge, co2_edge)

function default_data(t::Type{GridElectricity}, id=missing, style="full")
    if style == "full"
        return full_default_data(t, id)
    else
        return simple_default_data(t, id)
    end
end


function full_default_data(::Type{GridElectricity}, id=missing)
    return OrderedDict(
        :id => id,
        :transforms => @transform_data(
            :timedata => "Electricity",
            :emission_rate => 0.0,
            :constraints => Dict(
                :BalanceConstraint => true
            ),
        ),
        :edges => Dict(
            :grid_elec_edge => @edge_data(
                :commodity => "Electricity"
            ),
            :elec_edge => @edge_data(
                :commodity => "Electricity"
            ),
            :co2_edge => @edge_data(
                :commodity => "CO2",
                :co2_sink => missing
            )
        )
    )
end


function simple_default_data(::Type{GridElectricity}, id=missing)
    return OrderedDict(
        :id => id,
        :location => missing,
        :emission_rate => 0.0,
        :co2_sink => missing,
        :electricity_commodity => missing
    )
end


function set_commodity!(::Type{GridElectricity}, commodity::Type{<:Commodity}, data::AbstractDict)
    return nothing
end

function make(::Type{GridElectricity}, data::AbstractDict{Symbol,Any}, system::System)

    id = AssetId(data[:id])
    @setup_data(GridElectricity, data, id)

    trans_key = :transforms
    @process_data(
        transform_data,
        data[trans_key],
        [
            (data[trans_key], key),
            (data[trans_key], Symbol("transform_", key)),
            (data, Symbol("transform_", key)),
            (data, key),
        ]
    )

    gridelectricity_transform = Transformation(;
        id = Symbol(id, "_", trans_key),
        timedata = system.time_data[:Electricity],
        constraints = transform_data[:constraints],
    )

    grid_key = :grid_elec_edge
    @process_data(
        grid_elec_edge_data,
        data[:edges][grid_key],
        [
            (data[:edges][grid_key], key),
            (data[:edges][grid_key], Symbol("grid_elec_", key)),
            (data, Symbol("grid_elec_", key)),
        ]
    )

    @start_vertex(
        grid_elec_start_node,
        grid_elec_edge_data,
        Electricity,
        [(grid_elec_edge_data, :start_vertex), (data, :location)]
    )

    grid_elec_edge = Edge(
        Symbol(id, "_", grid_key),
        grid_elec_edge_data,
        system.time_data[:Electricity],
        Electricity,
        grid_elec_start_node,
        gridelectricity_transform
    )

    elec_key = :elec_edge
    @process_data(
        elec_edge_data,
        data[:edges][elec_key],
        [
            (data[:edges][elec_key], key),
            (data[:edges][elec_key], Symbol("elec_", key)),
            (data, Symbol("elec_", key)),
        ]
    )

    @end_vertex(
        elec_end_node,
        elec_edge_data,
        Electricity,
        [(elec_edge_data, :end_vertex), (data, :location)]
    )

    elec_edge = Edge(
        Symbol(id, "_", elec_key),
        elec_edge_data,
        system.time_data[:Electricity],
        Electricity,
        gridelectricity_transform,
        elec_end_node
    )

    co2_key = :co2_edge
    @process_data(
        co2_edge_data,
        data[:edges][co2_key],
        [
            (data[:edges][co2_key], key),
            (data[:edges][co2_key], Symbol("co2_", key)),
            (data, Symbol("co2_", key)),
        ]
    )

    @end_vertex(
        co2_end_node,
        co2_edge_data,
        CO2,
        [(co2_edge_data, :end_vertex), (data, :co2_sink), (data, :location)]
    )

    co2_edge = Edge(
        Symbol(id, "_", co2_key),
        co2_edge_data,
        system.time_data[:CO2],
        CO2,
        gridelectricity_transform,
        co2_end_node
    )

    gridelectricity_transform.balance_data = Dict(
        :electricity => Dict(
            grid_elec_edge.id => 1.0,
            elec_edge.id      => 1.0
        ),
        :emissions => Dict(
            grid_elec_edge.id => get(transform_data, :emission_rate, 0.0),
            co2_edge.id       => 1.0
        )
    )


    return GridElectricity(id, gridelectricity_transform, grid_elec_edge, elec_edge, co2_edge)
end
