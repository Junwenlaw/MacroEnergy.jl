@doc raw"""
    CO2PriceConstraint

A carbon tax applied at a CO2 sink node. Unlike [`CO2CapConstraint`](@ref), this
adds a linear cost term to the objective function:

```math
\sum_{t \in \text{time\_interval(n)}} \text{subperiod\_weight}(n,w) \cdot \text{carbon\_price} \cdot \text{emissions}(n, t)
```

without imposing a hard emissions cap or introducing budget linking variables.

**Benders advantage:** Because no linking (budget) variables are created, the subproblem
dual vector contains only capacity duals. Adaptive group clustering therefore groups
subperiods based on similar capacity dual patterns alone, which is the regime where
grouped Benders cuts outperform multi-cut.

**Configuration in `nodes.json`:**

Set `"CO2PriceConstraint": true` in the node's `constraints`, and set the carbon price
(in the same cost units as other variable costs, per unit CO2) in `price_unmet_policy`:

```json
{
  "type": "CO2",
  "global_data": {
    "time_interval": "CO2",
    "price_unmet_policy": {
      "CO2PriceConstraint": 50.0
    }
  },
  "instance_data": [
    {
      "id": "co2_sink",
      "constraints": { "CO2PriceConstraint": true }
    }
  ]
}
```

No `rhs_policy` is needed (there is no hard emissions cap).

!!! note "Mutually exclusive with CO2CapConstraint"
    Use either `CO2PriceConstraint` (carbon tax) or `CO2CapConstraint` (hard cap) on a
    given CO2 node. Both can coexist if desired (cap + tax above the cap), but the
    typical use case is one or the other.
"""
Base.@kwdef mutable struct CO2PriceConstraint <: OperationConstraint
    constraint_ref::Union{Missing,Nothing} = missing
end

function add_model_constraint!(ct::CO2PriceConstraint, n::Node{CO2}, model::Model)
    ct_type = typeof(ct)

    if !haskey(price_unmet_policy(n), ct_type)
        @warn("CO2PriceConstraint on node \"$(id(n))\" has no carbon price set in price_unmet_policy. Skipping.")
        ct.constraint_ref = nothing
        return nothing
    end

    carbon_price = price_unmet_policy(n, ct_type)

    for t in time_interval(n)
        w = current_subperiod(n, t)
        add_to_expression!(
            model[:eCO2PriceCost],
            subperiod_weight(n, w) * carbon_price,
            get_balance(n, :emissions, t),
        )
    end

    ct.constraint_ref = nothing
    return nothing
end
