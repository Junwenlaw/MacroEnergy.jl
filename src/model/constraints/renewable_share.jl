Base.@kwdef mutable struct RenewableShareConstraint <: PolicyConstraint
    value::Union{Missing,Vector{Float64}} = missing
    constraint_dual::Union{Missing,Vector{Float64}} = missing
    constraint_ref::Union{Missing,Nothing} = missing
end

@doc raw"""
    add_policy_linking_vars!(ct::RenewableShareConstraint, n::Node{Electricity}, model::Model)

Create one budget variable array per subperiod for the annual renewable share constraint:
- `vRenewableShareConstraint_VREBudget[w]`: weighted VRE generation in subperiod `w`

This linking variable connects the planning problem to the operational subproblems in
Benders decomposition. The planning constraint enforces the annual VRE generation
requirement against the fixed annual demand scalar.
"""
function add_policy_linking_vars!(ct::RenewableShareConstraint, n::Node{Electricity}, model::Model)
    ct_type = typeof(ct)
    n.policy_budgeting_vars[Symbol(string(ct_type) * "_VREBudget")] = @variable(
        model,
        [w in subperiod_indices(n)],
        base_name = "v" * string(ct_type) * "_VREBudget_$(id(n))_period$(period_index(n))"
    )
end

@doc raw"""
    add_policy_planning_constraint!(ct::RenewableShareConstraint, n::Node{Electricity}, model::Model)

No-op for `RenewableShareConstraint`. The global planning constraint is created once
across ALL electricity nodes with this constraint by
`add_global_renewable_share_constraint!(system, model)` (called from
`planning_model!(system, model)`), which aggregates VRE budgets and demands
system-wide:

```math
\sum_{\text{nodes}} \sum_{w} \text{vVREBudget}_{n,w}
    \geq X \cdot \sum_{\text{nodes}} \sum_{w} \sum_{t \in w} \omega(w) \cdot \text{demand}_n(t)
```

This ensures the constraint is system-wide (total VRE ≥ X% of total demand), not
enforced per-node.
"""
function add_policy_planning_constraint!(ct::RenewableShareConstraint, n::Node{Electricity}, model::Model)
    return nothing  # global constraint created at system level
end

@doc raw"""
    add_model_constraint!(ct::RenewableShareConstraint, n::Node{Electricity}, model::Model)

Enforce an annual renewable generation share requirement on an electricity node `n`.

The constraint requires that total VRE generation is at least `X%` of weighted annual
electricity demand:

```math
\sum_{w} \sum_{t \in w} \omega(w) \cdot \text{VRE\_gen}(t)
    \geq X \cdot \sum_{w} \sum_{t \in w} \omega(w) \cdot \text{demand}(t)
```

where `X = rhs_policy(n, RenewableShareConstraint)` and the right-hand side is a fixed
scalar computed from the exogenous demand time series (not a variable).

In Benders decomposition, per-subperiod budget variables `vVREBudget[w]` link the
operational VRE generation to the planning problem via the equality constraint
`vre_subperiod[w] == vVREBudget[w]`. This equality is always feasible because
`vVREBudget[w]` is a free variable in the planning problem — no Benders slack
relaxation is needed.

VRE generation is tracked via the `:vre_demand` balance on the electricity node, which
is populated only with VRE edge flows by `initialize_vre_balance_data!`.

If `price_unmet_policy` is also specified, a slack variable is added per subperiod to
allow VRE shortfall at a penalty cost (soft constraint).

**User configuration** (on the Electricity node in nodes.json):
```json
{
  "constraints": { "BalanceConstraint": true, "RenewableShareConstraint": true },
  "rhs_policy": { "RenewableShareConstraint": 0.50 },
  "price_unmet_policy": { "RenewableShareConstraint": 1000.0 }
}
```
"""
function add_model_constraint!(ct::RenewableShareConstraint, n::Node{Electricity}, model::Model)
    ct_type = typeof(ct)

    # Per-subperiod accumulator for weighted VRE generation
    vre_subperiod = @expression(model, [w in subperiod_indices(n)], 0 * model[:vREF])

    for t in time_interval(n)
        w = current_subperiod(n, t)
        add_to_expression!(
            vre_subperiod[w],
            subperiod_weight(n, w),
            get_balance(n, :vre_demand, t),
        )
    end

    # Optional soft constraint: allow VRE shortfall at a penalty cost
    if haskey(price_unmet_policy(n), ct_type)
        n.policy_slack_vars[Symbol(string(ct_type) * "_Slack")] = @variable(
            model,
            [w in subperiod_indices(n)],
            lower_bound = 0.0,
            base_name = "v" * string(ct_type) * "_Slack_$(id(n))_period$(period_index(n))"
        )
        for w in subperiod_indices(n)
            add_to_expression!(
                model[:eVariableCost],
                subperiod_weight(n, w) * price_unmet_policy(n, ct_type),
                n.policy_slack_vars[Symbol(string(ct_type) * "_Slack")][w],
            )
            # Slack adds to VRE side: allows the share constraint to be relaxed
            add_to_expression!(
                vre_subperiod[w],
                n.policy_slack_vars[Symbol(string(ct_type) * "_Slack")][w],
            )
        end
    end

    # Equality constraint linking operation to planning budget variable.
    # vVREBudget[w] is a free variable in the planning problem, so this equality
    # constraint is always feasible and needs no Benders slack relaxation.
    # constraint_ref is left as missing for this reason.
    vVREBudget = n.policy_budgeting_vars[Symbol(string(ct_type) * "_VREBudget")]
    @constraint(
        model,
        [w in subperiod_indices(n)],
        vre_subperiod[w] == vVREBudget[w]
    )
end

@doc raw"""
    update_policy_planning_solution!(ct::RenewableShareConstraint, n::Node, planning_variable_values::Dict)

Benders-specific: replace the `_VREBudget` variable array with the fixed Float64 values
from the planning solution, so that the subproblem equality constraint
`vre_sum[w] == V_w` uses the VRE budget target decided by the planning problem.
"""
function update_policy_planning_solution!(ct::RenewableShareConstraint, n::Node, planning_variable_values::Dict)
    ct_type = typeof(ct)
    key = Symbol(string(ct_type) * "_VREBudget")
    variable_ref = copy(n.policy_budgeting_vars[key])
    n.policy_budgeting_vars[key] = [
        planning_variable_values[name(variable_ref[w])] for w in subperiod_indices(n)
    ]
end
