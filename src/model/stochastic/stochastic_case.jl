"""
    StochasticScenario

One weather-year realization in a stochastic capacity expansion problem.

# Fields
- `id::Int`: Unique scenario identifier (used as `period_index` in all TimeData)
- `probability::Float64`: Probability weight (all must sum to 1.0)
- `system::System`: Full operational system for this realization — shared asset topology,
  scenario-specific time series (demand, availability, fuel prices)
"""
struct StochasticScenario
    id::Int
    probability::Float64
    system::System
end

"""
    StochasticCase

A stochastic capacity expansion case: ONE investment decision shared across multiple
weather-year operational realizations.

Objective: `C_invest(x) + Σ_r p_r · opexmult · Σ_{w∈r} C_op(x, w)`

Policy constraint mode (controlled by `stochastic_settings.PolicyMode`).
Applies to all `PolicyConstraint` types with linking variables: CO2Cap, CO2Storage,
RenewableShare. CO2Price is a cost-only constraint and is always probability-weighted
in the objective automatically.

- `"expected"` (default): one probability-weighted constraint coupling all scenarios.
  Enables cross-scenario trade-offs and fully leverages cross-scenario Benders grouping.
- `"per_realization"`: each scenario must independently satisfy each policy constraint.

# Fields
- `scenarios::Vector{StochasticScenario}`: Ordered scenario realizations
- `settings::NamedTuple`: Shared macro settings (BendersSettings, SolutionAlgorithm, etc.)
- `stochastic_settings::NamedTuple`: `(PolicyMode,)`
"""
struct StochasticCase
    scenarios::Vector{StochasticScenario}
    settings::NamedTuple
    stochastic_settings::NamedTuple
end

number_of_scenarios(sc::StochasticCase) = length(sc.scenarios)
get_scenarios(sc::StochasticCase) = sc.scenarios
get_settings(sc::StochasticCase) = sc.settings
solution_algorithm(sc::StochasticCase) = solution_algorithm(sc.settings[:SolutionAlgorithm])

"""
    investment_system(sc::StochasticCase) -> System

Return the reference system used for investment-level planning. All scenarios share the
same asset topology and investment data, so the first scenario's system is used.
"""
investment_system(sc::StochasticCase) = sc.scenarios[1].system

"""
    set_scenario_id!(system, scenario_id)

Override the `period_index` field in every commodity's TimeData to `scenario_id`.
This is called after loading each scenario so that Benders budget variable names
(`vCO2CapConstraint_Budget_nodeid_period{scenario_id}`) are unique across scenarios.
"""
function set_scenario_id!(system::System, scenario_id::Int)
    for c in keys(system.time_data)
        system.time_data[c].period_index = scenario_id
    end
    # Propagate to all nodes and edges inside assets/locations.
    # A single visited set is shared across all traversals to prevent cycles
    # (e.g. Edge{Electricity} ↔ Storage{Electricity} cross-reference).
    visited = Set{UInt64}()
    for loc in system.locations
        _set_period_index!(loc, scenario_id, visited)
    end
    for asset in system.assets
        _set_period_index!(asset, scenario_id, visited)
    end
    return system
end

function _set_period_index!(obj, scenario_id::Int, visited::Set{UInt64}=Set{UInt64}())
    oid = objectid(obj)
    oid in visited && return
    push!(visited, oid)
    for field in Base.fieldnames(typeof(obj))
        val = getfield(obj, field)
        if val isa TimeData
            val.period_index = scenario_id
        elseif val isa AbstractAsset || val isa AbstractVertex ||
               val isa AbstractEdge  || val isa AbstractStorage
            _set_period_index!(val, scenario_id, visited)
        end
    end
end
