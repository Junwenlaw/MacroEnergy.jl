"""
    load_stochastic_case(path) -> StochasticCase

Load a stochastic capacity expansion case from a directory or JSON file.

The entry-point file (`stochastic_data.json`) format:
```json
{
  "scenarios": [
    { "id": 1, "probability": 0.143, "system_data": "scenarios/year_2015/system_data.json" },
    { "id": 2, "probability": 0.143, "system_data": "scenarios/year_2016/system_data.json" },
    ...
  ],
  "settings": "settings/macro_settings.json",
  "stochastic_settings": "settings/stochastic_settings.json"
}
```

The `stochastic_settings.json` format:
```json
{
  "PolicyMode": "expected"
}
```

Each scenario folder must contain scenario-specific time series CSVs
(demand, availability, fuel prices) and a `system_data.json` that references
the **shared** asset JSON files at the top level of the case directory.

## Directory layout example
```
my_stochastic_case/
  settings/
    stochastic_data.json
    macro_settings.json
    stochastic_settings.json
  assets/           ← shared asset definitions (JSON)
  system/           ← shared node/commodity definitions
  scenarios/
    year_2015/
      system_data.json   ← points to shared assets + scenario-specific time series
      system/
        demand.csv
        availability.csv
        fuel_prices.csv
        time_data.json
    year_2016/
      ...
```
"""
function load_stochastic_case(path::AbstractString)::StochasticCase
    if isdir(path)
        case_dir = abspath(path)
        path = joinpath(case_dir, "settings", "stochastic_data.json")
    else
        # File path provided directly — case_dir is two levels up if inside settings/
        abs_path = abspath(path)
        case_dir = basename(dirname(abs_path)) == "settings" ?
            dirname(dirname(abs_path)) : dirname(abs_path)
    end
    isfile(path) || error("stochastic_data.json not found at: $path")
    raw = JSON3.read(read(path, String))

    # --- Case-level settings (DiscountRate, PeriodLengths, SolutionAlgorithm, BendersSettings) ---
    case_settings_path = rel_or_abs_path(String(raw.case_settings), case_dir)
    case_settings = configure_case(case_settings_path, case_dir)

    # --- Macro settings (ConstraintScaling, OutputLayout, …) ---
    settings_path = rel_or_abs_path(String(raw.settings), case_dir)
    macro_settings = configure_settings(settings_path, case_dir)

    # Merge: case_settings dominates for any key clash
    settings = merge(macro_settings, case_settings)

    # --- Stochastic-specific settings ---
    stoch_path = rel_or_abs_path(String(raw.stochastic_settings), case_dir)
    stochastic_settings = _load_stochastic_settings(stoch_path)

    # --- Validate probabilities ---
    total_prob = sum(Float64(sc.probability) for sc in raw.scenarios)
    if !isapprox(total_prob, 1.0; atol = 1e-6)
        @warn("Scenario probabilities sum to $total_prob, not 1.0. Check your stochastic_data.json.")
    end

    # --- Load each scenario system ---
    scenarios = StochasticScenario[]

    for sc_raw in raw.scenarios
        sc_id   = Int(sc_raw.id)
        sc_prob = Float64(sc_raw.probability)
        sc_path = rel_or_abs_path(String(sc_raw.system_data), case_dir)

        @info("Loading stochastic scenario $sc_id (p=$sc_prob) from $sc_path")

        sc_case_data = load_case_data(sc_path; lazy_load = true)
        # Each scenario is a single-period case; extract the one system
        sc_case = generate_case(sc_path, sc_case_data)
        sc_system = sc_case.systems[1]

        # Tag every TimeData entry with scenario_id so that planning-problem
        # budget variable names are unique across scenarios.
        set_scenario_id!(sc_system, sc_id)

        push!(scenarios, StochasticScenario(sc_id, sc_prob, sc_system))
    end

    @info("StochasticCase loaded: $(length(scenarios)) scenarios, " *
          "PolicyMode = $(stochastic_settings.PolicyMode)")

    return StochasticCase(scenarios, settings, stochastic_settings)
end

"""
    _load_stochastic_settings(path) -> NamedTuple

Read `stochastic_settings.json` and return a typed NamedTuple.

| Key         | Default        | Description                                                  |
|-------------|----------------|--------------------------------------------------------------|
| PolicyMode  | "expected"     | "expected" or "per_realization"                              |

Applies to all `PolicyConstraint` types with linking variables (CO2Cap, CO2Storage,
RenewableShare). CO2Price has no linking variables and is always probability-weighted
in the objective automatically — it is unaffected by this setting.

- `"per_realization"`: each scenario must independently satisfy each policy constraint.
- `"expected"`: probability-weighted constraints coupling all scenarios.
"""
function _load_stochastic_settings(path::AbstractString)::NamedTuple
    isfile(path) || error("Stochastic settings file not found: $path")
    raw = JSON3.read(read(path, String))

    mode = String(get(raw, :PolicyMode, "expected"))
    mode in ("expected", "per_realization") ||
        error("PolicyMode must be \"expected\" or \"per_realization\", got \"$mode\"")

    return (PolicyMode = mode,)
end
