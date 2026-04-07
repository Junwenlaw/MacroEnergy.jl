# Adding Policy Constraints to a System

Currently, Macro supports two types of policy constraints:

- **CO₂ cap constraint**
- **CO₂ storage annual constraint**

The units of both constraints are determined by the stoichiometric balances used in assets with CO₂ emissions or injection to a CO₂ capture node.

The following sections describe the steps to add these constraints to a system:

- [Adding a CO₂ cap constraint](@ref)
- [Adding a CO₂ storage annual constraint](@ref)

!!! note "Nodes file"
    To add a policy constraint to a node, the user needs to edit the nodes file in the system (typically located at `system/nodes.json`). For more information about the nodes file, please refer to the [Adding a Node to a System](@ref) page.

## Adding a CO₂ cap constraint

To add a CO₂ cap constraint to a CO₂ node in a system, the user needs to:

1\. Activate the constraint by setting `CO2CapConstraint` to `true` in the `constraints` field of a node of type `CO2` in the nodes file:

```json
{
    "constraints": {
        "CO2CapConstraint": true
    }
}
```

2\. Set the CO₂ cap value using the `CO2CapConstraint` key in the `rhs_policy` field:

```json
{
    "rhs_policy": {
        "CO2CapConstraint": 0   // e.g. zero emissions
    }
}
```

3\. (Optional) Add a carbon price penalty using the `price_unmet_policy` field:

```json
{
    "price_unmet_policy": {
        "CO2CapConstraint": 200  // e.g. 200 USD/tonne penalty cost
    }
}
```

As a result, to add a CO₂ cap constraint to a `CO2` node in a system, the nodes file should have a `CO2` node with the following structure:

```json
{
    "type": "CO2",
    "instance_data": [
        {
            "id": "co2_sink",
            "constraints": {
                "CO2CapConstraint": true
            },
            "rhs_policy": {
                "CO2CapConstraint": 0
            },
            "price_unmet_policy": {
                "CO2CapConstraint": 200
            }
        },
        // other CO2 nodes
    ],
    "global_data": {
        // other attributes
    }
}
```

4\. Link the CO₂ node's **id** to the `co2_sink` key of assets in the system that have CO₂ emissions to track their CO₂ output:

```json
{
    "type": "ThermalPower",
    "instance_data": [
        {
            "co2_sink": "co2_sink",
            // other attributes
        }
    ],
    "global_data": {
        // other attributes
    }
}
```

Macro will automatically track all CO₂ emissions from assets connected to the `co2_sink` node and will constrain total emissions to the value specified in the `rhs_policy` key (unless a carbon price penalty is applied using the `price_unmet_policy` field).

## Adding a CO₂ storage annual constraint

The CO₂ storage annual constraint limits the amount of CO₂ that can be injected and stored in a `CO2Captured` node. 

To add a CO₂ storage annual constraint to a `CO2Captured` node in a system, the user needs to:

1\. Activate the constraint by setting `CO2StorageConstraint` to `true` in the `constraints` key of a node of type `CO2Captured`:

```json
{
    "constraints": {
        "CO2StorageConstraint": true
    }
}
```

2\. Set the CO₂ storage annual constraint value using the `CO2StorageConstraint` key in the `rhs_policy` field:

```json
{
    "rhs_policy": {
        "CO2StorageConstraint": 4753600
    }
}
```

Consequently, the `nodes.json` file should have a `CO2Captured` node with the following structure:

```json
{
    "type": "CO2Captured",
    "instance_data": [
        {
            "id": "co2_storage_1",
            "constraints": {
                "CO2StorageConstraint": true
            },
            "rhs_policy": {
                "CO2StorageConstraint": 4753600
            }
        },
        // other CO2Captured nodes
    ],
    "global_data": {
        // other attributes
    }
}
```

3\. Link the `CO2Captured` node's **id** to the `co2_storage` key of assets in the system with CO₂ storage edges to track their CO₂ storage:

```json
{
    "type": "CO2Injection",
    "instance_data": [
        {
            "co2_storage": "co2_storage_1",
            // other attributes
        }
    ],
    "global_data": {
        // other attributes
    }
}
```

Macro will automatically track all CO₂ injection from assets linked to the `co2_storage` node and constrain the total injection to the value set in the `rhs_policy` key.