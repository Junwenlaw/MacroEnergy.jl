# Investment costs

The cost of investment used by the model is the annualized investment cost of a technology, expressed in \$/MW-yr. The annualized investment cost represents an annual payment (also referred to as an annuity) made by a developer to debt and equity investors.

The annualized investment cost is calculated inside Macro. Users input the CAPEX of a technology (as the investment_cost attribute of an edge) and, if desired, a Weighted Average Cost of Capital (WACC) for a given technology $y$.

# Cost of capital (WACC) assumptions

- The $WACC_y$ input is meant to represent the *real* weighted average cost of capital. The WACC can be used to represent market-based costs of capital (typically based on a combination of debt and equity financing).
- If a WACC input is not provided, Macro will default to the general discount rate.

# Annualized investment cost calculation in Macro

Macro calculates the annualized investment cost internally using the following expressions.

```math
\text{CRF}_y = \frac{WACC_y}{1-(1+\text{WACC}_y)^{-EL_y}}
```

where CRF is the Capital Recovery Factor.
    
```math
\text{Annualized investment cost}_y = \text{CRF}_y \text{CAPEX}_{y,z}
```

# Single-period modeling

The investment cost used in the objective function is based on the annuity paid in the modeled year; i.e. the annuities paid in non-modeled years are ignored.

# Multi-period modeling

**End of horizon effects** are treated such that annuities are paid in every year from the time of investment until the end of the technology's economic lifetime, or the remainder of the modeling horizon, whichever comes first. Thus the model does not include salvage values at the end of the modeling horizon. As this implies that the model ignores a technology's value after the modeling horizon, the model also ignores costs incurred after the modeling horizon. 
    
Each annuity is discounted using the general discount rate, which represents the time value of money from the system planner's perspective (see Multi period accounting for more details). Thus, Technologies with the same CAPEX but different costs of capital (i.e., $\text{WACC}_y$) will be treated differently by the model.
    