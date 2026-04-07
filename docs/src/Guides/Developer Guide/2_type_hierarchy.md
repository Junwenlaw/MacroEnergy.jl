# Macro type hierarchy

```@meta
CurrentModule = MacroEnergy
```

### Commodity Types
```@example type_hierarchy
using MacroEnergy # hide
using AbstractTrees # hide
using InteractiveUtils # hide
AbstractTrees.children(d::DataType) = subtypes(d) # hide
print_tree(Commodity)
```

### Asset Types
```@example type_hierarchy
print_tree(AbstractAsset)
```

### Constraint Types
```@example type_hierarchy
print_tree(MacroEnergy.AbstractTypeConstraint)
```


