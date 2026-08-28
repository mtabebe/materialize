# Synthetic catalog state

A demo of the synthetic catalog state toolkit, which populates a catalog with fake
objects, history and statistics so a situation can be modelled instead of built. It
doubles as the toolkit's regression net: every scene asserts what it showed.

```
bin/mzcompose --find synthetic-catalog run default
```

Both fleets are sized for CI. To see the numbers the toolkit is actually for:

```
bin/mzcompose --find synthetic-catalog run default --objects 50000 --dataflows 2000
```

The environment it runs against is destroyed by the objects it injects, which is why the
toolkit refuses to run unless the environment says so:
`ALTER SYSTEM SET enable_synthetic_catalog_state = on`, plus unsafe mode.
