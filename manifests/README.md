# Repository Manifests

Checked-in source manifests live here using the layout frozen in `docs/manifests.md`:

```text
manifests/<namespace>/<source_family>/<version>.toml
```

Alternate deployment-profile manifests use the same schema under profile-specific roots. The first ENSv2 Sepolia dev profile is documented, but not activated here:

```text
manifests-sepolia-dev/<namespace>/<source_family>/v1.toml
```

Bootstrap seed manifests are now checked in for the first ENS and Basenames source families.

Current policy:

- active manifests should contain only authoritative contract addresses we are ready to watch
- draft manifests may reserve shape for future source families without activating intake
- manifest changes must stay within the schema frozen in `docs/manifests.md`
- a runtime selects exactly one manifest profile root; `manifests/` and `manifests-sepolia-dev/` must not be loaded into the same canonical corpus, watch plan, discovery graph, or projection set
