# Retired identity façade benchmark notes

The 2026-05 measurements in the former version of this file covered deleted v1
routes and deleted supporting tables. They are historical Git evidence only and
must not be used for a serving or release decision.

The current `/v2/lookup` and list-route performance gate is the
[production-scale benchmark release gate](../runbooks/benchmark-gate.md). Its
checked-in throughput and percentile limits live in
[`benchmarks/release-gate.toml`](../../benchmarks/release-gate.toml), and its
requests use names, addresses, filters, search substrings, and cursors drawn
from current production-shaped projections.
