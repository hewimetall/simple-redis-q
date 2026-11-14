# Research index (architectural fork)

| Document | Purpose |
| --- | --- |
| [example/](example/README.md) | Fork contracts. Excluded from the workspace. Not linked |
| [baseline.md](baseline.md) | Pinned SHAs, versions, licenses, confirmed vs hypothesis |
| [capability-matrix.md](capability-matrix.md) | capability → current → candidate → coverage → gap → own code |
| [candidates.md](candidates.md) | Pinned Apalis 1.1.1 / Oxana 2.5.0 / watchexec 6.0.0 |
| [spikes/RESULTS.md](spikes/RESULTS.md) | Compile/run proofs + Redis v1.2.0 test log |
| [../architecture/plan.md](../architecture/plan.md) | Stage B plan |
| [../architecture/contracts.md](../architecture/contracts.md) | Binding Attempt / QueuePort / drain |
| [ADR 0001](../adr/0001-ports-and-adapters.md) | Architecture decision records |
| [../testing/strategy.md](../testing/strategy.md) | AAA and test levels |

**Baseline tag:** `v1.2.0` @ `9164eeab0361639d8cd2d0ca2bb91c355710c1a8`  
Main is past that tag. Shipping behavior is [architecture.md](../architecture.md). The [example](example/README.md) is not a workspace member and is not linked into `agent-worker`.  
**Do not** treat `1.0.0rc7` bench results as `1.2.0` proofs.
