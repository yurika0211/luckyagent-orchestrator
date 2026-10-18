# LuckyAgent Orchestrator

Independent Go engines for delegated tasks, multi-agent collaboration and
durable background execution. The module runs inside its embedding process.
Applications supply agent execution, sessions, model/tool access and lifecycle.

```text
github.com/yurika0211/luckyagent-orchestrator
```

## Packages

| Package | Ownership |
| --- | --- |
| `delegate` | Ordinary child tasks, asynchronous execution, waiting, cancellation, parallel delegation, execution callbacks and session ownership |
| `collab` | Pipeline/parallel/debate orchestration, agent registry, Planner/MDP, aggregation, blackboard and messages |
| `autonomy` | Persistent priority queue, worker pool, heartbeat, checkpoints, retries and continuation budgets |
| `task` | Task records, cost/outcome values, events, observations, policy and file storage |

Production packages depend only on the Go standard library and this module.
They do not import LuckyAgent, its tool SDK, API, gateways or model providers.
Existing engine tests moved with their implementations; tool registration and
permission tests stay in `luckyagent-tools`.

## Integration

- `delegate.NewDelegateManager` accepts `DelegateConfig` and an optional
  `WorkspacePolicy`. Inject `SetAgentExecutorWithOptions`, `SetSessionInitializer`
  and `SetTaskStore` as needed. Public `Handle*` methods accept plain values and
  argument maps without any dependency on a particular tool protocol.
- `collab.NewDelegateManager` accepts a registry and `TaskHandler`; handlers may
  implement `SubTaskVerifier` for substantive output validation.
- `autonomy.NewAutonomyKit` accepts `AutonomyConfig` and `AgentExecutor`. The
  embedding host owns starting/stopping it and reporting results.
- The standalone delegate workspace default permits the OS temporary directory.
  LuckyAgent's tool adapter injects the existing sandbox validation/expansion
  callbacks and keeps its prior workspace behavior. Callbacks must be safe for
  concurrent use. Tool authorization remains the host's responsibility.

The engines do not create an LLM client or choose a model. A worker is an
in-process executor with an isolated task session, rather than a separate OS
process or network service.

See [architecture and migration](docs/architecture.md) for ownership, storage,
compatibility and the local development setup.

## Build

Requires Go 1.25 or newer. The independent module builds without a workspace:

```bash
GOWORK=off go build ./...
```

The initial module version is `v0.1.0`. Consumers pin released tags or published
commit versions in `go.mod`. An ignored local `go.work` is optional for joint
development across repositories.

## Provenance

Extracted from `luckyagent-tools` commit
`31ef6846a4343ee0f1b649615202a31cd7407f4c`. The MIT license and original
attribution are retained. No new runtime configuration fields are introduced.
