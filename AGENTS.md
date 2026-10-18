# Orchestrator maintenance

- This Go module owns `delegate`, `collab`, `autonomy` and `task` engines.
- Keep production dependencies within this module and the Go standard library.
  Inject Agent execution, sessions and filesystem policy; do not import the
  host, tool SDK, API, gateways or model providers.
- The tool SDK owns schemas, permissions and execution-context adapters.
- Preserve task JSON, queue snapshots, session IDs and recovery semantics when
  changing implementation ownership. Foreground execution also uses autonomy
  persistence and must remain isolated from background dispatch.
- Keep `docs/architecture.md` aligned with execution and dependency changes.
- Existing tests were migrated from tools; follow the user's verification
  instructions before adding or running tests.
