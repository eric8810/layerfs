# LayerFS as a rutis plugin

> **Status:** Proposal; target LayerFS 1.0.0; not a released contract.

The implementation is in the same change: `sdk/js/src/rutis/`, the
`@layerfs/sdk/rutis` export, and `sdk/js/test/rutis*.mjs`. This document records
the boundary and the decisions a reviewer should check before release.

## Goal

Let a [rutis](https://github.com/arcships/rutis) host or plugin use one LayerFS
project through rutis services, without learning LayerFS's daemon, Sandbox,
Store, FUSE or native-binding layout.

Non-goal: make LayerFS itself a rutis process, move the daemon into rutis, or
replace the dsh LayerFS UI. The product remains the same; this is an adapter
over the public JavaScript SDK.

## Why an adapter

The JavaScript SDK is the product's public surface
([`sdk/js/README.md`](../../../sdk/js/README.md)): 27 grouped methods plus
`call` and `close`. Its native binding already owns Sandbox creation, Store
opening, daemon authentication, Workspace mount and Commit. A rutis plugin only
has to:

1. validate one row's configuration;
2. expose the SDK table under stable service names;
3. give the services a lifecycle (apply, cleanup);
4. serialize calls that the SDK requires to be sequential.

Writing a Rust plugin over `layerfs-api` would duplicate the binding's
deployment and authentication work. It would also put rutis's MSRV (1.88 at the
time of writing) into the core workspace (1.85). A Rust plugin or dylib remains
possible later if an in-process Rust host needs LayerFS without Node; that is
not this proposal.

## Placement

`@layerfs/sdk/rutis` is a subpath export of the existing SDK package:

```text
sdk/js/src/rutis/
  index.js       definePlugin wrapper and public exports
  plugin.js      service table, configuration, lifecycle (no rutis import)
```

`plugin.js` has no rutis import, so the service table, forwarding, lanes,
refusal mapping and cleanup are testable without a host. `index.js` is the only
module that imports `@arcships/rutis`; that package is an optional peer and a
test-only development dependency.

This placement keeps one version for the SDK and its adapter: a change to the
SDK method table and the adapter's method table lands in the same commit. The
`tools/production_loc.py` `sdk` scope already counts `sdk/js/src`, so the
adapter does not create a new product scope.

## Services

| Service | Methods | SDK source |
| --- | --- | --- |
| `layerfs` | `info()`, `call(method, args)` | client identity; Store method table (`fs.*`, `branch.*`, `project.list_branches`) |
| `layerfs.project` | `init`, `listBranches` | `project` |
| `layerfs.fs` | `read`, `list`, `stat`, `grep`, `glob`, `diffFiles`, `blame`, `write`, `edit`, `apply`, `mkdir`, `rm`, `cp`, `mv` | `fs` |
| `layerfs.branch` | `fork`, `listCommits`, `diffCommits`, `diffBranches`, `merge`, `publish` | `branch` |
| `layerfs.workspace` | `mount`, `exec`, `status`, `commit`, `unmount` | `workspace` |

Service methods are camelCase aliases of the SDK's snake_case methods.
`layerfs.call` avoids a second method table for Store methods when a consumer
needs the exact SDK name. `project.init` and Workspace methods use their group
services: the SDK's `call` opens the Store first, which a fresh
`project.init` cannot do.

`close` is deliberately not a service method. The Store and the client belong
to the plugin row; one consumer must not close the client another consumer is
using. Unload runs `close` once.

### Naming

The names are stable strings, not language- or machine-specific. A Python, Go
or Node consumer injects the same names. A provider replacement is therefore a
matter of supplying another implementation of the same names.

## Lifecycle

`apply(ctx, config)`:

1. Reject non-Linux processes with `Unsupported`. Store calls are Linux-only;
   the service must not appear healthy on a platform where every Store call
   would fail.
2. Build a lazy `LayerFS`. `apply` performs no Store I/O, so `project.init` can
   install a Store that does not exist yet.
3. Provide the five services.
4. Return a cleanup that drains Workspace lanes and closes the client.

Unload:

- closes the Store; it does not Commit;
- does not unmount a Workspace; only an explicit `workspace.unmount` ends one;
- does not cancel a command. The SDK's `exec` has no cancellation route, and a
  process exit is not proof that children are gone.

Each call is attempted once. The adapter adds no retry, busy handler, replay or
fallback. A refusal keeps its stable `code`; `LayerFSError.detail` crosses as
the rutis error `data`.

## Concurrency

The SDK requires one Workspace's calls to be sequential. The adapter keeps one
promise lane per `{sandbox, path}`:

- `exec`, `status`, `commit` and `unmount` on one Workspace run in order;
- different Workspaces run in parallel;
- `mount` is not lanned; the SDK already serializes placement per project.

`fs`, `branch`, `project` and `call` use the SDK's Store concurrency directly.
The adapter holds no Store lock and does not sit between the Store's own
publication lane and the caller.

## Values and errors

- Group methods keep the SDK's positional arguments.
- `layerfs.call(method, args)` forwards the SDK's argument object unchanged.
- A `LayerFSError` keeps `code`, `message`, `detail` (`data` after crossing).
- Calls emit nothing by default. With `events.calls: true`, each call emits
  `layerfs/call` with `{ method, ok, ms, code? }`; arguments, file content and
  messages are never included.
- The adapter does not expose the operator surface. A consumer that needs
  operator diagnostics should get a separate, explicitly enabled service.

## Configuration and platform

Required: `project`, `home`, `storePath`.

Optional: `storePolicy`, `trace`, `deploy` (`docker`, `image`, `volume`,
`daemon`, `daemonHost`, `containerPrefix`), `events.calls`.

The SDK reads `home` and deployment from the documented `LAYERFS_*`
environment, and reads `storePath`, `storePolicy` and `trace` from constructor
options. The adapter therefore applies `home` and `deploy` to the environment
before constructing the client. A Node process has one environment, so one
rutis Node runtime serves one project. Changing the project in a live runtime
requires a new runtime; the adapter refuses to overwrite a different existing
value.

Linux is required because Store calls are Linux-only. On macOS the same plugin
runs in a Linux rutis remote runtime; the host links to that runtime. This is
the same topology as the dsh Store container, expressed with rutis's own remote
runtime instead of a bespoke worker.

The SDK is currently installed from this checkout, as the dsh bundle does
(`link:<repo>/sdk/js`). A published SDK would make the row installable from
npm without changing the service contract.

## Limits

- `workspace.exec` has no Writable sinks at the service boundary; stdout and
  stderr cross as text. Large output needs a dedicated executor or a file-based
  route, not one unbounded rutis value.
- One project per rutis Node runtime (environment-scoped deployment).
- The operator surface is not exposed.
- This change has unit tests with a fake client and a rutis test host. It does
  not run a real Store, Sandbox, daemon or FUSE mount; those require the Linux
  build and Docker environment.
- The SDK package is private; the export is usable through the repository link,
  not from npm yet.

## Alternatives considered

| Alternative | Why not now |
| --- | --- |
| Rust plugin over `layerfs-api` | duplicates binding deployment/authentication; raises MSRV; more code for the same contract |
| dylib plugin | hot-swap is not required for an adapter that a Node runtime already reloads |
| separate repository | the SDK is private and not published; the adapter must change with the SDK method table |
| rutis inside LayerFS core | makes rutis a product dependency; the adapter is an application integration |
| dsh UI replacement | the LayerFS UI remains the dsh plugin |

## Verification

- `node --test sdk/js/test/rutis.mjs`: service table, forwarding, lanes,
  refusal code/detail, cleanup drain and close.
- `node --test sdk/js/test/rutis_host.mjs`: a real rutis test host loads the
  declared services, calls one, and unloads them.
- `rutis-host check` remains the deployment check once the package resolves in
  the runtime project.

## Open review questions

1. Are the five service names and the camelCase aliases the right public
   contract, or should `layerfs.call` be the only exposed surface?
2. Should the environment-based one-project-per-runtime limit be accepted for
   1.0.0, or should the SDK constructor start honoring `options.home` and
   `options.deploy` so one runtime can serve several projects?
3. Should `workspace.exec` get a bounded sink/file route before this is
   published, or is returning text acceptable for the first adapter release?
4. Should the operator surface become an explicitly enabled sixth service?
