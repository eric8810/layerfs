// The rutis plugin spec of the LayerFS SDK: five named services over the
// public SDK table, and their lifecycle. This module has no rutis import so
// its service table, forwarding and cleanup can be tested without a host;
// `index.js` wraps the spec with @arcships/rutis's definePlugin.
import { LayerFS, LayerFSError } from "../index.js";

// Service method -> SDK method. The service names camelCase; the SDK keeps
// its published snake_case names.
const GROUP_METHODS = Object.freeze({
  project: Object.freeze({ init: "init", listBranches: "list_branches" }),
  fs: Object.freeze({
    read: "read",
    list: "list",
    stat: "stat",
    grep: "grep",
    glob: "glob",
    diffFiles: "diff_files",
    blame: "blame",
    write: "write",
    edit: "edit",
    apply: "apply",
    mkdir: "mkdir",
    rm: "rm",
    cp: "cp",
    mv: "mv",
  }),
  branch: Object.freeze({
    fork: "fork",
    listCommits: "list_commits",
    diffCommits: "diff_commits",
    diffBranches: "diff_branches",
    merge: "merge",
    publish: "publish",
  }),
});

const WORKSPACE_METHODS = Object.freeze([
  "mount",
  "exec",
  "status",
  "commit",
  "unmount",
]);

const shape = (methods) =>
  Object.fromEntries(methods.map((method) => [method, "async"]));

/** What the plugin declares to rutis, before it runs. */
export const SERVICE_METHODS = Object.freeze({
  layerfs: Object.freeze({ info: "async", call: "async" }),
  "layerfs.project": Object.freeze(shape(Object.keys(GROUP_METHODS.project))),
  "layerfs.fs": Object.freeze(shape(Object.keys(GROUP_METHODS.fs))),
  "layerfs.branch": Object.freeze(shape(Object.keys(GROUP_METHODS.branch))),
  "layerfs.workspace": Object.freeze(shape(WORKSPACE_METHODS)),
});

/** The row configuration, validated by the rutis host before `apply`. */
export const CONFIG_SCHEMA = Object.freeze({
  type: "object",
  additionalProperties: false,
  required: ["project", "home", "storePath"],
  properties: {
    project: {
      type: "string",
      pattern: "^[a-z0-9](?:[a-z0-9_.-]{0,61}[a-z0-9])?$",
    },
    home: { type: "string", minLength: 1 },
    storePath: { type: "string", minLength: 1 },
    storePolicy: {
      type: "object",
      additionalProperties: false,
      properties: {
        smallFileThreshold: { type: "integer", minimum: 1 },
        wholeFileDepth: { type: "integer", minimum: 0 },
        chunkDepth: { type: "integer", minimum: 0 },
        metadataDepth: { type: "integer", minimum: 0 },
      },
    },
    trace: { type: "string", minLength: 1 },
    deploy: {
      type: "object",
      additionalProperties: false,
      properties: {
        docker: { type: "string" },
        image: { type: "string" },
        volume: { type: "string" },
        daemon: { type: "string" },
        daemonHost: { type: "string" },
        containerPrefix: { type: "string" },
      },
    },
    events: {
      type: "object",
      additionalProperties: false,
      properties: { calls: { type: "boolean" } },
    },
  },
});

const DEPLOY_ENV = Object.freeze({
  docker: "DOCKER",
  image: "IMAGE",
  volume: "VOLUME",
  daemon: "DAEMON",
  daemonHost: "DAEMON_HOST",
  containerPrefix: "CONTAINER_PREFIX",
});

// The SDK reads home and deployment from LAYERFS_*; it reads only storePath,
// storePolicy and trace from its constructor options. A Node process has one
// set of environment variables, so one rutis Node runtime serves one project.
function configureEnvironment(config) {
  const set = (name, value) => {
    if (value === undefined || value === "") return;
    const key = `LAYERFS_${name}`;
    const previous = process.env[key];
    if (previous !== undefined && previous !== "" && previous !== value) {
      throw new LayerFSError(
        "InvalidInput",
        `${key} is already set to another project's value; run one LayerFS project per rutis Node runtime`,
        null,
      );
    }
    process.env[key] = value;
  };
  set("HOME", config.home);
  for (const [field, name] of Object.entries(DEPLOY_ENV)) {
    set(name, config.deploy?.[field]);
  }
}

function configuredClient(config) {
  configureEnvironment(config);
  return new LayerFS(config.project, {
    storePath: config.storePath,
    ...(config.storePolicy ? { storePolicy: config.storePolicy } : {}),
    ...(config.trace ? { trace: config.trace } : {}),
  });
}

// A LayerFSError's structured detail crosses as `data`; every refusal keeps
// its stable code. Non-object throws are wrapped so rutis has an error graph.
function crossing(error) {
  if (error !== null && typeof error === "object") {
    if (error.code !== undefined && error.data === undefined) {
      error.data = error.detail ?? null;
    }
    return error;
  }
  return new Error(String(error));
}

function report(ctx, config, method, started, error) {
  if (!config.events?.calls) return;
  ctx.emit("layerfs/call", {
    method,
    ok: error === undefined,
    ms: Date.now() - started,
    ...(error === undefined ? {} : { code: error?.code ?? "Error" }),
  });
}

function once(ctx, config, method, run) {
  const started = Date.now();
  return run().then(
    (value) => {
      report(ctx, config, method, started, undefined);
      return value;
    },
    (error) => {
      report(ctx, config, method, started, error);
      throw crossing(error);
    },
  );
}

function workspaceKey(workspace) {
  if (
    !workspace ||
    typeof workspace.sandbox !== "string" ||
    workspace.sandbox === "" ||
    typeof workspace.path !== "string" ||
    workspace.path === ""
  ) {
    throw new LayerFSError(
      "InvalidInput",
      "a Workspace { sandbox, path } from workspace.mount is required",
      null,
    );
  }
  return `${workspace.sandbox}\0${workspace.path}`;
}

// One Workspace's calls run in order; different Workspaces run in parallel.
function lane(lanes, workspace, run) {
  const key = workspaceKey(workspace);
  const prior = lanes.get(key) ?? Promise.resolve();
  const result = prior.then(run);
  const settled = result.then(
    () => {},
    () => {},
  );
  lanes.set(key, settled);
  settled.then(() => {
    if (lanes.get(key) === settled) lanes.delete(key);
  });
  return result;
}

function groupService(client, prefix, methods, call) {
  return Object.fromEntries(
    Object.entries(methods).map(([name, sdkName]) => [
      name,
      (...args) => call(`${prefix}.${sdkName}`, () => client[prefix][sdkName](...args)),
    ]),
  );
}

async function apply(ctx, config, createClient) {
  if (process.platform !== "linux") {
    throw new LayerFSError(
      "Unsupported",
      "LayerFS Store calls require Linux; run this plugin in a Linux rutis runtime",
      null,
    );
  }

  const client = createClient(config);
  const lanes = new Map();
  const call = (method, run) => once(ctx, config, method, run);

  // `client.call` is the Store method table; `project.init` and Workspace
  // methods have their own group services because `call` opens the Store
  // first.
  ctx.provide("layerfs", {
    info: async () => ({ project: config.project, platform: process.platform }),
    call: (method, args) => call(method, () => client.call(method, args)),
  });
  ctx.provide(
    "layerfs.project",
    groupService(client, "project", GROUP_METHODS.project, call),
  );
  ctx.provide(
    "layerfs.fs",
    groupService(client, "fs", GROUP_METHODS.fs, call),
  );
  ctx.provide(
    "layerfs.branch",
    groupService(client, "branch", GROUP_METHODS.branch, call),
  );
  ctx.provide("layerfs.workspace", {
    mount: (branch, options) =>
      call("workspace.mount", () => client.workspace.mount(branch, options)),
    exec: (workspace, command, message) =>
      call("workspace.exec", () =>
        lane(lanes, workspace, () =>
          client.workspace.exec(workspace, command, message),
        ),
      ),
    status: (workspace) =>
      call("workspace.status", () =>
        lane(lanes, workspace, () => client.workspace.status(workspace)),
      ),
    commit: (workspace, message, force) =>
      call("workspace.commit", () =>
        lane(lanes, workspace, () =>
          client.workspace.commit(workspace, message, force),
        ),
      ),
    unmount: (workspace, discard) =>
      call("workspace.unmount", () =>
        lane(lanes, workspace, () =>
          client.workspace.unmount(workspace, discard),
        ),
      ),
  });

  return async () => {
    await Promise.allSettled([...lanes.values()]);
    await client.close();
  };
}

/**
 * The rutis plugin spec. `createClient` is injectable so a host test can use
 * a fake; the default client configures the documented LAYERFS_* environment
 * and constructs a lazy `LayerFS`.
 */
export function layerfsPluginSpec({ createClient = configuredClient } = {}) {
  return {
    name: "layerfs",
    inject: [],
    provides: SERVICE_METHODS,
    config: CONFIG_SCHEMA,
    apply: (ctx, config) => apply(ctx, config, createClient),
  };
}
