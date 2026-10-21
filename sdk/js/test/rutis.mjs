// The rutis plugin spec without a rutis host: service table, forwarding,
// Workspace lanes, refusal mapping and cleanup.
import test from "node:test";
import assert from "node:assert/strict";
import { LayerFSError } from "../src/index.js";
import {
  SERVICE_METHODS,
  layerfsPluginSpec,
} from "../src/rutis/plugin.js";

const config = {
  project: "website",
  home: "/tmp/layerfs-home",
  storePath: "/tmp/layerfs-store.sqlite",
  events: { calls: true },
};

const workspace = { sandbox: "1", path: "/workspaces/1" };

function fakeClient(log = []) {
  const method = (group, name) => async (...args) => {
    log.push([group, name, args]);
    if (group === "fs" && name === "stat") {
      throw new LayerFSError("NoSuchPath", "no such path", { path: args[1] });
    }
    if (group === "workspace" && name === "exec") {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    return { group, method: name, args };
  };
  const group = (name, names) =>
    Object.fromEntries(names.map((item) => [item, method(name, item)]));
  return {
    log,
    project: group("project", ["init", "list_branches"]),
    fs: group("fs", [
      "read",
      "list",
      "stat",
      "grep",
      "glob",
      "diff_files",
      "blame",
      "write",
      "edit",
      "apply",
      "mkdir",
      "rm",
      "cp",
      "mv",
    ]),
    branch: group("branch", [
      "fork",
      "list_commits",
      "diff_commits",
      "diff_branches",
      "merge",
      "publish",
    ]),
    workspace: group("workspace", [
      "mount",
      "exec",
      "status",
      "commit",
      "unmount",
    ]),
    call: async (name, args) => {
      log.push(["call", name, args]);
      return { method: name, args };
    },
    close: async () => {
      log.push(["close"]);
    },
  };
}

function fakeContext() {
  const services = new Map();
  const events = [];
  return {
    services,
    events,
    provide(name, value) {
      services.set(name, value);
    },
    emit(name, value) {
      events.push([name, value]);
    },
  };
}

async function mount(createClient) {
  const spec = layerfsPluginSpec({ createClient });
  const ctx = fakeContext();
  const cleanup = await spec.apply(ctx, config);
  return { spec, ctx, cleanup };
}

test("declares every SDK group method", async () => {
  const { ctx, cleanup } = await mount(() => fakeClient());
  for (const [name, methods] of Object.entries(SERVICE_METHODS)) {
    assert.ok(ctx.services.has(name), name);
    assert.deepEqual(
      Object.keys(ctx.services.get(name)).sort(),
      Object.keys(methods).sort(),
      name,
    );
  }
  await cleanup();
});

test("forwards group and call methods once", async () => {
  const log = [];
  const { ctx, cleanup } = await mount(() => fakeClient(log));
  assert.deepEqual(await ctx.services.get("layerfs").info(), {
    project: "website",
    platform: process.platform,
  });
  assert.deepEqual(
    await ctx.services.get("layerfs.fs").read("main", "/README.md"),
    { group: "fs", method: "read", args: ["main", "/README.md"] },
  );
  assert.deepEqual(await ctx.services.get("layerfs").call("fs.read", { path: "/a" }), {
    method: "fs.read",
    args: { path: "/a" },
  });
  assert.deepEqual(
    log.map(([group, name]) => `${group}.${name}`),
    ["fs.read", "call.fs.read"],
  );
  assert.equal(
    ctx.events.filter(([name]) => name === "layerfs/call").length,
    2,
  );
  await cleanup();
});

test("runs one Workspace's calls in order and leaves other Workspaces alone", async () => {
  const log = [];
  const { ctx, cleanup } = await mount(() => fakeClient(log));
  const service = ctx.services.get("layerfs.workspace");
  const first = service.exec(workspace, "true");
  const second = service.status(workspace);
  const other = { sandbox: "2", path: "/workspaces/1" };
  const parallel = service.status(other);
  await Promise.all([first, second, parallel]);
  const order = log
    .filter(
      ([group, name]) =>
        group === "workspace" && (name === "exec" || name === "status"),
    )
    .map(([, name, args]) => `${name}:${args[0].sandbox}`);
  assert.deepEqual(order, ["exec:1", "status:2", "status:1"]);
  await cleanup();
});

test("keeps a refusal's code and structured detail", async () => {
  const { ctx, cleanup } = await mount(() => fakeClient());
  await assert.rejects(
    ctx.services.get("layerfs.fs").stat("main", "/missing"),
    (error) => {
      assert.equal(error.name, "LayerFSError");
      assert.equal(error.code, "NoSuchPath");
      assert.deepEqual(error.data, { path: "/missing" });
      return true;
    },
  );
  const event = ctx.events.at(-1);
  assert.equal(event[0], "layerfs/call");
  assert.deepEqual(event[1], {
    method: "fs.stat",
    ok: false,
    ms: event[1].ms,
    code: "NoSuchPath",
  });
  await cleanup();
});

test("cleans up by draining lanes and closing the client", async () => {
  const log = [];
  const { ctx, cleanup } = await mount(() => fakeClient(log));
  const running = ctx.services
    .get("layerfs.workspace")
    .exec(workspace, "sleep 1");
  await cleanup();
  assert.equal(log.filter(([group]) => group === "workspace").length, 1);
  assert.deepEqual(log.at(-1), ["close"]);
  await running;
});
