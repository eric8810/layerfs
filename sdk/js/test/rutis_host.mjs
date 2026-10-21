// The plugin as a rutis host sees it: definePlugin, declared methods,
// service calls and unload cleanup. Requires `npm install` for
// @arcships/rutis (the SDK's optional peer and test dependency).
import test from "node:test";
import assert from "node:assert/strict";
import { load } from "@arcships/rutis/testing";
import { layerfsPlugin } from "../src/rutis/index.js";
import { SERVICE_METHODS } from "../src/rutis/plugin.js";

const config = {
  project: "website",
  home: "/tmp/layerfs-home",
  storePath: "/tmp/layerfs-store.sqlite",
};

function fakeClient(log) {
  const method = (group, name) => async (...args) => {
    log.push([group, name, args]);
    return { group, method: name, args };
  };
  const group = (name, names) =>
    Object.fromEntries(names.map((item) => [item, method(name, item)]));
  return {
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
    call: method("call", "call"),
    close: async () => {
      log.push(["close"]);
    },
  };
}

test("a rutis host loads the declared services and unloads them", async () => {
  const log = [];
  const t = await load(layerfsPlugin({ createClient: () => fakeClient(log) }), {
    config,
  });
  for (const [name, methods] of Object.entries(SERVICE_METHODS)) {
    assert.deepEqual(
      Object.keys(t.methods(name)).sort(),
      Object.keys(methods).sort(),
      name,
    );
  }
  assert.deepEqual(await t.service("layerfs.fs").read("main", "/README.md"), {
    group: "fs",
    method: "read",
    args: ["main", "/README.md"],
  });
  await t.unload();
  assert.deepEqual(log.at(-1), ["close"]);
});
