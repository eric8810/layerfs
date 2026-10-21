// The LayerFS SDK as a rutis plugin: `@layerfs/sdk/rutis`.
//
//   { "id": "layerfs", "name": "@layerfs/sdk/rutis", "config": { ... } }
//
// The plugin provides `layerfs`, `layerfs.project`, `layerfs.fs`,
// `layerfs.branch` and `layerfs.workspace`. Store calls run in this process
// and therefore require Linux; Workspace calls go to the project's Sandboxes.
import { definePlugin } from "@arcships/rutis";
import { layerfsPluginSpec } from "./plugin.js";

export {
  CONFIG_SCHEMA,
  SERVICE_METHODS,
  layerfsPluginSpec,
} from "./plugin.js";

/** A rutis plugin over one LayerFS project. */
export function layerfsPlugin(options) {
  return definePlugin(layerfsPluginSpec(options));
}

export default layerfsPlugin();
