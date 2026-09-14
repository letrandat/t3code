// @effect-diagnostics nodeBuiltinImport:off - the suite stages real config files in temp dirs.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeCrypto from "node:crypto";
import { afterEach, describe, expect, it } from "vite-plus/test";
import { buildDevinConfig, verifyDevinBinary } from "./DevinLaunchConfig.ts";

const cleanups: Array<() => unknown> = [];
afterEach(async () => {
  let cleanup = cleanups.pop();
  while (cleanup) {
    await cleanup();
    cleanup = cleanups.pop();
  }
});

describe("devin launch config", () => {
  it("copies JSONC preferences, replaces hooks, and leaves originals and cwd alone", async () => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "devin-config-"));
    cleanups.push(() => NodeFSP.rm(root, { recursive: true, force: true }));
    const configDir = NodePath.join(root, ".config", "devin");
    await NodeFSP.mkdir(configDir, { recursive: true });
    const source =
      '// user settings\n{"theme_mode":"dark","agent":{"model":"old"},"hooks":{"Stop":[{"hooks":[{"command":"old"}]}]},}';
    await NodeFSP.writeFile(NodePath.join(configDir, "config.json"), source);
    const project = NodePath.join(root, "project");
    await NodeFSP.mkdir(project);
    const rule = NodePath.join(project, "AGENTS.md");
    await NodeFSP.writeFile(rule, "project rule");
    const options = {
      cwd: project,
      environment: { HOME: root },
      hookCommand: "owned-hook",
      model: "exact-model",
      compactionThresholdTokens: 240000,
    };
    const config = await buildDevinConfig(options);
    expect(config.theme_mode).toBe("dark");
    expect(config.agent).toEqual({
      model: "exact-model",
      compaction_threshold_tokens: 240000,
    });
    expect(config.auto_update).toBe(false);
    expect(config.subagents_enabled).toBe(false);
    expect(JSON.stringify(config.hooks)).not.toMatch(/old/);
    expect(JSON.stringify(config.hooks)).toMatch(/owned-hook/);
    expect(await NodeFSP.readFile(NodePath.join(configDir, "config.json"), "utf8")).toBe(source);
    expect(await NodeFSP.readFile(rule, "utf8")).toBe("project rule");
    await NodeFSP.mkdir(NodePath.join(project, ".devin"));
    await NodeFSP.writeFile(
      NodePath.join(project, ".devin", "config.local.json"),
      '{"hooks":{"Stop":[{}]}}',
    );
    await expect(buildDevinConfig(options)).rejects.toThrow(/native hook takeover/);
  });

  it("binary compatibility rejects a wrong ABI and changed bytes before launch", async () => {
    const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "devin-binary-"));
    cleanups.push(() => NodeFSP.rm(dir, { recursive: true, force: true }));
    const binary = NodePath.join(dir, "devin");
    await NodeFSP.writeFile(binary, "pinned bytes");
    const manifest = {
      control_abi: 1,
      owner_check: "pid",
      clear: false,
      capabilities: ["private-control-directory", "owner-pid", "compact"],
      output_sha256: NodeCrypto.createHash("sha256").update("pinned bytes").digest("hex"),
    };
    await NodeFSP.writeFile(binary + ".manifest.json", JSON.stringify(manifest));
    await verifyDevinBinary(binary, { platform: "darwin", arch: "arm64" });
    await NodeFSP.writeFile(binary, "changed");
    await expect(verifyDevinBinary(binary, { platform: "darwin", arch: "arm64" })).rejects.toThrow(
      /does not match/,
    );
    await NodeFSP.writeFile(
      binary + ".manifest.json",
      JSON.stringify({ ...manifest, control_abi: 99 }),
    );
    await expect(verifyDevinBinary(binary, { platform: "darwin", arch: "arm64" })).rejects.toThrow(
      /ABI 1/,
    );
  });
});
