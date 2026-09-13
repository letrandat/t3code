import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeCrypto from "node:crypto";
import { buildDevinConfig, verifyDevinBinary } from "./DevinLaunchConfig.ts";

NodeTest.test(
  "copy JSONC preferences, replace hooks, and leave originals and cwd alone",
  async (t) => {
    const root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "devin-config-"));
    t.after(() => NodeFSP.rm(root, { recursive: true, force: true }));
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
    NodeAssert.equal(config.theme_mode, "dark");
    NodeAssert.deepEqual(config.agent, {
      model: "exact-model",
      compaction_threshold_tokens: 240000,
    });
    NodeAssert.equal(config.auto_update, false);
    NodeAssert.equal(config.subagents_enabled, false);
    NodeAssert.doesNotMatch(JSON.stringify(config.hooks), /old/);
    NodeAssert.match(JSON.stringify(config.hooks), /owned-hook/);
    NodeAssert.equal(
      await NodeFSP.readFile(NodePath.join(configDir, "config.json"), "utf8"),
      source,
    );
    NodeAssert.equal(await NodeFSP.readFile(rule, "utf8"), "project rule");
    await NodeFSP.mkdir(NodePath.join(project, ".devin"));
    await NodeFSP.writeFile(
      NodePath.join(project, ".devin", "config.local.json"),
      '{"hooks":{"Stop":[{}]}}',
    );
    await NodeAssert.rejects(buildDevinConfig(options), /native hook takeover/);
  },
);

NodeTest.test(
  "binary compatibility rejects a wrong ABI and changed bytes before launch",
  async (t) => {
    const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "devin-binary-"));
    t.after(() => NodeFSP.rm(dir, { recursive: true, force: true }));
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
    await NodeAssert.rejects(
      verifyDevinBinary(binary, { platform: "darwin", arch: "arm64" }),
      /does not match/,
    );
    await NodeFSP.writeFile(
      binary + ".manifest.json",
      JSON.stringify({ ...manifest, control_abi: 99 }),
    );
    await NodeAssert.rejects(
      verifyDevinBinary(binary, { platform: "darwin", arch: "arm64" }),
      /ABI 1/,
    );
  },
);
