import * as NodeTest from "node:test";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { install, sha256, verifyRelease } from "./install.mjs";
import { prepareSettings, acquireLaunchLock } from "./launch.mjs";

async function fixture(t, version = "t3-fork-test-1") {
  const temp = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-fork-install-test-"));
  t.after(() => NodeFSP.rm(temp, { recursive: true, force: true }));
  const source = NodePath.join(temp, "package");
  const root = NodePath.join(temp, "destination with spaces");
  const files = {};
  for (const name of [
    "runtime/bin/node",
    "server/dist/bin.mjs",
    "server/dist/client/index.html",
    "devin/devin-patched",
    "launch.mjs",
    "install.mjs",
  ]) {
    await NodeFSP.mkdir(NodePath.dirname(NodePath.join(source, name)), { recursive: true });
    await NodeFSP.writeFile(NodePath.join(source, name), `test ${name}`);
    files[name] = await sha256(NodePath.join(source, name));
  }
  const sidecar = {
    control_abi: 1,
    owner_check: "pid",
    clear: false,
    capabilities: ["private-control-directory", "owner-pid", "compact"],
    output_sha256: files["devin/devin-patched"],
  };
  await NodeFSP.writeFile(
    NodePath.join(source, "devin/devin-patched.manifest.json"),
    JSON.stringify(sidecar),
  );
  files["devin/devin-patched.manifest.json"] = await sha256(
    NodePath.join(source, "devin/devin-patched.manifest.json"),
  );
  const manifest = { version, platform: "darwin", arch: "arm64", files };
  await NodeFSP.writeFile(NodePath.join(source, "release.json"), JSON.stringify(manifest));
  return { source, root, manifest };
}

NodeTest.test(
  "install and repeat preserve data; a new release preserves old executable bytes",
  async (t) => {
    const { source, root, manifest } = await fixture(t);
    const launcher = await install(source, root);
    const first = await NodeFSP.realpath(NodePath.join(root, "current"));
    await prepareSettings(root, first);
    const settingsPath = NodePath.join(root, "data/userdata/settings.json");
    const settings = JSON.parse(await NodeFSP.readFile(settingsPath, "utf8"));
    NodeAssert.equal(
      settings.providerInstances.devin_fork.config.compactionThresholdTokens,
      "240000",
    );
    settings.customSetting = "keep";
    settings.providerInstances.devin_fork.config.compactionThresholdTokens = "180000";
    settings.providerInstances.other = { driver: "codex", enabled: false };
    await NodeFSP.writeFile(settingsPath, JSON.stringify(settings));
    const saved = await NodeFSP.readFile(settingsPath, "utf8");
    NodeAssert.equal(await install(source, root), launcher);
    NodeAssert.equal(await NodeFSP.readFile(settingsPath, "utf8"), saved);
    manifest.version = "t3-fork-test-2";
    await NodeFSP.writeFile(NodePath.join(source, "release.json"), JSON.stringify(manifest));
    await install(source, root);
    const second = await NodeFSP.realpath(NodePath.join(root, "current"));
    NodeAssert.notEqual(second, first);
    NodeAssert.equal(
      await NodeFSP.readFile(NodePath.join(first, "devin/devin-patched"), "utf8"),
      "test devin/devin-patched",
    );
    NodeAssert.equal(await NodeFSP.readFile(settingsPath, "utf8"), saved);
    await prepareSettings(root, second);
    const updated = JSON.parse(await NodeFSP.readFile(settingsPath, "utf8"));
    NodeAssert.equal(
      updated.providerInstances.devin_fork.config.binaryPath,
      NodePath.join(second, "devin/devin-patched"),
    );
    NodeAssert.equal(
      updated.providerInstances.devin_fork.config.compactionThresholdTokens,
      "180000",
    );
    NodeAssert.deepEqual(updated.providerInstances.other, settings.providerInstances.other);
    NodeAssert.equal(updated.customSetting, "keep");
  },
);

NodeTest.test("damaged downloads and mismatched Devin ABI never replace current", async (t) => {
  const { source, root, manifest } = await fixture(t);
  await install(source, root);
  const current = await NodeFSP.readlink(NodePath.join(root, "current"));
  await NodeFSP.writeFile(NodePath.join(source, "devin/devin-patched"), "changed");
  await NodeAssert.rejects(install(source, root), /Checksum mismatch/);
  NodeAssert.equal(await NodeFSP.readlink(NodePath.join(root, "current")), current);
  manifest.files["devin/devin-patched"] = await sha256(
    NodePath.join(source, "devin/devin-patched"),
  );
  await NodeFSP.writeFile(NodePath.join(source, "release.json"), JSON.stringify(manifest));
  await NodeAssert.rejects(verifyRelease(source), /Devin ABI or checksum mismatch/);
});

NodeTest.test("changed bytes cannot reuse a release version", async (t) => {
  const { source, root, manifest } = await fixture(t);
  await install(source, root);
  await NodeFSP.writeFile(NodePath.join(source, "launch.mjs"), "changed");
  manifest.files["launch.mjs"] = await sha256(NodePath.join(source, "launch.mjs"));
  await NodeFSP.writeFile(NodePath.join(source, "release.json"), JSON.stringify(manifest));
  await NodeAssert.rejects(install(source, root), /different release already/);
});

NodeTest.test("starting while the server is alive never edits provider settings", async (t) => {
  const { source, root } = await fixture(t);
  await install(source, root);
  const release = await NodeFSP.realpath(NodePath.join(root, "current"));
  await prepareSettings(root, release);
  const state = NodePath.join(root, "data/userdata");
  const before = await NodeFSP.readFile(NodePath.join(state, "settings.json"), "utf8");
  await NodeFSP.writeFile(
    NodePath.join(state, "server-runtime.json"),
    JSON.stringify({ pid: process.pid }),
  );
  await NodeAssert.rejects(prepareSettings(root, release), /already running/);
  NodeAssert.equal(await NodeFSP.readFile(NodePath.join(state, "settings.json"), "utf8"), before);
});

NodeTest.test("two launches cannot prepare settings concurrently", async (t) => {
  const { root } = await fixture(t);
  const unlock = await acquireLaunchLock(root);
  await NodeAssert.rejects(acquireLaunchLock(root), /already running/);
  await unlock();
  await (
    await acquireLaunchLock(root)
  )();
});
