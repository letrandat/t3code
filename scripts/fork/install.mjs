import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeURL from "node:url";

export async function sha256(filename) {
  const hash = NodeCrypto.createHash("sha256");
  for await (const chunk of NodeFS.createReadStream(filename)) hash.update(chunk);
  return hash.digest("hex");
}

export async function verifyRelease(source) {
  const manifest = JSON.parse(
    await NodeFSP.readFile(NodePath.join(source, "release.json"), "utf8"),
  );
  if (
    manifest.platform !== "darwin" ||
    manifest.arch !== "arm64" ||
    !/^t3-fork-[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(manifest.version)
  )
    throw new Error("Unsupported release manifest");
  for (const required of [
    "runtime/bin/node",
    "server/dist/bin.mjs",
    "server/dist/client/index.html",
    "devin/devin-patched",
    "devin/devin-patched.manifest.json",
    "launch.mjs",
    "install.mjs",
  ]) {
    if (!manifest.files?.[required]) throw new Error(`Missing required release file: ${required}`);
  }
  for (const [relative, hash] of Object.entries(manifest.files)) {
    if (
      NodePath.isAbsolute(relative) ||
      relative.split("/").includes("..") ||
      !/^[a-f0-9]{64}$/.test(hash)
    )
      throw new Error("Invalid release file entry");
    if ((await sha256(NodePath.join(source, relative))) !== hash)
      throw new Error(`Checksum mismatch: ${relative}`);
  }
  for (const [relative, target] of Object.entries(manifest.links ?? {})) {
    const location = NodePath.join(source, relative);
    const resolved = NodePath.resolve(NodePath.dirname(location), target);
    if (
      NodePath.isAbsolute(relative) ||
      relative.split("/").includes("..") ||
      NodePath.isAbsolute(target) ||
      !resolved.startsWith(NodePath.resolve(source) + NodePath.sep)
    )
      throw new Error("Release symlink leaves the package");
    if ((await NodeFSP.readlink(location)) !== target)
      throw new Error(`Symlink mismatch: ${relative}`);
  }
  const sidecar = JSON.parse(
    await NodeFSP.readFile(NodePath.join(source, "devin/devin-patched.manifest.json"), "utf8"),
  );
  if (
    sidecar.control_abi !== 1 ||
    sidecar.owner_check !== "pid" ||
    sidecar.clear !== false ||
    !["private-control-directory", "owner-pid", "compact"].every((key) =>
      sidecar.capabilities?.includes(key),
    ) ||
    (await sha256(NodePath.join(source, "devin/devin-patched"))) !== sidecar.output_sha256
  )
    throw new Error("Devin ABI or checksum mismatch");
  return manifest;
}

export async function install(source, root) {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Installed outside the repo with only Node builtins available.
  if (NodeOS.platform() !== "darwin" || NodeOS.arch() !== "arm64")
    throw new Error("This release requires an Apple Silicon Mac.");
  const manifest = await verifyRelease(source);
  root = NodePath.resolve(root);
  const releases = NodePath.join(root, "releases");
  await NodeFSP.mkdir(releases, { recursive: true, mode: 0o700 });
  const destination = NodePath.join(releases, manifest.version);
  try {
    const previous = await verifyRelease(destination);
    if (JSON.stringify(previous) !== JSON.stringify(manifest))
      throw new Error("A different release already uses this version. Choose a new version.");
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    // Never replace an existing version, including an incomplete or damaged one.
    try {
      await NodeFSP.lstat(destination);
      throw new Error("Existing release is incomplete. Choose a new version.", { cause: error });
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    const stage = NodePath.join(releases, `.install-${NodeCrypto.randomUUID()}`);
    try {
      await NodeFSP.cp(source, stage, { recursive: true, verbatimSymlinks: true });
      await verifyRelease(stage);
      await NodeFSP.rename(stage, destination);
    } finally {
      await NodeFSP.rm(stage, { recursive: true, force: true });
    }
  }
  const pointer = NodePath.join(root, `.current-${NodeCrypto.randomUUID()}`);
  await NodeFSP.symlink(NodePath.join("releases", manifest.version), pointer);
  await NodeFSP.rename(pointer, NodePath.join(root, "current"));
  const bin = NodePath.join(root, "bin");
  await NodeFSP.mkdir(bin, { recursive: true });
  const launcher = `#!/bin/sh\nset -eu\nroot=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)\nexport T3_FORK_ROOT="$root"\nrelease=$(CDPATH= cd -- "$root/current" && pwd -P)\nexec "$release/runtime/bin/node" "$release/launch.mjs" "$@"\n`;
  const launcherTemp = NodePath.join(bin, `.launcher-${NodeCrypto.randomUUID()}`);
  await NodeFSP.writeFile(launcherTemp, launcher, { mode: 0o755, flag: "wx" });
  await NodeFSP.rename(launcherTemp, NodePath.join(bin, "t3-fork"));
  return NodePath.join(bin, "t3-fork");
}

if (
  process.argv[1] &&
  NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)
) {
  install(
    NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
    process.env.T3_FORK_ROOT || NodePath.join(NodeOS.homedir(), ".local/share/t3-fork"),
  )
    .then((launcher) =>
      console.log(
        `Installed. Start with:\n${JSON.stringify(launcher)}\nDevin must be signed in on this Mac. Existing servers and sessions were not restarted.`,
      ),
    )
    .catch((error) => {
      console.error(error.message);
      process.exitCode = 1;
    });
}
