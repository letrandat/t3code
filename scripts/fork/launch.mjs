import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeURL from "node:url";
import * as NodeChildProcess from "node:child_process";

const release = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));

export async function acquireLaunchLock(root) {
  await NodeFSP.mkdir(root, { recursive: true, mode: 0o700 });
  const lock = NodePath.join(root, "launcher.lock");
  try {
    await NodeFSP.mkdir(lock);
  } catch (error) {
    if (error.code !== "EEXIST") throw error;
    const owner = Number(await NodeFSP.readFile(NodePath.join(lock, "pid"), "utf8"));
    if (!Number.isSafeInteger(owner) || owner <= 0)
      throw new Error("Invalid launcher lock. Inspect it before removing it.", { cause: error });
    try {
      process.kill(owner, 0);
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
      await NodeFSP.rm(lock, { recursive: true });
      return acquireLaunchLock(root);
    }
    throw new Error(`T3 fork launcher is already running (PID ${owner}).`, { cause: error });
  }
  await NodeFSP.writeFile(NodePath.join(lock, "pid"), String(process.pid));
  return () => NodeFSP.rm(lock, { recursive: true, force: true });
}

export async function prepareSettings(root, releaseDir) {
  const home = NodePath.join(root, "data");
  const state = NodePath.join(home, "userdata");
  await NodeFSP.mkdir(state, { recursive: true, mode: 0o700 });
  try {
    const runtime = JSON.parse(
      await NodeFSP.readFile(NodePath.join(state, "server-runtime.json"), "utf8"),
    );
    if (!Number.isSafeInteger(runtime.pid) || runtime.pid <= 0)
      throw new Error("Invalid saved server PID");
    try {
      process.kill(runtime.pid, 0);
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
      return await writeSettings(state, releaseDir, home);
    }
    throw new Error(
      `T3 fork is already running (PID ${runtime.pid}). Use its existing window, or stop it before starting a new version.`,
    );
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  return await writeSettings(state, releaseDir, home);
}

async function writeSettings(state, releaseDir, home) {
  const filename = NodePath.join(state, "settings.json");
  let settings = {};
  try {
    settings = JSON.parse(await NodeFSP.readFile(filename, "utf8"));
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  const id = "devin_fork";
  const existing = settings.providerInstances?.[id];
  if (existing && existing.driver !== "devin")
    throw new Error("Provider ID devin_fork belongs to another driver.");
  settings.providerInstances ??= {};
  settings.providerInstances[id] = {
    ...existing,
    driver: "devin",
    displayName: existing?.displayName ?? "Devin",
    enabled: existing?.enabled ?? true,
    config: {
      compactionThresholdTokens: "240000",
      ...existing?.config,
      binaryPath: NodePath.join(releaseDir, "devin", "devin-patched"),
    },
  };
  const temp = `${filename}.${process.pid}.tmp`;
  await NodeFSP.writeFile(temp, JSON.stringify(settings, null, 2) + "\n", {
    mode: 0o600,
    flag: "wx",
  });
  await NodeFSP.rename(temp, filename);
  return home;
}

async function main() {
  const root = NodePath.resolve(
    process.env.T3_FORK_ROOT || NodePath.join(NodeOS.homedir(), ".local/share/t3-fork"),
  );
  const args = process.argv.slice(2);
  if (args.some((arg) => arg === "--base-dir" || arg.startsWith("--base-dir="))) {
    throw new Error("Use T3_FORK_ROOT to choose a separate fork installation.");
  }
  const informational = args.includes("--help") || args.includes("--version");
  const unlock = informational ? async () => {} : await acquireLaunchLock(root);
  let home;
  try {
    home = informational ? NodePath.join(root, "data") : await prepareSettings(root, release);
  } catch (error) {
    await unlock();
    throw error;
  }
  const env = {
    ...process.env,
    T3CODE_HOME: home,
    PATH: `${NodePath.join(release, "runtime/bin")}:${process.env.PATH || ""}`,
  };
  // A dev shell must not redirect the installed app to a Vite server or shared state.
  delete env.VITE_DEV_SERVER_URL;
  delete env.T3CODE_DEV_INSTANCE;
  delete env.T3CODE_PORT_OFFSET;
  const child = NodeChildProcess.spawn(
    NodePath.join(release, "runtime/bin/node"),
    [NodePath.join(release, "server/dist/bin.mjs"), "--base-dir", home, ...args],
    { stdio: "inherit", env },
  );
  for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"])
    process.on(signal, () => child.kill(signal));
  child.on("error", async (error) => {
    await unlock();
    console.error(error.message);
    process.exitCode = 1;
  });
  child.on("exit", async (code) => {
    await unlock();
    process.exitCode = code ?? 1;
  });
}

if (
  process.argv[1] &&
  NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)
) {
  main().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}
