import * as NodeOS from "node:os";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeModule from "node:module";
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import { selectCliRuntimeExternalDependencies } from "../lib/cli-external-packages.ts";
import { sha256, verifyRelease } from "./install.mjs";

const directory = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const repo = NodePath.resolve(directory, "../..");
const [version, devinBinary, outputDirectory] = process.argv.slice(2);
if (
  !version ||
  !devinBinary ||
  !outputDirectory ||
  !/^t3-fork-[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(version)
) {
  throw new Error(
    "Usage: node scripts/fork/build.mjs VERSION /absolute/devin-patched /absolute/output-directory",
  );
}
// oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone packaging command; no Effect runtime is loaded.
if (NodeOS.platform() !== "darwin" || NodeOS.arch() !== "arm64")
  throw new Error("Build on an Apple Silicon Mac.");
const output = NodePath.resolve(outputDirectory);
await NodeFSP.mkdir(output, { recursive: true });
const stage = await NodeFSP.mkdtemp(NodePath.join(output, ".stage-"));
const bundle = NodePath.join(stage, "t3-fork");
const server = NodePath.join(bundle, "server");

try {
  await NodeFSP.access(NodePath.join(repo, "apps/server/dist/client/index.html"));
  await NodeFSP.mkdir(server, { recursive: true });
  await NodeFSP.cp(NodePath.join(repo, "apps/server/dist"), NodePath.join(server, "dist"), {
    recursive: true,
  });
  await NodeFSP.mkdir(NodePath.join(server, "dist/resource-monitor"), { recursive: true });
  await NodeFSP.copyFile(
    NodePath.join(repo, "native/resource-monitor/target/release/t3-resource-monitor"),
    NodePath.join(server, "dist/resource-monitor/t3-resource-monitor"),
  );
  await NodeFSP.writeFile(
    NodePath.join(server, "package.json"),
    '{"private":true,"type":"module"}\n',
  );

  // Copy the exact installed dependency graph, including native optional packages.
  // Relative links keep pnpm's distinct versions without referring back to this checkout.
  const copied = new Map();
  async function copyPackage(name, from, linkDirectory, optional = false) {
    const require = NodeModule.createRequire(NodePath.join(from, "package.json"));
    let source;
    for (const candidate of require.resolve.paths(name) ?? []) {
      try {
        source = await NodeFSP.realpath(NodePath.join(candidate, name));
        await NodeFSP.access(NodePath.join(source, "package.json"));
        break;
      } catch (error) {
        if (error.code !== "ENOENT") throw error;
        source = undefined;
      }
    }
    if (!source) {
      if (optional) return;
      throw new Error(`Missing runtime dependency: ${name} from ${from}`);
    }
    const pkg = JSON.parse(await NodeFSP.readFile(NodePath.join(source, "package.json"), "utf8"));
    const supports = (list, value) =>
      !list ||
      (!list.includes(`!${value}`) &&
        (list.every((item) => item.startsWith("!")) || list.includes(value)));
    if (!supports(pkg.os, "darwin") || !supports(pkg.cpu, "arm64")) {
      if (optional) return;
      throw new Error(`Runtime package ${name} does not support darwin-arm64`);
    }
    let target = copied.get(source);
    if (!target) {
      target = NodePath.join(server, ".deps", `${copied.size}-${name.replaceAll("/", "-")}`);
      copied.set(source, target);
      await NodeFSP.cp(source, target, {
        recursive: true,
        dereference: true,
        filter: (entry) =>
          entry === source ||
          !NodePath.relative(source, entry).split(NodePath.sep).includes("node_modules"),
      });
      for (const dependency of Object.keys(pkg.dependencies ?? {})) {
        await copyPackage(
          dependency,
          source,
          NodePath.join(target, "node_modules"),
          dependency in (pkg.optionalDependencies ?? {}),
        );
      }
      for (const dependency of Object.keys(pkg.optionalDependencies ?? {})) {
        if (!(dependency in (pkg.dependencies ?? {})))
          await copyPackage(dependency, source, NodePath.join(target, "node_modules"), true);
      }
    }
    const link = NodePath.join(linkDirectory, name);
    await NodeFSP.mkdir(NodePath.dirname(link), { recursive: true });
    await NodeFSP.symlink(NodePath.relative(NodePath.dirname(link), target), link);
  }
  const serverPackage = JSON.parse(
    await NodeFSP.readFile(NodePath.join(repo, "apps/server/package.json"), "utf8"),
  );
  for (const name of Object.keys(
    selectCliRuntimeExternalDependencies(serverPackage.dependencies),
  )) {
    await copyPackage(
      name,
      NodePath.join(repo, "apps/server"),
      NodePath.join(server, "node_modules"),
    );
  }

  const runtime = NodePath.join(bundle, "runtime");
  await NodeFSP.mkdir(NodePath.join(runtime, "bin"), { recursive: true });
  const libraries = NodeChildProcess.execFileSync("otool", ["-L", process.execPath], {
    encoding: "utf8",
  })
    .split("\n")
    .slice(1)
    .filter((line) => line.trim());
  if (libraries.some((line) => !/^\s+\/(usr\/lib|System\/Library)\//.test(line)))
    throw new Error(
      "Node depends on non-system libraries; use the official standalone Node distribution.",
    );
  await NodeFSP.copyFile(process.execPath, NodePath.join(runtime, "bin/node"));
  await NodeFSP.copyFile(
    NodePath.resolve(process.execPath, "../../LICENSE"),
    NodePath.join(runtime, "LICENSE"),
  );
  await NodeFSP.mkdir(NodePath.join(bundle, "devin"));
  await NodeFSP.copyFile(
    NodePath.resolve(devinBinary),
    NodePath.join(bundle, "devin/devin-patched"),
  );
  await NodeFSP.copyFile(
    NodePath.resolve(devinBinary) + ".manifest.json",
    NodePath.join(bundle, "devin/devin-patched.manifest.json"),
  );
  for (const name of ["launch.mjs", "install.mjs"])
    await NodeFSP.copyFile(NodePath.join(directory, name), NodePath.join(bundle, name));
  await NodeFSP.copyFile(NodePath.join(repo, "LICENSE"), NodePath.join(bundle, "LICENSE"));
  await NodeFSP.copyFile(NodePath.join(directory, "README.md"), NodePath.join(bundle, "README.md"));
  const files = {};
  const links = {};
  async function inventory(dir) {
    for (const entry of await NodeFSP.readdir(dir, { withFileTypes: true })) {
      const filename = NodePath.join(dir, entry.name);
      const relative = NodePath.relative(bundle, filename);
      if (entry.isDirectory()) await inventory(filename);
      else if (entry.isSymbolicLink()) links[relative] = await NodeFSP.readlink(filename);
      else files[relative] = await sha256(filename);
    }
  }
  await inventory(bundle);
  const commit = NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: repo,
    encoding: "utf8",
  }).trim();
  await NodeFSP.writeFile(
    NodePath.join(bundle, "release.json"),
    JSON.stringify(
      { version, platform: "darwin", arch: "arm64", commit, node: process.version, files, links },
      null,
      2,
    ) + "\n",
  );
  await verifyRelease(bundle);
  NodeChildProcess.execFileSync(
    NodePath.join(runtime, "bin/node"),
    [NodePath.join(server, "dist/bin.mjs"), "--help"],
    {
      cwd: stage,
      stdio: "pipe",
    },
  );
  const archiveName = `${version}-macos-arm64.tar.gz`;
  const archive = NodePath.join(output, archiveName);
  try {
    await NodeFSP.access(archive);
    throw new Error("Archive already exists; choose a new version.");
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  NodeChildProcess.execFileSync("tar", ["-czf", archive, "-C", stage, "t3-fork"], {
    env: { ...process.env, COPYFILE_DISABLE: "1" },
  });
  const hash = await sha256(archive);
  const url = `https://github.com/letrandat/t3code/releases/download/${version}/${archiveName}`;
  const template = await NodeFSP.readFile(NodePath.join(directory, "install.sh"), "utf8");
  // Replace the assigned values only; retain the template-use guard.
  await NodeFSP.writeFile(
    NodePath.join(output, "install-t3-fork.sh"),
    template
      .replace("archive_url='@ARCHIVE_URL@'", `archive_url='${url}'`)
      .replace("archive_sha256='@ARCHIVE_SHA256@'", `archive_sha256='${hash}'`),
    { mode: 0o755 },
  );
  await NodeFSP.writeFile(NodePath.join(output, "SHA256SUMS"), `${hash}  ${archiveName}\n`);
  console.log(
    `Built ${archive}\nSHA256 ${hash}\nInstaller: ${NodePath.join(output, "install-t3-fork.sh")}`,
  );
} finally {
  await NodeFSP.rm(stage, { recursive: true, force: true });
}
