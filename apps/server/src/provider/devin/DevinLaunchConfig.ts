import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeCrypto from "node:crypto";
import { parse, type ParseError } from "jsonc-parser/lib/esm/main.js";

type JsonObject = Record<string, unknown>;
async function readConfig(path: string): Promise<JsonObject> {
  let text: string;
  try {
    text = await NodeFSP.readFile(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return {};
    throw error;
  }
  const errors: ParseError[] = [];
  const value: unknown = parse(text, errors, { allowTrailingComma: true });
  if (errors.length || !value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`Invalid Devin configuration: ${path}`);
  }
  return value as JsonObject;
}

/** A sidecar is a compatibility claim tied to exact bytes, not a trust signature. */
export async function verifyDevinBinary(
  binary: string,
  host: { platform: NodeJS.Platform; arch: NodeJS.Architecture },
) {
  if (host.platform !== "darwin" || host.arch !== "arm64") {
    throw new Error("The private-control Devin patch currently requires macOS ARM64.");
  }
  const manifest = await readConfig(binary + ".manifest.json");
  const capabilities = manifest.capabilities;
  if (
    manifest.control_abi !== 1 ||
    manifest.owner_check !== "pid" ||
    manifest.clear !== false ||
    !Array.isArray(capabilities) ||
    !["private-control-directory", "owner-pid", "compact"].every((name) =>
      capabilities.includes(name),
    )
  ) {
    throw new Error("Select a Devin binary built with T3 private-control ABI 1 and its manifest.");
  }
  const hash = NodeCrypto.createHash("sha256");
  for await (const chunk of NodeFS.createReadStream(binary)) hash.update(chunk);
  if (hash.digest("hex") !== manifest.output_sha256) {
    throw new Error("Devin binary does not match its compatibility manifest. No prompt sent.");
  }
}

export async function buildDevinConfig(options: {
  cwd: string;
  environment: NodeJS.ProcessEnv;
  hookCommand: string;
  model: string;
  compactionThresholdTokens?: number | undefined;
}): Promise<JsonObject> {
  const home = options.environment.HOME || NodeOS.homedir();
  const configHome = options.environment.XDG_CONFIG_HOME || NodePath.join(home, ".config");
  const machine = await readConfig(NodePath.join(configHome, "devin", "config.json"));
  // --config replaces only user config. Native project hooks are additive.
  // Until native hook takeover is proved, refuse those projects instead of
  // changing workspace files or claiming their hooks have been replaced.
  let directory = await NodeFSP.realpath(options.cwd);
  while (true) {
    for (const name of ["config.json", "config.local.json"]) {
      const path = NodePath.join(directory, ".devin", name);
      const project = await readConfig(path);
      if (project.hooks && Object.keys(project.hooks as object).length) {
        throw new Error(
          `Project Devin hooks require native hook takeover, not yet supported: ${path}`,
        );
      }
    }
    const hookFile = NodePath.join(directory, ".devin", "hooks.v1.json");
    try {
      await NodeFSP.access(hookFile);
      throw new Error(`Project hook file is not supported by managed Devin: ${hookFile}`);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    const parent = NodePath.dirname(directory);
    if (parent === directory) break;
    directory = parent;
  }
  const agent =
    machine.agent && typeof machine.agent === "object" && !Array.isArray(machine.agent)
      ? (machine.agent as JsonObject)
      : {};
  return {
    ...machine,
    auto_update: false,
    subagents_enabled: false,
    agent: {
      ...agent,
      model: options.model,
      ...(options.compactionThresholdTokens === undefined
        ? {}
        : {
            compaction_threshold_tokens: options.compactionThresholdTokens,
          }),
    },
    hooks: Object.fromEntries(
      ["Stop", "PostCompaction", "PreToolUse"].map((name) => [
        name,
        [{ hooks: [{ type: "command", command: options.hookCommand, timeout: 315360000 }] }],
      ]),
    ),
  };
}
