// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - This bounded Node subprocess bridge owns and closes its metadata-only child before returning.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import type {
  ModelSelection,
  ProviderOptionDescriptor,
  ServerProviderModel,
} from "@t3tools/contracts";
import { buildDevinConfig } from "./DevinLaunchConfig.ts";

const execFile = NodeUtil.promisify(NodeChildProcess.execFile);
const object = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
const array = (value: unknown): unknown[] => (Array.isArray(value) ? value : []);
const text = (value: unknown) => (typeof value === "string" ? value : "");

type Variant = { id: string; options: { id: string; value: string }[] };
export type DevinModelCatalog = {
  models: ServerProviderModel[];
  variants: Map<string, Variant[]>;
  defaultId: string;
};

export function allowedDevinModels(session: unknown) {
  const result = object(session);
  const option = array(result.configOptions)
    .map(object)
    .find((item) => item.id === "model");
  const entries = option ? array(option.options) : array(object(result.models).availableModels);
  const flatten = (items: unknown[]): string[] =>
    items.flatMap((item) => {
      const entry = object(item);
      return Array.isArray(entry.options)
        ? flatten(entry.options)
        : [text(entry.value ?? entry.modelId)].filter(Boolean);
    });
  const ids = new Set(flatten(entries));
  if (!ids.size)
    throw new Error(
      "Devin did not report any allowed models. Retry model discovery in Settings > Providers.",
    );
  return { ids, current: text(option?.currentValue ?? object(result.models).currentModelId) };
}

/** Join account-offered IDs to catalog metadata. Never construct a native model ID. */
export function buildDevinModelCatalog(
  raw: unknown,
  session: unknown,
  configuredDefault: string,
): DevinModelCatalog {
  const allowed = allowedDevinModels(session);
  const defaultId = configuredDefault || allowed.current;
  if (!allowed.ids.has(defaultId))
    throw new Error(
      "The default Devin model is no longer allowed. Choose an allowed default in Settings and retry.",
    );
  const models: ServerProviderModel[] = [];
  const variants = new Map<string, Variant[]>();
  const labels: Record<string, string> = {
    low: "Low",
    medium: "Medium",
    high: "High",
    xhigh: "Extra High",
    max: "Max",
    ultra: "Ultra",
    none: "None",
  };
  for (const rawFamily of array(object(raw).families)) {
    const family = object(rawFamily);
    const offered = array(family.variants)
      .map(object)
      .filter((v) => allowed.ids.has(text(v.model_uid)));
    if (!offered.length) continue;
    const slug =
      array(family.variants).length === 1 ? text(offered[0]?.model_uid) : text(family.family_uid);
    if (!slug || variants.has(slug)) throw new Error("Invalid Devin model family metadata.");
    const parsed = offered.map((v) => {
      const id = text(v.model_uid);
      const match = /-(low|medium|high|xhigh|max|ultra|none)(-priority)?$/.exec(id);
      return { id, match };
    });
    // Unknown variant shapes retain native labels in a Variant selector.
    const reasoning = parsed.every((v) => v.match !== null);
    const choices: Variant[] = parsed.map(({ id, match }) => ({
      id,
      options:
        reasoning && match
          ? [
              { id: "reasoningEffort", value: match[1]! },
              { id: "serviceTier", value: match[2] ? "fast" : "standard" },
            ]
          : offered.length > 1
            ? [{ id: "variant", value: id }]
            : [],
    }));
    const preferred = choices.find((v) => v.id === defaultId) ?? choices[0]!;
    const optionDescriptors: ProviderOptionDescriptor[] = [];
    for (const key of reasoning
      ? ["reasoningEffort", "serviceTier"]
      : offered.length > 1
        ? ["variant"]
        : []) {
      const values = [
        ...new Set(
          choices.flatMap((v) => v.options.filter((o) => o.id === key).map((o) => o.value)),
        ),
      ];
      if (key === "serviceTier" && values.length === 1 && values[0] === "standard") continue;
      const currentValue = preferred.options.find((o) => o.id === key)!.value;
      optionDescriptors.push({
        id: key,
        label:
          key === "reasoningEffort"
            ? "Reasoning"
            : key === "serviceTier"
              ? "Service Tier"
              : "Variant",
        type: "select",
        currentValue,
        options: values.map((value) => ({
          id: value,
          label:
            key === "reasoningEffort"
              ? labels[value]!
              : key === "serviceTier"
                ? value === "fast"
                  ? "Fast"
                  : "Standard"
                : text(offered.find((v) => v.model_uid === value)?.label) || value,
          isDefault: value === currentValue,
        })),
      });
    }
    const visibleChoices = choices.map((v) => ({
      ...v,
      options: v.options.filter((o) => optionDescriptors.some((d) => d.id === o.id)),
    }));
    variants.set(slug, visibleChoices);
    models.push({
      slug,
      name: text(family.family_label) || slug,
      isCustom: false,
      isDefault: choices.some((v) => v.id === defaultId),
      capabilities: { optionDescriptors, optionCombinations: visibleChoices.map((v) => v.options) },
    });
  }
  if (!models.length || ![...variants.values()].flat().some((v) => v.id === defaultId))
    throw new Error(
      "Devin's allowed models could not be matched to its catalog. Retry model discovery in Settings > Providers.",
    );
  return { models, variants, defaultId };
}

export function resolveDevinModel(
  catalog: DevinModelCatalog,
  selection?: Pick<ModelSelection, "model" | "options">,
): string {
  if (!selection) return catalog.defaultId;
  const exact = [...catalog.variants.values()].flat().find((v) => v.id === selection.model);
  const candidates = catalog.variants.get(selection.model) ?? (exact ? [exact] : undefined);
  if (!candidates)
    throw new Error(
      "This Devin model is not allowed. Retry model discovery in Settings > Providers.",
    );
  const descriptors =
    catalog.models.find((m) => m.slug === selection.model)?.capabilities?.optionDescriptors ?? [];
  const selections = selection.options ?? [];
  if (new Set(selections.map((o) => o.id)).size !== selections.length)
    throw new Error("Duplicate Devin model options.");
  const options = descriptors.map((d) => ({
    id: d.id,
    value: selections.find((o) => o.id === d.id)?.value ?? d.currentValue,
  }));
  if (selections.some((o) => !descriptors.some((d) => d.id === o.id)))
    throw new Error("Unknown Devin model option.");
  const variant = candidates.find((v) =>
    options.every((o) => v.options.some((c) => c.id === o.id && c.value === o.value)),
  );
  if (!variant) throw new Error("This Devin model and variant combination is not allowed.");
  return variant.id;
}

async function readSession(
  binary: string,
  config: string,
  cwd: string,
  env: NodeJS.ProcessEnv,
  signal?: AbortSignal,
) {
  return await new Promise<unknown>((resolve, reject) => {
    const child = NodeChildProcess.spawn(binary, ["--config", config, "acp"], {
      cwd,
      env,
      stdio: "pipe",
    });
    let result: unknown;
    let failure: Error | undefined;
    let buffer = "";
    let size = 0;
    let done = false;
    let killTimer: ReturnType<typeof setTimeout> | undefined;
    const stop = (error?: Error) => {
      if (done) return;
      done = true;
      failure = error;
      child.kill("SIGTERM");
      killTimer = setTimeout(() => child.kill("SIGKILL"), 1_000);
    };
    const abort = () => stop(new Error("Devin model discovery cancelled."));
    const timeout = setTimeout(
      () => stop(new Error("Devin model discovery timed out. Retry in Settings > Providers.")),
      15_000,
    );
    signal?.addEventListener("abort", abort, { once: true });
    const send = (id: number, method: string, params: unknown) =>
      child.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n");
    child.stdin.on("error", () => stop(new Error("Devin model discovery connection closed.")));
    child.on("error", (error) => stop(error));
    child.on("close", () => {
      clearTimeout(timeout);
      clearTimeout(killTimer);
      signal?.removeEventListener("abort", abort);
      if (failure) reject(failure);
      else if (result === undefined)
        reject(new Error("Devin exited before reporting allowed models."));
      else resolve(result);
    });
    child.stderr.resume();
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      if (done) return;
      size += chunk.length;
      if (size > 8 * 1024 * 1024) return stop(new Error("Devin discovery response is too large."));
      buffer += chunk;
      let newline: number;
      while ((newline = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        try {
          const message = object(JSON.parse(line));
          if (message.error)
            return stop(
              new Error("Devin rejected model discovery. Check account access and retry."),
            );
          if (message.id === 1 && message.result) send(2, "session/new", { cwd, mcpServers: [] });
          if (message.id === 2 && message.result) {
            result = message.result;
            stop();
            return;
          }
          if (message.method && message.id !== undefined)
            child.stdin.write(
              JSON.stringify({
                jsonrpc: "2.0",
                id: message.id,
                error: { code: -32601, message: "Metadata discovery only" },
              }) + "\n",
            );
        } catch {
          stop(new Error("Invalid Devin discovery response."));
          return;
        }
      }
    });
    if (signal?.aborted) abort();
    else
      send(1, "initialize", {
        protocolVersion: 1,
        clientCapabilities: {},
        clientInfo: { name: "t3-model-discovery", version: "1" },
      });
  });
}

export async function discoverDevinModels(
  binary: string,
  environment: NodeJS.ProcessEnv,
  configuredDefault: string,
  signal?: AbortSignal,
) {
  const cwd = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devin-models-"));
  try {
    const env = { ...environment };
    for (const key of Object.keys(env))
      if (key.startsWith("DEVIN_CONTROL_") || key === "DEVIN_MODEL") delete env[key];
    const config = NodePath.join(cwd, "config.json");
    const settings = await buildDevinConfig({
      cwd,
      environment: env,
      hookCommand: "true",
      model: "",
    });
    await NodeFSP.writeFile(config, JSON.stringify({ ...settings, hooks: {} }), { mode: 0o600 });
    const { stdout } = await execFile(
      binary,
      ["--config", config, "models", "list", "--format", "json"],
      { cwd, env, timeout: 15_000, maxBuffer: 8 * 1024 * 1024, signal },
    );
    const session = await readSession(binary, config, cwd, env, signal);
    return buildDevinModelCatalog(JSON.parse(stdout), session, configuredDefault);
  } finally {
    await NodeFSP.rm(cwd, { recursive: true, force: true });
  }
}
