// @effect-diagnostics nodeBuiltinImport:off - cwd comparisons and run-evidence reads are sync helpers beside the Effect-free native runtime.
// @effect-diagnostics globalDate:off - event stamps are minted in the sync host-callback path, outside any Effect runtime.
import { HostProcessPlatform, HostProcessArchitecture } from "@t3tools/shared/hostProcess";
import * as NodePath from "node:path";
import { ServerConfig } from "../../config.ts";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeCrypto from "node:crypto";

import {
  DevinSettings,
  EventId,
  type ModelSelection,
  RuntimeRequestId,
  ProviderDriverKind,
  TurnId,
  TextGenerationError,
  type ProviderSession,
  type ProviderRuntimeEvent,
  type ThreadId,
} from "@t3tools/contracts";
import {
  discoverDevinModels,
  familySlugForNativeId,
  resolveDevinModel,
  type DevinModelCatalog,
} from "../devin/DevinModels.ts";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { DevinOpenTurn, type DevinEvent } from "../devin/DevinOpenTurn.ts";
import { ProviderAdapterRequestError, type ProviderAdapterError } from "../Errors.ts";
import { defaultProviderContinuationIdentity, type ProviderDriver } from "../ProviderDriver.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import { buildServerProvider } from "../providerSnapshot.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";

import { parsePermissionRequest } from "../acp/AcpRuntimeModel.ts";
import {
  makeAcpRequestOpenedEvent,
  makeAcpRequestResolvedEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";

const provider = ProviderDriverKind.make("devin");

/** Resume cursor v1: enough to re-attach to a detached host by run directory.
    Legacy pre-survival cursors ({ nativeOpenTurn: true }) carry no run and
    are treated as absent; their native processes died with their server. */
const decodeJsonUnknown = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

/** A working child silent this long may be stuck (attach-time report only). */
const STUCK_IDLE_MS = 5 * 60 * 1000;
/** A live run older than this was likely frozen by sleep while detached. */
const SLEEP_GAP_MS = 120 * 1000;

/** Pure attach-health report: mid-turn state, stuck child, and sleep gaps are
    warnings, never failures. The run is already verified reachable. */
export function attachHealthWarnings(
  snapshot: { phase?: string | undefined; stdoutIdleMs?: number | undefined },
  detachedForMs: number | undefined,
): string[] {
  const warnings: string[] = [];
  const midTurn =
    snapshot.phase === "working" ||
    snapshot.phase === "bootstrapping" ||
    snapshot.phase === "context";
  if (midTurn)
    warnings.push(
      "Reattached to the native run while a turn was in flight. Output produced while detached was not streamed; the full transcript is in the run's protocol.jsonl.",
    );
  if (midTurn && snapshot.stdoutIdleMs !== undefined && snapshot.stdoutIdleMs > STUCK_IDLE_MS)
    warnings.push(
      `Native child has produced no output for ${Math.round(snapshot.stdoutIdleMs / 1000)}s while a turn is in flight. It may be stuck; the run stays attached and its evidence is in the run folder.`,
    );
  if (detachedForMs !== undefined && detachedForMs > SLEEP_GAP_MS)
    warnings.push(
      `Native run was unreachable for ${Math.round(detachedForMs / 1000)}s (possible sleep). Health check passed; continuing the same run.`,
    );
  return warnings;
}

type DevinResumeCursorV1 = { runDir: string; runId: string };
function readDevinResumeCursor(value: unknown): DevinResumeCursorV1 | undefined {
  if (value === null || value === undefined) return undefined;
  if (typeof value !== "object") return undefined;
  const cursor = value as { schemaVersion?: unknown; runDir?: unknown; runId?: unknown };
  if (cursor.schemaVersion !== 1) return undefined;
  if (typeof cursor.runDir !== "string" || typeof cursor.runId !== "string")
    throw new Error("Resume state for this Devin run is corrupt. Start a new thread explicitly.");
  return { runDir: cursor.runDir, runId: cursor.runId };
}

type State = {
  session: ProviderSession;
  runtime?: DevinOpenTurn | undefined;
  active?: TurnId | undefined;
  text: string;
  catalog: DevinModelCatalog;
  /** Resolved native variant id for the live run (e.g. `grok-4-high`). */
  nativeModel: string;
  /** Original picker selection (family slug + options) for variant-aware checks. */
  selection?: ModelSelection | undefined;
};

export const DevinDriver: ProviderDriver<typeof DevinSettings.Type, ServerConfig> = {
  driverKind: provider,
  metadata: { displayName: "Devin", supportsMultipleInstances: true },
  configSchema: DevinSettings,
  defaultConfig: () => ({
    binaryPath: "",
    model: "",
    compactionThresholdTokens: "240000",
  }),
  create: ({ config, instanceId, displayName, accentColor, environment, enabled }) =>
    Effect.gen(function* () {
      const serverConfig = yield* ServerConfig;
      const host = { platform: yield* HostProcessPlatform, arch: yield* HostProcessArchitecture };
      const events = yield* Queue.unbounded<ProviderRuntimeEvent>();
      const sessions = new Map<ThreadId, State>();
      let catalog: DevinModelCatalog | undefined;
      let discoveryError =
        "Devin model discovery has not completed. Retry in Settings > Providers.";
      let discovering: Promise<void> | undefined;
      const runDiscovery = async (signal?: AbortSignal) => {
        try {
          catalog = await discoverDevinModels(
            config.binaryPath,
            mergeProviderInstanceEnvironment(environment),
            config.model,
            signal,
          );
          discoveryError = "";
        } catch (cause) {
          catalog = undefined;
          discoveryError = cause instanceof Error ? cause.message : String(cause);
        }
      };
      const discover = (signal?: AbortSignal) => {
        if (!discovering)
          discovering = runDiscovery(signal).finally(() => {
            discovering = undefined;
          });
        return discovering;
      };
      if (enabled && config.binaryPath) yield* Effect.promise(discover);
      const requireCatalog = () => {
        if (!catalog) throw new Error(discoveryError);
        return catalog;
      };
      const emit = (event: ProviderRuntimeEvent) => {
        Queue.offerUnsafe(events, event);
      };
      const error = (method: string, cause: unknown) =>
        new ProviderAdapterRequestError({
          provider,
          method,
          detail: cause instanceof Error ? cause.message : String(cause),
        });
      const request = <T>(method: string, body: () => Promise<T>) =>
        Effect.tryPromise({ try: body, catch: (cause) => error(method, cause) });
      const requireState = (id: ThreadId) => {
        const state = sessions.get(id);
        if (!state)
          throw new Error("Devin session is unavailable. Create a new thread explicitly.");
        return state;
      };
      const base = (state: State) => ({
        eventId: EventId.make(NodeCrypto.randomUUID()),
        provider,
        providerInstanceId: instanceId,
        threadId: state.session.threadId,
        createdAt: new Date().toISOString(),
        ...(state.active ? { turnId: state.active } : {}),
      });
      const onEvent = (state: State, event: DevinEvent) => {
        if (event.type === "permission" || event.type === "permission-resolved") {
          const stamp = base(state);
          const common = {
            stamp,
            provider,
            threadId: state.session.threadId,
            turnId: state.active,
            requestId: RuntimeRequestId.make(event.requestId),
            permissionRequest: parsePermissionRequest(event.request),
          };
          emit({
            ...(event.type === "permission"
              ? makeAcpRequestOpenedEvent({
                  ...common,
                  approvalOptions: event.options,
                  detail: common.permissionRequest.detail ?? "Devin requests permission.",
                  args: event.request,
                  source: "acp.jsonrpc",
                  method: "session/request_permission",
                  rawPayload: event.request,
                })
              : makeAcpRequestResolvedEvent({ ...common, decision: event.decision })),
            providerInstanceId: instanceId,
          });
        } else if (event.type === "text" && state.active) {
          state.text += event.text;
          emit({
            ...base(state),
            type: "content.delta",
            payload: { streamKind: "assistant_text", delta: event.text },
          });
        } else if (event.type === "waiting") {
          if (state.active) {
            emit({
              ...base(state),
              type: "turn.completed",
              payload: { state: event.cancelled ? "interrupted" : "completed" },
              providerRefs: { providerTurnId: event.promptId },
            });
          }
          state.active = undefined;
          state.session = { ...state.session, status: "ready", activeTurnId: undefined };
        } else if (event.type === "cancel-requested") {
          emit({
            ...base(state),
            type: "runtime.warning",
            payload: {
              message:
                "Stopping after the current tool. Waiting for Devin to reach a tool boundary.",
            },
          });
        } else if (event.type === "context") {
          emit({
            ...base(state),
            type: "thread.state.changed",
            payload: { state: "compacted", detail: { mode: event.mode } },
          });
        } else if (event.type === "failed") {
          if (state.active)
            emit({
              ...base(state),
              type: "turn.completed",
              payload: { state: "failed", errorMessage: event.message },
            });
          emit({ ...base(state), type: "runtime.error", payload: { message: event.message } });
          state.session = { ...state.session, status: "error", lastError: event.message };
          state.active = undefined;
        } else if (event.type === "respawned") {
          emit({
            ...base(state),
            type: "runtime.warning",
            payload: {
              message:
                `Native run respawned (generation ${event.generation}` +
                `${event.redelivered ? ", task replayed once" : ""}): ${event.reason.slice(0, 500)}`,
            },
          });
        } else if (event.type === "host-lost") {
          // The host (and with it the native child) is gone. Report loudly;
          // the next user message starts a fresh run instead of resurrecting
          // this one, and only on that explicit user action.
          state.runtime = undefined;
          if (state.active) {
            const message = "The native host is gone. The next message starts a fresh run.";
            emit({
              ...base(state),
              type: "turn.completed",
              payload: { state: "failed", errorMessage: message },
            });
            emit({ ...base(state), type: "runtime.error", payload: { message } });
            state.session = { ...state.session, status: "error", lastError: message };
            state.active = undefined;
          } else {
            emit({
              ...base(state),
              type: "runtime.warning",
              payload: {
                message: "Native host was lost while idle. The next message starts a fresh run.",
              },
            });
            state.session = { ...state.session, status: "ready", activeTurnId: undefined };
          }
        }
      };
      const unsupported = (method: string) =>
        Effect.fail(
          error(
            method,
            "Not supported in the small Devin provider. The open native turn has been retained.",
          ),
        );
      const adapter: ProviderAdapterShape<ProviderAdapterError> = {
        provider,
        capabilities: { sessionModelSwitch: "unsupported", supportsConversationRollback: false },
        startSession: (input) =>
          request("startSession", async () => {
            const now = new Date().toISOString();
            const existing = sessions.get(input.threadId);
            if (existing) {
              // Permission-mode-only changes apply live. The native run cannot
              // be torn down and resumed, but the mode is read per permission
              // request, so sync it without touching native state. Anything
              // else still needs an explicitly new thread. (Restart survival
              // re-attaches by resume cursor in the branch below; this branch
              // only reconciles a session that never went away.)
              const sameNative = (() => {
                try {
                  return (
                    !input.modelSelection ||
                    resolveDevinModel(existing.catalog, input.modelSelection) ===
                      existing.nativeModel
                  );
                } catch {
                  return false;
                }
              })();
              const sameCwd =
                !input.cwd ||
                existing.session.cwd === undefined ||
                NodePath.resolve(input.cwd.trim()) === NodePath.resolve(existing.session.cwd);
              if (sameNative && sameCwd) {
                existing.session = {
                  ...existing.session,
                  runtimeMode: input.runtimeMode,
                  updatedAt: now,
                };
                if (input.modelSelection) existing.selection = input.modelSelection;
                existing.runtime?.setRuntimeMode(input.runtimeMode);
                return existing.session;
              }
              if (!sameNative) throw new Error("Model changes require an explicitly new thread.");
              throw new Error("Workspace changes require an explicitly new thread.");
            }
            const resume = readDevinResumeCursor(input.resumeCursor);
            if (resume) {
              // A previous server detached from a live host. Re-attach by run
              // directory after the same same-model/same-cwd checks the live
              // path applies; any mismatch or unreachable host reports without
              // spawning a replacement.
              await Effect.runPromise(refresh);
              const freshCatalog = requireCatalog();
              let runStatus: { model?: unknown; cwd?: unknown; updatedAt?: unknown };
              try {
                runStatus = decodeJsonUnknown(
                  await NodeFSP.readFile(NodePath.join(resume.runDir, "status.json"), "utf8"),
                ) as { model?: unknown; cwd?: unknown; updatedAt?: unknown };
              } catch {
                throw new Error(
                  "Cannot reattach: the native run has no status. No replacement prompt was sent.",
                );
              }
              if (typeof runStatus.model !== "string" || typeof runStatus.cwd !== "string")
                throw new Error(
                  "Cannot reattach: native run status is incomplete. No replacement prompt was sent.",
                );
              if (input.modelSelection) {
                let wanted: string;
                try {
                  wanted = resolveDevinModel(freshCatalog, input.modelSelection);
                } catch {
                  throw new Error("Model changes require an explicitly new thread.");
                }
                if (wanted !== runStatus.model)
                  throw new Error("Model changes require an explicitly new thread.");
              }
              const sameCwd =
                !input.cwd ||
                NodePath.resolve(input.cwd.trim()) === NodePath.resolve(runStatus.cwd);
              if (!sameCwd) throw new Error("Workspace changes require an explicitly new thread.");
              const state: State = {
                text: "",
                catalog: freshCatalog,
                nativeModel: runStatus.model,
                ...(input.modelSelection ? { selection: input.modelSelection } : {}),
                session: {
                  provider,
                  providerInstanceId: instanceId,
                  threadId: input.threadId,
                  runtimeMode: input.runtimeMode,
                  cwd: runStatus.cwd,
                  model:
                    input.modelSelection?.model ??
                    familySlugForNativeId(freshCatalog, runStatus.model) ??
                    runStatus.model,
                  status: "ready",
                  createdAt: now,
                  updatedAt: now,
                },
              };
              state.runtime = await DevinOpenTurn.attach({
                cwd: runStatus.cwd,
                runRoot: NodePath.join(serverConfig.stateDir, "providers", "devin", "runs"),
                threadId: state.session.threadId,
                providerInstanceId: instanceId,
                host,
                binary: config.binaryPath,
                model: runStatus.model,
                environment: mergeProviderInstanceEnvironment(environment),
                compactionThresholdTokens: config.compactionThresholdTokens
                  ? Number(config.compactionThresholdTokens)
                  : undefined,
                runtimeMode: state.session.runtimeMode,
                onEvent: (event) => onEvent(state, event),
                attachTo: resume,
              });
              try {
                await state.runtime.ping();
              } catch {
                state.runtime.detach();
                throw new Error(
                  "Cannot reattach: the native host did not answer its health check. The run was left untouched; no replacement prompt was sent.",
                );
              }
              sessions.set(input.threadId, state);
              const snapshot = state.runtime.snapshot();
              const seenAt =
                typeof runStatus.updatedAt === "string" ? Date.parse(runStatus.updatedAt) : NaN;
              for (const message of attachHealthWarnings(
                snapshot,
                Number.isNaN(seenAt) ? undefined : Date.now() - seenAt,
              ))
                emit({ ...base(state), type: "runtime.warning", payload: { message } });
              return state.session;
            }
            if (!input.cwd) throw new Error("A workspace directory is required.");
            const catalog = requireCatalog();
            // Resolve once to validate the picker and to pin the native variant
            // for the live run. The persisted session keeps the picker family
            // slug so the orchestration model lock (which compares picker vs
            // picker) does not mistake `grok-4` + high for a switch to
            // `grok-4-high` on the next turn after a soft stop.
            const nativeModel = resolveDevinModel(catalog, input.modelSelection);
            const state: State = {
              text: "",
              catalog,
              nativeModel,
              ...(input.modelSelection ? { selection: input.modelSelection } : {}),
              session: {
                provider,
                providerInstanceId: instanceId,
                threadId: input.threadId,
                runtimeMode: input.runtimeMode,
                cwd: input.cwd,
                model:
                  input.modelSelection?.model ??
                  familySlugForNativeId(catalog, nativeModel) ??
                  nativeModel,
                status: "ready",
                createdAt: now,
                updatedAt: now,
              },
            };
            sessions.set(input.threadId, state);
            return state.session;
          }),
        sendTurn: (input) =>
          request("sendTurn", async () => {
            const state = requireState(input.threadId);
            if (state.active)
              throw new Error("Wait for the current reply before sending another message.");
            // ProviderService adds server-side file paths for every attachment. The
            // Stop hook carries those references without starting another ACP prompt.
            if (!input.input) throw new Error("A message is required.");
            if (input.input === "/clear")
              throw new Error(
                "Native clear did not remove prior task memory in the live test. Clear is disabled until the patch is verified; the existing native prompt is retained.",
              );
            // Fast path on the known catalog; selections unknown here defer to
            // the refreshed check below instead of failing against stale data.
            if (input.modelSelection) {
              let fastNative: string | undefined;
              try {
                fastNative = resolveDevinModel(state.catalog, input.modelSelection);
              } catch {
                fastNative = undefined;
              }
              if (fastNative !== undefined && fastNative !== state.nativeModel)
                throw new Error("Model changes require an explicitly new thread.");
            }
            if (!state.runtime) {
              await Effect.runPromise(refresh);
              const freshCatalog = requireCatalog();
              // Re-resolve the original picker against the refreshed catalog so
              // the runtime keeps the pinned variant; fall back to validating
              // the stored native id when the thread started without a picker.
              // A stored picker the fresh catalog no longer allows means the
              // pinned model is gone, which also needs an explicitly new thread.
              let pinnedNative: string;
              try {
                pinnedNative = state.selection
                  ? resolveDevinModel(freshCatalog, state.selection)
                  : resolveDevinModel(freshCatalog, { model: state.nativeModel });
              } catch {
                throw new Error("Model changes require an explicitly new thread.");
              }
              if (
                input.modelSelection &&
                resolveDevinModel(freshCatalog, input.modelSelection) !== pinnedNative
              )
                throw new Error("Model changes require an explicitly new thread.");
              state.nativeModel = pinnedNative;
              state.catalog = freshCatalog;
              state.runtime = new DevinOpenTurn({
                cwd: state.session.cwd!,
                runRoot: NodePath.join(serverConfig.stateDir, "providers", "devin", "runs"),
                threadId: state.session.threadId,
                providerInstanceId: instanceId,
                host,
                binary: config.binaryPath,
                model: pinnedNative,
                environment: mergeProviderInstanceEnvironment(environment),
                compactionThresholdTokens: config.compactionThresholdTokens
                  ? Number(config.compactionThresholdTokens)
                  : undefined,
                runtimeMode: state.session.runtimeMode,
                onEvent: (event) => onEvent(state, event),
              });
              try {
                await state.runtime.start();
              } catch (cause) {
                state.runtime.close();
                throw cause;
              }
            }
            const turnId = TurnId.make(NodeCrypto.randomUUID());
            state.active = turnId;
            state.text = "";
            state.session = { ...state.session, status: "running", activeTurnId: turnId };
            emit({ ...base(state), type: "turn.started", payload: {} });
            try {
              if (input.input === "/compact") await state.runtime.context("compact");
              else await state.runtime.submit(input.input);
            } catch (cause) {
              onEvent(state, { type: "failed", message: String(cause) });
              throw cause;
            }
            // The snapshot is read per turn: a respawn may have replaced the
            // session, prompt, or generation since the previous turn.
            return {
              threadId: input.threadId,
              turnId,
              resumeCursor: { schemaVersion: 1, ...state.runtime.snapshot() },
            };
          }),
        compaction: { type: "slash-command", command: "/compact" },
        interruptTurn: (threadId, turnId) =>
          Effect.sync(() => {
            const state = sessions.get(threadId);
            if (state && (!turnId || turnId === state.active)) state.runtime?.softCancel();
          }),
        respondToRequest: (threadId, requestId, decision) =>
          request("respondToRequest", async () => {
            const runtime = requireState(threadId).runtime;
            if (!runtime) throw new Error("Devin session has no native run.");
            await runtime.respondToPermission(requestId, decision);
          }),
        respondToUserInput: () => unsupported("respondToUserInput"),
        rollbackThread: () => unsupported("rollbackThread"),
        readThread: (threadId) =>
          request("readThread", async () => {
            requireState(threadId);
            return { threadId, turns: [] };
          }),
        stopSession: (threadId) =>
          Effect.promise(async () => {
            // Deliberate termination: shut the host down and mark the run so
            // a later attach refuses instead of adopting a dead run.
            await sessions.get(threadId)?.runtime?.close();
            sessions.delete(threadId);
          }),
        listSessions: () => Effect.sync(() => [...sessions.values()].map((state) => state.session)),
        hasSession: (id) => Effect.sync(() => sessions.has(id)),
        stopAll: () =>
          Effect.sync(() => {
            // Server shutdown or driver disposal: detach and leave every host
            // and child alive. The next server re-attaches by resume cursor.
            for (const state of sessions.values()) state.runtime?.detach();
            sessions.clear();
          }),
        streamEvents: Stream.fromQueue(events),
      };
      yield* Effect.addFinalizer(() => adapter.stopAll().pipe(Effect.ignore));
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: provider,
        instanceId,
      });
      const makeSnapshot = () =>
        withInstanceIdentity({
          instanceId,
          driverKind: provider,
          displayName,
          accentColor,
          continuationGroupKey: continuationIdentity.continuationKey,
        })(
          buildServerProvider({
            enabled,
            checkedAt: new Date().toISOString(),
            presentation: {
              displayName: "Devin",
              badgeLabel: "Beta",
              showInteractionModeToggle: false,
              requiresNewThreadForModelChange: true,
              supportsConversationRollback: false,
            },
            models: catalog?.models ?? [],
            slashCommands: [
              { name: "compact", description: "Compact within the open native turn" },
            ],
            probe: {
              installed: NodeFS.existsSync(config.binaryPath),
              version: null,
              status: catalog && NodeFS.existsSync(config.binaryPath) ? "ready" : "warning",
              auth: { status: "unknown" },
              message:
                discoveryError ||
                "Chat preview with file references and permission approvals. Uses one native turn per workspace.",
            },
          }),
        );
      const snapshotRef = yield* SubscriptionRef.make(makeSnapshot());
      const snapshot = SubscriptionRef.get(snapshotRef);
      const refresh = Effect.gen(function* () {
        yield* Effect.promise(discover);
        const next = makeSnapshot();
        yield* SubscriptionRef.set(snapshotRef, next);
        return next;
      });
      const noGeneration = () =>
        Effect.fail(
          new TextGenerationError({
            operation: "Devin",
            detail: "Background generation is disabled to avoid extra native prompts.",
          }),
        );
      return {
        instanceId,
        driverKind: provider,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        adapter,
        snapshot: {
          getSnapshot: snapshot,
          refresh,
          streamChanges: SubscriptionRef.changes(snapshotRef),
          applyUsageLimits: () => Effect.void,
          resolveMaintenance: () =>
            Effect.succeed(
              makeManualOnlyProviderMaintenanceCapabilities({ provider, packageName: null }),
            ),
        },
        textGeneration: {
          generateCommitMessage: noGeneration,
          generatePrContent: noGeneration,
          generateBranchName: noGeneration,
          generateThreadTitle: noGeneration,
        },
      };
    }),
};
