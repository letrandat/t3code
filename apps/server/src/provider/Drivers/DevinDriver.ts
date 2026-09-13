import { HostProcessPlatform, HostProcessArchitecture } from "@t3tools/shared/hostProcess";
import * as NodePath from "node:path";
import { ServerConfig } from "../../config.ts";
import * as NodeFS from "node:fs";
import * as NodeCrypto from "node:crypto";

import {
  DevinSettings,
  EventId,
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
  resolveDevinModel,
  type DevinModelCatalog,
} from "../devin/DevinModels.ts";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Effect from "effect/Effect";
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
type State = {
  session: ProviderSession;
  runtime?: DevinOpenTurn;
  active?: TurnId | undefined;
  text: string;
  catalog: DevinModelCatalog;
};

export const DevinDriver: ProviderDriver<typeof DevinSettings.Type, ServerConfig> = {
  driverKind: provider,
  metadata: { displayName: "Devin", supportsMultipleInstances: true },
  configSchema: DevinSettings,
  defaultConfig: () => ({
    binaryPath: "",
    model: "",
    allowNativePrompt: false,
    compactionThresholdTokens: "",
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
            if (input.resumeCursor || sessions.has(input.threadId))
              throw new Error("Automatic native restart/resume is disabled.");
            if (!input.cwd) throw new Error("A workspace directory is required.");
            const now = new Date().toISOString();
            const state: State = {
              text: "",
              catalog: requireCatalog(),
              session: {
                provider,
                providerInstanceId: instanceId,
                threadId: input.threadId,
                runtimeMode: input.runtimeMode,
                cwd: input.cwd,
                model: resolveDevinModel(requireCatalog(), input.modelSelection),
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
            if (
              input.modelSelection &&
              resolveDevinModel(state.catalog, input.modelSelection) !== state.session.model
            )
              throw new Error("Model changes require an explicitly new thread.");
            if (!state.runtime) {
              await Effect.runPromise(refresh);
              resolveDevinModel(requireCatalog(), { model: state.session.model! });
              state.runtime = new DevinOpenTurn({
                cwd: state.session.cwd!,
                runRoot: NodePath.join(serverConfig.stateDir, "providers", "devin", "runs"),
                threadId: state.session.threadId,
                providerInstanceId: instanceId,
                host,
                binary: config.binaryPath,
                model: state.session.model!,
                allowNativePrompt: config.allowNativePrompt,
                environment: mergeProviderInstanceEnvironment(environment),
                compactionThresholdTokens: config.compactionThresholdTokens
                  ? Number(config.compactionThresholdTokens)
                  : undefined,
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
            return { threadId: input.threadId, turnId, resumeCursor: { nativeOpenTurn: true } };
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
          Effect.sync(() => {
            sessions.get(threadId)?.runtime?.close();
            sessions.delete(threadId);
          }),
        listSessions: () => Effect.sync(() => [...sessions.values()].map((state) => state.session)),
        hasSession: (id) => Effect.sync(() => sessions.has(id)),
        stopAll: () =>
          Effect.sync(() => {
            for (const state of sessions.values()) state.runtime?.close();
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
              status:
                config.allowNativePrompt && catalog && NodeFS.existsSync(config.binaryPath)
                  ? "ready"
                  : "warning",
              auth: { status: "unknown" },
              message:
                discoveryError ||
                (config.allowNativePrompt
                  ? "Chat preview with file references and permission approvals. Uses one native turn per workspace."
                  : "Native prompts disabled until approved."),
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
