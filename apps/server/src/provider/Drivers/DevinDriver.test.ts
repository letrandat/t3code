// @effect-diagnostics nodeBuiltinImport:off - the suite spawns fake native peers and reads their evidence files directly.
// @effect-diagnostics preferSchemaOverJson:off - assertions parse the host's own JSON evidence files.
// @effect-diagnostics globalTimers:off - host-phase polling in tests; no Effect runtime in these helpers.
// @effect-diagnostics globalDate:off - same: wall-clock test deadlines without a Clock service.
import { HostProcessPlatform, HostProcessArchitecture } from "@t3tools/shared/hostProcess";
import * as NodeCrypto from "node:crypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ServerConfig } from "../../config.ts";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeURL from "node:url";
import { it, expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import {
  ApprovalRequestId,
  DevinSettings,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { attachHealthWarnings, DevinDriver } from "./DevinDriver.ts";

const decodeDevinSettings = Schema.decodeUnknownSync(DevinSettings);

/** Poll host evidence until the run settles; the orphaned turn's completion
    emits no turn event (its session is gone), so the test watches the host. */
async function waitForHostPhase(runDir: string, phase: string, timeoutMs = 15000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const status = JSON.parse(
        await NodeFSP.readFile(NodePath.join(runDir, "status.json"), "utf8"),
      );
      if (status.phase === phase) return;
    } catch {
      /* Still booting. */
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Timed out waiting for host phase ${phase}.`);
}

const testLayers = (cwd: string) =>
  ServerConfig.layerTest(cwd, NodePath.join(cwd, "t3-home")).pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        NodeServices.layer,
        Layer.succeed(HostProcessPlatform, "darwin"),
        Layer.succeed(HostProcessArchitecture, "arm64"),
      ),
    ),
  );

const withFakeDevin = Effect.fn("withFakeDevin")(function* <A, E>(
  body: (input: {
    instance: Effect.Success<ReturnType<(typeof DevinDriver)["create"]>>;
    cwd: string;
    take: (...types: ProviderRuntimeEvent["type"][]) => Promise<ProviderRuntimeEvent>;
  }) => Effect.Effect<A, E>,
) {
  const cwd = yield* Effect.promise(() =>
    NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devin-adapter-")),
  );
  const binaryPath = NodePath.join(cwd, "fake-devin");
  const fake = NodeURL.fileURLToPath(
    new URL("../devin/testFixtures/fakeDevin.mjs", import.meta.url),
  );
  yield* Effect.promise(() =>
    NodeFSP.writeFile(binaryPath, `#!/bin/sh\nexec '${process.execPath}' '${fake}' "$@"\n`, {
      mode: 0o700,
    }),
  );
  const binaryBytes = yield* Effect.promise(() => NodeFSP.readFile(binaryPath));
  yield* Effect.promise(() =>
    NodeFSP.writeFile(
      binaryPath + ".manifest.json",
      JSON.stringify({
        control_abi: 1,
        owner_check: "pid",
        clear: false,
        capabilities: ["private-control-directory", "owner-pid", "compact"],
        output_sha256: NodeCrypto.createHash("sha256").update(binaryBytes).digest("hex"),
      }),
    ),
  );
  const allowedPath = NodePath.join(cwd, "allowed.json");
  yield* Effect.promise(() => NodeFSP.writeFile(allowedPath, '["fake"]'));
  const catalogPath = NodePath.join(cwd, "models.json");
  yield* Effect.promise(() =>
    NodeFSP.writeFile(
      catalogPath,
      '{"families":[{"family_uid":"fake","variants":[{"model_uid":"fake"}]}]}',
    ),
  );
  const events: ProviderRuntimeEvent[] = [];
  let wake: (() => void) | undefined;
  const take = async (...types: ProviderRuntimeEvent["type"][]) => {
    while (true) {
      const index = events.findIndex((event) => types.includes(event.type));
      if (index >= 0) return events.splice(index, 1)[0]!;
      await new Promise<void>((resolve) => {
        wake = resolve;
      });
    }
  };
  yield* Effect.scoped(
    Effect.gen(function* () {
      const instance = yield* DevinDriver.create({
        instanceId: ProviderInstanceId.make("devin-test"),
        displayName: "Devin test",
        environment: [
          { name: "HOME", value: cwd, sensitive: false },
          { name: "DEVIN_TEST_ALLOWED_FILE", value: allowedPath, sensitive: false },
          { name: "DEVIN_TEST_CATALOG_FILE", value: catalogPath, sensitive: false },
        ],
        enabled: true,
        config: decodeDevinSettings({
          binaryPath,
          model: "fake",
          allowNativePrompt: false,
          compactionThresholdTokens: "240000",
        }),
      }).pipe(Effect.provide(testLayers(cwd)));
      yield* Stream.runForEach(instance.adapter.streamEvents, (event) =>
        Effect.sync(() => {
          events.push(event);
          wake?.();
        }),
      ).pipe(Effect.forkScoped);
      // stopAll detaches (hosts survive by design); the test scope must shut
      // every session down or hosts leak past the test.
      yield* body({ instance, cwd, take }).pipe(
        Effect.ensuring(
          Effect.ignore(
            Effect.gen(function* () {
              const sessions = yield* instance.adapter.listSessions();
              for (const session of sessions) {
                yield* Effect.ignore(instance.adapter.stopSession(session.threadId));
              }
            }),
          ),
        ),
      );
    }),
  );
});

it.effect(
  "T3 routes attachments, approvals, and soft cancellation through one native prompt",
  () =>
    Effect.gen(function* () {
      const cwd = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devin-adapter-")),
      );
      const binaryPath = NodePath.join(cwd, "fake-devin");
      const fake = NodeURL.fileURLToPath(
        new URL("../devin/testFixtures/fakeDevin.mjs", import.meta.url),
      );
      yield* Effect.promise(() =>
        NodeFSP.writeFile(binaryPath, `#!/bin/sh\nexec '${process.execPath}' '${fake}' "$@"\n`, {
          mode: 0o700,
        }),
      );
      const binaryBytes = yield* Effect.promise(() => NodeFSP.readFile(binaryPath));
      yield* Effect.promise(() =>
        NodeFSP.writeFile(
          binaryPath + ".manifest.json",
          JSON.stringify({
            control_abi: 1,
            owner_check: "pid",
            clear: false,
            capabilities: ["private-control-directory", "owner-pid", "compact"],
            output_sha256: NodeCrypto.createHash("sha256").update(binaryBytes).digest("hex"),
          }),
        ),
      );
      const allowedPath = NodePath.join(cwd, "allowed.json");
      const allowedIds = '["fake","gpt-6-astra-low","gpt-6-astra-high"]';
      yield* Effect.promise(() => NodeFSP.writeFile(allowedPath, allowedIds));
      const catalogPath = NodePath.join(cwd, "models.json");
      yield* Effect.promise(() =>
        NodeFSP.writeFile(
          catalogPath,
          '{"families":[{"family_uid":"fake","variants":[{"model_uid":"fake"}]},{"family_uid":"gpt-6-astra","variants":[{"model_uid":"gpt-6-astra-low"},{"model_uid":"gpt-6-astra-high"}]}]}',
        ),
      );
      const instanceId = ProviderInstanceId.make("devin-test");
      const threadId = ThreadId.make("devin-thread");
      const events: ProviderRuntimeEvent[] = [];
      let wake: (() => void) | undefined;
      const take = async (type: ProviderRuntimeEvent["type"]) => {
        while (true) {
          const index = events.findIndex((event) => event.type === type);
          if (index >= 0) return events.splice(index, 1)[0]!;
          await new Promise<void>((resolve) => {
            wake = resolve;
          });
        }
      };
      yield* Effect.scoped(
        Effect.gen(function* () {
          const instance = yield* DevinDriver.create({
            instanceId,
            displayName: "Devin test",
            environment: [
              { name: "HOME", value: cwd, sensitive: false },
              { name: "DEVIN_TEST_ALLOWED_FILE", value: allowedPath, sensitive: false },
              { name: "DEVIN_TEST_CATALOG_FILE", value: catalogPath, sensitive: false },
            ],
            enabled: true,
            config: decodeDevinSettings({
              binaryPath,
              model: "fake",
              // Older saved settings must no longer block the first message.
              allowNativePrompt: false,
              compactionThresholdTokens: "240000",
            }),
          }).pipe(Effect.provide(testLayers(cwd)));
          yield* Stream.runForEach(instance.adapter.streamEvents, (event) =>
            Effect.sync(() => {
              events.push(event);
              wake?.();
            }),
          ).pipe(Effect.forkScoped);
          try {
            const snapshot = yield* instance.snapshot.getSnapshot;
            expect(snapshot.models.map((m) => m.slug)).toEqual(["fake", "gpt-6-astra"]);
            const unknown = yield* instance.adapter
              .startSession({
                threadId: ThreadId.make("unknown"),
                cwd,
                runtimeMode: "approval-required",
                modelSelection: { instanceId, model: "unknown" },
              })
              .pipe(Effect.exit);
            expect(unknown._tag).toBe("Failure");
            const selected = yield* instance.adapter.startSession({
              threadId,
              cwd,
              runtimeMode: "approval-required",
              modelSelection: {
                instanceId,
                model: "gpt-6-astra",
                options: [{ id: "reasoningEffort", value: "high" }],
              },
            });
            // The persisted session keeps the picker family slug so the
            // orchestration model lock (picker vs picker) does not mistake an
            // unchanged `gpt-6-astra` + high selection for a switch to the native
            // `gpt-6-astra-high` variant after a soft stop. The native variant
            // stays internal for the live run.
            expect(selected.model).toBe("gpt-6-astra");
            // ProviderService supplies attachment paths before dispatching to the driver.
            const attachmentPath = NodePath.join(cwd, "example.txt");
            yield* Effect.promise(() => NodeFSP.writeFile(attachmentPath, "attached first task"));
            const attachment = {
              type: "file" as const,
              id: "example",
              name: "example.txt",
              mimeType: "text/plain",
              sizeBytes: 19,
            };
            const first = yield* instance.adapter.sendTurn({
              threadId,
              input: `READ_ATTACHMENT\n[Attached file "example.txt" is saved at: ${attachmentPath}]`,
              attachments: [attachment],
            });
            const attachmentDelta = yield* Effect.promise(() => take("content.delta"));
            expect(attachmentDelta.type === "content.delta" && attachmentDelta.payload.delta).toBe(
              "ATTACHMENT_CONTENT:attached first task",
            );
            const completed = yield* Effect.promise(() => take("turn.completed"));
            expect(completed.turnId).toBe(first.turnId);
            const clear = yield* instance.adapter
              .sendTurn({ threadId, input: "/clear" })
              .pipe(Effect.exit);
            expect(clear._tag).toBe("Failure");
            const changedVariant = yield* instance.adapter
              .sendTurn({
                threadId,
                input: "should not send",
                modelSelection: {
                  instanceId,
                  model: "gpt-6-astra",
                  options: [{ id: "reasoningEffort", value: "low" }],
                },
              })
              .pipe(Effect.exit);
            expect(changedVariant._tag).toBe("Failure");
            yield* Effect.promise(() => NodeFSP.writeFile(allowedPath, "[]"));
            const failedRefresh = yield* instance.snapshot.refresh;
            expect(failedRefresh.models).toEqual([]);
            expect(failedRefresh.status).toBe("warning");
            const blocked = yield* instance.adapter
              .startSession({
                threadId: ThreadId.make("blocked"),
                cwd,
                runtimeMode: "approval-required",
              })
              .pipe(Effect.exit);
            expect(blocked._tag).toBe("Failure");
            // Discovery failure blocks new runs, but never replaces the waiting native prompt.
            const second = yield* instance.adapter.sendTurn({ threadId, input: "LONG_TOOL" });
            // Consume previous deltas before awaiting the long-tool marker.
            while (true) {
              const event = yield* Effect.promise(() => take("content.delta"));
              if (event.type === "content.delta" && event.payload.delta === "TOOL_RUNNING") break;
            }
            yield* instance.adapter.interruptTurn(threadId);
            yield* Effect.promise(() => NodeFSP.writeFile(NodePath.join(cwd, "release-tool"), ""));
            const cancelled = yield* Effect.promise(() => take("turn.completed"));
            expect(cancelled.turnId).toBe(second.turnId);
            expect(cancelled.type === "turn.completed" && cancelled.payload.state).toBe(
              "interrupted",
            );
            expect(second.turnId).not.toBe(first.turnId);
            yield* Effect.promise(() => NodeFSP.writeFile(allowedPath, allowedIds));
            const recovered = yield* instance.snapshot.refresh;
            expect(recovered.models.map((m) => m.slug)).toEqual(["fake", "gpt-6-astra"]);
            yield* instance.adapter.startSession({
              threadId: ThreadId.make("recovered"),
              cwd,
              runtimeMode: "approval-required",
            });
            yield* instance.adapter.stopSession(ThreadId.make("recovered"));
            yield* instance.adapter.sendTurn({ threadId, input: "AFTER_CANCEL" });
            yield* Effect.promise(() => take("turn.completed"));
            yield* instance.adapter.sendTurn({ threadId, input: "/compact" });
            yield* Effect.promise(() => take("thread.state.changed"));
            yield* Effect.promise(() => take("turn.completed"));
            const png =
              "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aV1kAAAAASUVORK5CYII=";
            const imagePath = NodePath.join(cwd, "screenshot.png");
            yield* Effect.promise(() => NodeFSP.writeFile(imagePath, Buffer.from(png, "base64")));
            yield* instance.adapter.sendTurn({
              threadId,
              // An attachment-only message becomes this reference in ProviderService.
              input: `[Attached image "screenshot.png" is saved at: ${imagePath}]`,
              attachments: [
                {
                  type: "image",
                  id: "screenshot",
                  name: "screenshot.png",
                  mimeType: "image/png",
                  sizeBytes: Buffer.from(png, "base64").length,
                },
              ],
            });
            while (true) {
              const event = yield* Effect.promise(() => take("content.delta"));
              if (
                event.type === "content.delta" &&
                event.payload.delta.startsWith("ATTACHMENT_CONTENT:PNG:")
              ) {
                expect(event.payload.delta).toBe(`ATTACHMENT_CONTENT:PNG:${png}`);
                break;
              }
            }
            yield* Effect.promise(() => take("turn.completed"));
            for (const decision of ["accept", "acceptAlways", "decline", "cancel"] as const) {
              const turn = yield* instance.adapter.sendTurn({ threadId, input: "ASK_PERMISSION" });
              const opened = yield* Effect.promise(() => take("request.opened"));
              expect(opened.turnId).toBe(turn.turnId);
              expect(opened.providerInstanceId).toBe(instanceId);
              expect(
                opened.type === "request.opened" &&
                  opened.payload.options?.map((option) => option.decision),
              ).toEqual(["accept", "acceptAlways", "decline", "cancel"]);
              const requestId = ApprovalRequestId.make(opened.requestId!);
              const invalid = yield* instance.adapter
                .respondToRequest(threadId, requestId, "acceptForSession")
                .pipe(Effect.exit);
              expect(invalid._tag).toBe("Failure");
              yield* instance.adapter.respondToRequest(threadId, requestId, decision);
              const resolved = yield* Effect.promise(() => take("request.resolved"));
              expect(resolved.requestId).toBe(opened.requestId);
              expect(resolved.type === "request.resolved" && resolved.payload.decision).toBe(
                decision,
              );
              yield* Effect.promise(() => take("turn.completed"));
              const stale = yield* instance.adapter
                .respondToRequest(threadId, requestId, decision)
                .pipe(Effect.exit);
              expect(stale._tag).toBe("Failure");
            }
            yield* instance.adapter.sendTurn({ threadId, input: "ASK_PERMISSION" });
            yield* Effect.promise(() => take("request.opened"));
            yield* instance.adapter.interruptTurn(threadId);
            const interruptedRequest = yield* Effect.promise(() => take("request.resolved"));
            expect(
              interruptedRequest.type === "request.resolved" && interruptedRequest.payload.decision,
            ).toBe("cancel");
            const interruptedTurn = yield* Effect.promise(() => take("turn.completed"));
            expect(interruptedTurn.type === "turn.completed" && interruptedTurn.payload.state).toBe(
              "interrupted",
            );
            const runRoot = NodePath.join(cwd, "t3-home", "userdata", "providers", "devin", "runs");
            const runs = yield* Effect.promise(() => NodeFSP.readdir(runRoot));
            expect(runs).toHaveLength(1);
            const log = yield* Effect.promise(() =>
              NodeFSP.readFile(NodePath.join(runRoot, runs[0]!, "protocol.jsonl"), "utf8"),
            );
            expect(log.match(/"method":"session\/prompt"/g)).toHaveLength(1);
            expect(log).not.toContain("session/cancel");
            for (const optionId of ["once-id", "always-id", "deny-id"]) {
              expect(log).toContain(`"outcome":"selected","optionId":"${optionId}"`);
            }
            yield* instance.adapter.sendTurn({ threadId, input: "ASK_PERMISSION" });
            yield* Effect.promise(() => take("request.opened"));
            yield* instance.adapter.stopSession(threadId);
            const closedRequest = yield* Effect.promise(() => take("request.resolved"));
            expect(
              closedRequest.type === "request.resolved" && closedRequest.payload.decision,
            ).toBe("cancel");
          } finally {
            yield* Effect.ignore(instance.adapter.stopSession(threadId));
          }
        }),
      );
    }),
  30000,
);

it.effect(
  "full-access auto-approves native permissions without request events",
  () =>
    withFakeDevin(({ instance, cwd, take }) =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("devin-full-access");
        yield* instance.adapter.startSession({
          threadId,
          cwd,
          runtimeMode: "full-access",
        });
        const turn = yield* instance.adapter.sendTurn({ threadId, input: "ASK_PERMISSION" });
        const event = yield* Effect.promise(() => take("content.delta", "request.opened"));
        expect(event.type).toBe("content.delta");
        expect(event.turnId).toBe(turn.turnId);
        expect(event.type === "content.delta" && event.payload.delta).toBe(
          'PERMISSION_RESULT:{"outcome":{"outcome":"selected","optionId":"always-id"}}',
        );
        const completed = yield* Effect.promise(() => take("turn.completed"));
        expect(completed.turnId).toBe(turn.turnId);
      }),
    ),
  10000,
);

it.effect(
  "permission-mode switches apply live without a new native run",
  () =>
    withFakeDevin(({ instance, cwd, take }) =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("devin-mode-switch");
        yield* instance.adapter.startSession({
          threadId,
          cwd,
          runtimeMode: "approval-required",
        });
        const first = yield* instance.adapter.sendTurn({ threadId, input: "ASK_PERMISSION" });
        const opened = yield* Effect.promise(() => take("request.opened"));
        expect(opened.turnId).toBe(first.turnId);
        const requestId = ApprovalRequestId.make(opened.requestId!);
        yield* instance.adapter.respondToRequest(threadId, requestId, "cancel");
        yield* Effect.promise(() => take("request.resolved"));
        yield* Effect.promise(() => take("turn.completed"));
        // Mid-thread mode switch syncs live instead of restarting the native run.
        const updated = yield* instance.adapter.startSession({
          threadId,
          cwd,
          runtimeMode: "full-access",
        });
        expect(updated.runtimeMode).toBe("full-access");
        const second = yield* instance.adapter.sendTurn({ threadId, input: "ASK_PERMISSION" });
        // Drain any leftover deltas from the first turn before asserting.
        let auto = yield* Effect.promise(() => take("content.delta", "request.opened"));
        while (auto.type === "content.delta" && auto.turnId !== second.turnId) {
          auto = yield* Effect.promise(() => take("content.delta", "request.opened"));
        }
        expect(auto.type).toBe("content.delta");
        expect(auto.turnId).toBe(second.turnId);
        expect(auto.type === "content.delta" && auto.payload.delta).toBe(
          'PERMISSION_RESULT:{"outcome":{"outcome":"selected","optionId":"always-id"}}',
        );
        yield* Effect.promise(() => take("turn.completed"));
        const runRoot = NodePath.join(cwd, "t3-home", "userdata", "providers", "devin", "runs");
        const runs = yield* Effect.promise(() => NodeFSP.readdir(runRoot));
        expect(runs).toHaveLength(1);
      }),
    ),
  10000,
);

it.effect(
  "restart re-attaches to the same native run without a new prompt",
  () =>
    withFakeDevin(({ instance, cwd, take }) =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("devin-restart");
        yield* instance.adapter.startSession({
          threadId,
          cwd,
          runtimeMode: "approval-required",
        });
        const first = yield* instance.adapter.sendTurn({ threadId, input: "FIRST" });
        while (true) {
          const delta = yield* Effect.promise(() => take("content.delta"));
          if (delta.type === "content.delta" && delta.payload.delta.includes("FIRST")) break;
        }
        const completedFirst = yield* Effect.promise(() => take("turn.completed"));
        expect(completedFirst.turnId).toBe(first.turnId);
        const cursor = first.resumeCursor as { runDir: string; runId: string };
        expect(cursor.runDir).toContain("runs");
        const identityBefore = JSON.parse(
          yield* Effect.promise(() =>
            NodeFSP.readFile(NodePath.join(cursor.runDir, "identity.json"), "utf8"),
          ),
        );
        // A T3 restart: stopAll detaches (hosts survive) and sessions clear.
        yield* instance.adapter.stopAll();
        expect(yield* instance.adapter.hasSession(threadId)).toBe(false);
        // ProviderService injects the persisted cursor on the next turn; the
        // adapter receives it the same way here.
        yield* instance.adapter.startSession({
          threadId,
          cwd,
          runtimeMode: "approval-required",
          resumeCursor: cursor,
        });
        const second = yield* instance.adapter.sendTurn({ threadId, input: "SECOND" });
        while (true) {
          const delta = yield* Effect.promise(() => take("content.delta"));
          if (delta.type === "content.delta" && delta.payload.delta.includes("SECOND")) break;
        }
        const completedSecond = yield* Effect.promise(() => take("turn.completed"));
        expect(completedSecond.turnId).toBe(second.turnId);
        // Same native session, process, and prompt across the restart.
        expect(completedSecond.providerRefs?.providerTurnId).toBe(
          completedFirst.providerRefs?.providerTurnId,
        );
        const identityAfter = JSON.parse(
          yield* Effect.promise(() =>
            NodeFSP.readFile(NodePath.join(cursor.runDir, "identity.json"), "utf8"),
          ),
        );
        expect(identityAfter.pid).toBe(identityBefore.pid);
        expect(identityAfter.sessionId).toBe(identityBefore.sessionId);
        const log = yield* Effect.promise(() =>
          NodeFSP.readFile(NodePath.join(cursor.runDir, "protocol.jsonl"), "utf8"),
        );
        expect(log.match(/"method":"session\/prompt"/g)).toHaveLength(1);
      }),
    ),
  30000,
);

it.effect(
  "re-attach refuses a changed workspace without touching the run",
  () =>
    withFakeDevin(({ instance, cwd, take }) =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("devin-refuse");
        yield* instance.adapter.startSession({
          threadId,
          cwd,
          runtimeMode: "approval-required",
        });
        const first = yield* instance.adapter.sendTurn({ threadId, input: "FIRST" });
        yield* Effect.promise(() => take("turn.completed"));
        const cursor = first.resumeCursor as { runDir: string; runId: string };
        yield* instance.adapter.stopAll();
        const other = yield* Effect.promise(() =>
          NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devin-other-")),
        );
        const error = yield* instance.adapter
          .startSession({
            threadId,
            cwd: other,
            runtimeMode: "approval-required",
            resumeCursor: cursor,
          })
          .pipe(Effect.flip);
        expect(error._tag).toBe("ProviderAdapterRequestError");
        if (error._tag === "ProviderAdapterRequestError")
          expect(error.detail).toMatch(/Workspace changes/);
        expect(yield* instance.adapter.hasSession(threadId)).toBe(false);
        // The live run is untouched: a correct attach still works, one prompt total.
        yield* instance.adapter.startSession({
          threadId,
          cwd,
          runtimeMode: "approval-required",
          resumeCursor: cursor,
        });
        yield* instance.adapter.sendTurn({ threadId, input: "SECOND" });
        yield* Effect.promise(() => take("turn.completed"));
        const log = yield* Effect.promise(() =>
          NodeFSP.readFile(NodePath.join(cursor.runDir, "protocol.jsonl"), "utf8"),
        );
        expect(log.match(/"method":"session\/prompt"/g)).toHaveLength(1);
      }),
    ),
  30000,
);

it.effect(
  "idle host loss warns and starts a fresh run on the next message",
  () =>
    withFakeDevin(({ instance, cwd, take }) =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("devin-host-loss");
        yield* instance.adapter.startSession({
          threadId,
          cwd,
          runtimeMode: "approval-required",
        });
        const first = yield* instance.adapter.sendTurn({ threadId, input: "FIRST" });
        yield* Effect.promise(() => take("turn.completed"));
        const cursor = first.resumeCursor as { runDir: string; runId: string };
        const claim = JSON.parse(
          yield* Effect.promise(() =>
            NodeFSP.readFile(NodePath.join(cursor.runDir, "host.claim"), "utf8"),
          ),
        );
        process.kill(claim.hostPid, "SIGKILL");
        const warning = yield* Effect.promise(() => take("runtime.warning"));
        expect(warning.type === "runtime.warning" && warning.payload.message).toMatch(
          /lost while idle/,
        );
        yield* instance.adapter.sendTurn({ threadId, input: "SECOND" });
        yield* Effect.promise(() => take("turn.completed"));
        const runRoot = NodePath.join(cwd, "t3-home", "userdata", "providers", "devin", "runs");
        const runs = yield* Effect.promise(() => NodeFSP.readdir(runRoot));
        expect(runs).toHaveLength(2);
        for (const run of runs) {
          const log = yield* Effect.promise(() =>
            NodeFSP.readFile(NodePath.join(runRoot, run, "protocol.jsonl"), "utf8"),
          );
          expect(log.match(/"method":"session\/prompt"/g)).toHaveLength(1);
        }
      }),
    ),
  30000,
);

it.effect(
  "restart mid-turn re-attaches and finishes in the background",
  () =>
    withFakeDevin(({ instance, cwd, take }) =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("devin-midturn");
        yield* instance.adapter.startSession({
          threadId,
          cwd,
          runtimeMode: "approval-required",
        });
        const first = yield* instance.adapter.sendTurn({ threadId, input: "LONG_TOOL" });
        while (true) {
          const delta = yield* Effect.promise(() => take("content.delta"));
          if (delta.type === "content.delta" && delta.payload.delta === "TOOL_RUNNING") break;
        }
        const cursor = first.resumeCursor as { runDir: string; runId: string };
        // Detach while the tool runs, then re-attach like a restart would.
        yield* instance.adapter.stopAll();
        yield* instance.adapter.startSession({
          threadId,
          cwd,
          runtimeMode: "approval-required",
          resumeCursor: cursor,
        });
        const warning = yield* Effect.promise(() => take("runtime.warning"));
        expect(warning.type === "runtime.warning" && warning.payload.message).toMatch(/in flight/);
        // The orphaned turn finishes in the background: no turn.completed (its
        // session is gone), but the run goes idle and serves the next turn.
        yield* Effect.promise(() => NodeFSP.writeFile(NodePath.join(cwd, "release-tool"), ""));
        yield* Effect.promise(() => waitForHostPhase(cursor.runDir, "waiting"));
        const second = yield* instance.adapter.sendTurn({ threadId, input: "SECOND" });
        while (true) {
          const delta = yield* Effect.promise(() => take("content.delta"));
          if (delta.type === "content.delta" && delta.payload.delta.includes("SECOND")) break;
        }
        const completed = yield* Effect.promise(() => take("turn.completed"));
        expect(completed.turnId).toBe(second.turnId);
        const log = yield* Effect.promise(() =>
          NodeFSP.readFile(NodePath.join(cursor.runDir, "protocol.jsonl"), "utf8"),
        );
        expect(log.match(/"method":"session\/prompt"/g)).toHaveLength(1);
      }),
    ),
  30000,
);

it.effect(
  "re-attach after a long gap warns about possible sleep",
  () =>
    withFakeDevin(({ instance, cwd, take }) =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("devin-sleep-gap");
        yield* instance.adapter.startSession({
          threadId,
          cwd,
          runtimeMode: "approval-required",
        });
        const first = yield* instance.adapter.sendTurn({ threadId, input: "FIRST" });
        yield* Effect.promise(() => take("turn.completed"));
        const cursor = first.resumeCursor as { runDir: string; runId: string };
        yield* instance.adapter.stopAll();
        // Age the status to simulate a frozen host clock (sleep).
        const statusPath = NodePath.join(cursor.runDir, "status.json");
        const status = JSON.parse(
          yield* Effect.promise(() => NodeFSP.readFile(statusPath, "utf8")),
        );
        yield* Effect.promise(() =>
          NodeFSP.writeFile(
            statusPath,
            JSON.stringify({ ...status, updatedAt: "2020-01-01T00:00:00.000Z" }),
          ),
        );
        yield* instance.adapter.startSession({
          threadId,
          cwd,
          runtimeMode: "approval-required",
          resumeCursor: cursor,
        });
        const warning = yield* Effect.promise(() => take("runtime.warning"));
        expect(warning.type === "runtime.warning" && warning.payload.message).toMatch(
          /unreachable.*sleep/,
        );
        yield* instance.adapter.sendTurn({ threadId, input: "SECOND" });
        yield* Effect.promise(() => take("turn.completed"));
      }),
    ),
  30000,
);

it("attach health warnings cover mid-turn, stuck, and sleep gaps", () => {
  expect(attachHealthWarnings({ phase: "waiting" }, 1000)).toEqual([]);
  expect(attachHealthWarnings({ phase: "working" }, 1000)).toHaveLength(1);
  expect(attachHealthWarnings({ phase: "working", stdoutIdleMs: 999_999 }, 1000)).toHaveLength(2);
  expect(attachHealthWarnings({ phase: "waiting" }, 999_999)).toHaveLength(1);
  expect(attachHealthWarnings({ phase: "waiting", stdoutIdleMs: 999_999 }, 1000)).toEqual([]);
  expect(attachHealthWarnings({}, undefined)).toEqual([]);
});

it.effect(
  "stopped runs refuse re-attach",
  () =>
    withFakeDevin(({ instance, cwd, take }) =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("devin-stopped");
        yield* instance.adapter.startSession({
          threadId,
          cwd,
          runtimeMode: "approval-required",
        });
        const first = yield* instance.adapter.sendTurn({ threadId, input: "FIRST" });
        yield* Effect.promise(() => take("turn.completed"));
        yield* instance.adapter.stopSession(threadId);
        const refused = yield* instance.adapter
          .startSession({
            threadId,
            cwd,
            runtimeMode: "approval-required",
            resumeCursor: first.resumeCursor,
          })
          .pipe(Effect.exit);
        expect(refused._tag).toBe("Failure");
      }),
    ),
  30000,
);
