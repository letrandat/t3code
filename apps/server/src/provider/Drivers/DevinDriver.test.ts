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
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import {
  ApprovalRequestId,
  DevinSettings,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { DevinDriver } from "./DevinDriver.ts";

const decodeDevinSettings = Schema.decodeUnknownSync(DevinSettings);

const withFakeDevin = Effect.fn("withFakeDevin")(function* (
  body: (input: {
    instance: Effect.Effect.Success<ReturnType<(typeof DevinDriver)["create"]>>;
    cwd: string;
    take: (...types: ProviderRuntimeEvent["type"][]) => Promise<ProviderRuntimeEvent>;
  }) => Effect.Effect<unknown>,
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
      }).pipe(
        Effect.provide(ServerConfig.layerTest(cwd, NodePath.join(cwd, "t3-home"))),
        Effect.provide(NodeServices.layer),
        Effect.provideService(HostProcessPlatform, "darwin"),
        Effect.provideService(HostProcessArchitecture, "arm64"),
      );
      yield* Stream.runForEach(instance.adapter.streamEvents, (event) =>
        Effect.sync(() => {
          events.push(event);
          wake?.();
        }),
      ).pipe(Effect.forkScoped);
      yield* body({ instance, cwd, take });
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
          }).pipe(
            Effect.provide(ServerConfig.layerTest(cwd, NodePath.join(cwd, "t3-home"))),
            Effect.provide(NodeServices.layer),
            Effect.provideService(HostProcessPlatform, "darwin"),
            Effect.provideService(HostProcessArchitecture, "arm64"),
          );
          yield* Stream.runForEach(instance.adapter.streamEvents, (event) =>
            Effect.sync(() => {
              events.push(event);
              wake?.();
            }),
          ).pipe(Effect.forkScoped);
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
          expect(closedRequest.type === "request.resolved" && closedRequest.payload.decision).toBe(
            "cancel",
          );
        }),
      );
    }),
  10000,
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
