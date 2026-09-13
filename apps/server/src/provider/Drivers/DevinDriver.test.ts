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
import {
  ApprovalRequestId,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { DevinDriver } from "./DevinDriver.ts";

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
            environment: [{ name: "HOME", value: cwd, sensitive: false }],
            enabled: true,
            config: {
              binaryPath,
              model: "fake",
              allowNativePrompt: true,
              compactionThresholdTokens: "240000",
            },
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
          expect(snapshot.models.map((m) => m.slug)).toEqual(["fake"]);
          const unknown = yield* instance.adapter
            .startSession({
              threadId: ThreadId.make("unknown"),
              cwd,
              runtimeMode: "approval-required",
              modelSelection: { instanceId, model: "unknown" },
            })
            .pipe(Effect.exit);
          expect(unknown._tag).toBe("Failure");
          yield* instance.adapter.startSession({ threadId, cwd, runtimeMode: "approval-required" });
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
                model: "fake",
                options: [{ id: "reasoningEffort", value: "high" }],
              },
            })
            .pipe(Effect.exit);
          expect(changedVariant._tag).toBe("Failure");
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
