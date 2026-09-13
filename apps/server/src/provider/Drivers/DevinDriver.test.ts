import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeURL from "node:url";
import { it, expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { ProviderInstanceId, ThreadId, type ProviderRuntimeEvent } from "@t3tools/contracts";
import { DevinDriver } from "./DevinDriver.ts";

it.effect(
  "T3 provider emits separate visible turns and soft-cancel completion from one native prompt",
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
            environment: [],
            enabled: true,
            config: {
              binaryPath,
              model: "fake",
              allowNativePrompt: true,
              compactionThresholdTokens: "240000",
            },
          });
          yield* Stream.runForEach(instance.adapter.streamEvents, (event) =>
            Effect.sync(() => {
              events.push(event);
              wake?.();
            }),
          ).pipe(Effect.forkScoped);
          yield* instance.adapter.startSession({ threadId, cwd, runtimeMode: "approval-required" });
          const first = yield* instance.adapter.sendTurn({ threadId, input: "FIRST" });
        const completed = yield* Effect.promise(() => take("turn.completed"));
        expect(completed.turnId).toBe(first.turnId);
        const clear = yield* instance.adapter.sendTurn({ threadId, input: "/clear" }).pipe(Effect.exit);
        expect(clear._tag).toBe("Failure");
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
          const log = yield* Effect.promise(() =>
            NodeFSP.readFile(NodePath.join(cwd, ".devin-worker/protocol.jsonl"), "utf8"),
          );
          expect(log.match(/"method":"session\/prompt"/g)).toHaveLength(1);
          expect(log).not.toContain("session/cancel");
        }),
      );
    }),
  10000,
);
