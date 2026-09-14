// @effect-diagnostics nodeBuiltinImport:off - the suite boots real hosts over real sockets and reads their evidence files.
// @effect-diagnostics preferSchemaOverJson:off - assertions parse the host's own JSON evidence files.
// @effect-diagnostics globalTimers:off - connection retries and test deadlines use node timers; no Effect runtime here.
// @effect-diagnostics globalDate:off - same: wall-clock deadlines without a Clock service.
// Wire-contract tests for the detached Devin lifetime host. A raw IPC client
// speaks to hostSource directly: this pins the wire protocol the driver
// depends on and proves restart/detach survival without the driver involved.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeFSP from "node:fs/promises";
import * as NodeURL from "node:url";
import { afterEach, describe, expect, it } from "vite-plus/test";
import {
  DEVIN_BOOTSTRAP_PROMPT,
  HOST_WIRE_VERSION,
  devinSocketPaths,
  hostSource,
} from "./hostSource.ts";
import { hookSource } from "./hookSource.ts";
import { buildDevinConfig } from "./DevinLaunchConfig.ts";

type HostMessage = { type: string; id?: number | null; [key: string]: unknown };

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const cleanups: Array<() => unknown> = [];
afterEach(async () => {
  let cleanup = cleanups.pop();
  while (cleanup) {
    await cleanup();
    cleanup = cleanups.pop();
  }
});

async function waitFor(
  body: () => Promise<unknown>,
  what: string,
  timeoutMs = 10000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      if (await body()) return;
    } catch (error) {
      lastError = error;
    }
    await sleep(100);
  }
  throw new Error(`Timed out waiting for ${what}: ${String(lastError ?? "condition never met")}`);
}

class RawClient {
  private buffer = "";
  private events: HostMessage[] = [];
  private wake: (() => void) | undefined;
  private nextId = 1;
  private dead = false;
  private pending = new Map<
    number,
    { resolve: (value: HostMessage) => void; reject: (error: Error) => void }
  >();

  private readonly socket: NodeNet.Socket;
  private constructor(socket: NodeNet.Socket) {
    this.socket = socket;
    socket.on("data", (chunk) => {
      this.buffer += chunk.toString("utf8");
      const lines = this.buffer.split("\n");
      this.buffer = lines.pop() ?? "";
      for (const line of lines) {
        if (!line.trim()) continue;
        const message = JSON.parse(line) as HostMessage;
        if (
          (message.type === "ack" || message.type === "error" || message.type === "init") &&
          typeof message.id === "number" &&
          this.pending.has(message.id)
        ) {
          this.pending.get(message.id)!.resolve(message);
          this.pending.delete(message.id);
        } else {
          this.events.push(message);
        }
        this.wake?.();
      }
    });
    const fail = () => {
      this.dead = true;
      for (const entry of this.pending.values()) entry.reject(new Error("Host connection closed."));
      this.pending.clear();
      this.wake?.();
    };
    socket.on("close", fail);
    socket.on("error", () => socket.destroy());
  }

  static async connect(
    hostSock: string,
    runId: string,
    wire: number = HOST_WIRE_VERSION,
  ): Promise<{ client: RawClient; init: HostMessage }> {
    const deadline = Date.now() + 10000;
    let lastError: unknown;
    while (Date.now() < deadline) {
      try {
        const socket = await new Promise<NodeNet.Socket>((resolve, reject) => {
          const attempt = NodeNet.connect(hostSock);
          attempt.once("connect", () => resolve(attempt));
          attempt.once("error", reject);
        });
        const client = new RawClient(socket);
        const id = client.nextId++;
        const init = new Promise<HostMessage>((resolve, reject) => {
          client.pending.set(id, { resolve, reject });
        });
        socket.write(JSON.stringify({ type: "hello", id, wire, pid: process.pid, runId }) + "\n");
        return { client, init: await init };
      } catch (error) {
        lastError = error;
        await sleep(100);
      }
    }
    throw new Error(`Could not connect to host: ${String(lastError)}`);
  }

  command(type: string, extra: Record<string, unknown> = {}): Promise<HostMessage> {
    const id = this.nextId++;
    return new Promise<HostMessage>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Timed out waiting for ${type} ack.`));
      }, 10000);
      this.pending.set(id, {
        resolve: (message) => {
          clearTimeout(timer);
          resolve(message);
        },
        reject: (error) => {
          clearTimeout(timer);
          reject(error);
        },
      });
      this.socket.write(JSON.stringify({ type, id, ...extra }) + "\n");
    });
  }

  async take<T = HostMessage>(...types: string[]): Promise<T> {
    const deadline = Date.now() + 15000;
    while (true) {
      const index = this.events.findIndex((event) => types.includes(event.type));
      if (index >= 0) return this.events.splice(index, 1)[0]! as unknown as T;
      if (this.dead)
        throw new Error(`Host connection closed while waiting for ${types.join("/")}.`);
      const remaining = deadline - Date.now();
      if (remaining <= 0)
        throw new Error(
          `Timed out waiting for ${types.join("/")} (queued: ${this.events.map((e) => e.type).join(",") || "none"}).`,
        );
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, remaining);
        this.wake = () => {
          clearTimeout(timer);
          resolve();
        };
      });
    }
  }

  destroy() {
    this.socket.destroy();
  }
}

async function setupRun(
  extraEnv: Record<string, string> = {},
  launchOverrides: Record<string, unknown> = {},
) {
  const cwd = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devin-host-"));
  const runId = NodeCrypto.randomUUID();
  const runDir = NodePath.join(cwd, "runs", runId);
  await NodeFSP.mkdir(runDir, { recursive: true, mode: 0o700 });
  const { hookSock, hostSock } = devinSocketPaths(runId);
  const hookToken = NodeCrypto.randomBytes(32).toString("hex");
  const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";
  const hookCommand = [process.execPath, NodePath.join(runDir, "hook.mjs"), hookSock, hookToken]
    .map(quote)
    .join(" ");
  const environment = { ...process.env, HOME: cwd, ...extraEnv };
  const configPath = NodePath.join(runDir, "config.json");
  await NodeFSP.writeFile(
    configPath,
    JSON.stringify(
      await buildDevinConfig({
        cwd,
        environment,
        hookCommand,
        model: "fake-model",
        compactionThresholdTokens: 240000,
      }),
    ),
    { mode: 0o600 },
  );
  await NodeFSP.writeFile(NodePath.join(runDir, "hook.mjs"), hookSource, { mode: 0o600 });
  await NodeFSP.writeFile(NodePath.join(runDir, "host.mjs"), hostSource, { mode: 0o600 });
  await NodeFSP.writeFile(
    NodePath.join(runDir, "sockets.json"),
    JSON.stringify({ hookSock, hostSock }),
    {
      mode: 0o600,
    },
  );
  const { pinBinary, ...restOverrides } = launchOverrides;
  const launch: Record<string, unknown> = {
    wire: HOST_WIRE_VERSION,
    runId,
    threadId: "test-thread",
    providerInstanceId: "test-provider",
    cwd,
    binary: process.execPath,
    args: [NodeURL.fileURLToPath(new URL("./testFixtures/fakeDevin.mjs", import.meta.url))],
    model: "fake-model",
    configPath,
    hookToken,
    hookSock,
    hostSock,
    env: environment,
    bootstrap: DEVIN_BOOTSTRAP_PROMPT,
    ...restOverrides,
  };
  if (pinBinary === true)
    launch.binarySha256 = NodeCrypto.createHash("sha256")
      .update(await NodeFSP.readFile(launch.binary as string))
      .digest("hex");
  await NodeFSP.writeFile(NodePath.join(runDir, "launch.json"), JSON.stringify(launch), {
    mode: 0o600,
  });
  const host = NodeChildProcess.spawn(
    process.execPath,
    [NodePath.join(runDir, "host.mjs"), runDir],
    {
      detached: true,
      stdio: "ignore",
    },
  );
  host.unref();
  const clients: RawClient[] = [];
  cleanups.push(async () => {
    for (const client of clients) client.destroy();
    // Prefer a graceful shutdown so the child group is reaped; fall back to
    // killing whatever the claim/status files still name.
    try {
      await Promise.race([
        (async () => {
          const { client } = await RawClient.connect(hostSock, runId);
          clients.push(client);
          await client.command("shutdown");
          await client.take("bye");
        })(),
        sleep(3000),
      ]);
    } catch {
      // Already gone or refusing; kill below.
    }
    for (const file of ["host.claim", "status.json"]) {
      try {
        const data = JSON.parse(await NodeFSP.readFile(NodePath.join(runDir, file), "utf8"));
        for (const pid of [data.hostPid, data.childPid]) {
          if (typeof pid === "number") {
            try {
              process.kill(pid, "SIGKILL");
            } catch {}
            try {
              process.kill(-pid, "SIGKILL");
            } catch {}
          }
        }
      } catch {}
    }
  });
  const track = async (wire?: number) => {
    const { client, init } = await RawClient.connect(hostSock, runId, wire);
    clients.push(client);
    return { client, init };
  };
  return { cwd, runDir, runId, track };
}

const readJson = async (dir: string, name: string) =>
  JSON.parse(await NodeFSP.readFile(NodePath.join(dir, name), "utf8"));

describe("devin lifetime host", () => {
  it("embedded source passes node --check", async () => {
    const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devin-host-check-"));
    const file = NodePath.join(dir, "host.mjs");
    await NodeFSP.writeFile(file, hostSource);
    await new Promise<void>((resolve, reject) => {
      NodeChildProcess.execFile(process.execPath, ["--check", file], (error) =>
        error ? reject(error) : resolve(),
      );
    });
  });

  it("boots one native child and delivers tasks through one prompt", async () => {
    const { runDir, track } = await setupRun();
    const { client, init } = await track();
    expect(init.type).toBe("init");
    expect(init.wire).toBe(HOST_WIRE_VERSION);
    const status = init.status as Record<string, unknown>;
    expect(status.runId).toBeDefined();
    expect(typeof status.sessionId).toBe("string");
    expect(status.promptSent).toBe(false);

    expect((await client.command("submit", { text: "FIRST" })).type).toBe("ack");
    expect((await client.take<{ text: string }>("text")).text).toMatch(/FIRST/);
    const first = await client.take<{ promptId: string; sessionId: string }>("waiting");
    expect((await client.command("submit", { text: "SECOND" })).type).toBe("ack");
    expect((await client.take<{ text: string }>("text")).text).toMatch(/SECOND/);
    const second = await client.take<{ promptId: string; sessionId: string }>("waiting");
    expect(second.promptId).toBe(first.promptId);
    expect(second.sessionId).toBe(first.sessionId);

    const protocol = (await NodeFSP.readFile(NodePath.join(runDir, "protocol.jsonl"), "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    const prompts = protocol.filter(
      (entry) => entry.direction === "sent" && entry.message.method === "session/prompt",
    );
    expect(prompts).toHaveLength(1);
    expect(prompts[0].generation).toBe(0);
    expect(JSON.stringify(prompts[0])).not.toMatch(/FIRST/);
    const identity = await readJson(runDir, "identity.json");
    expect(identity.status).toBe("connected");
    expect(typeof identity.hostPid).toBe("number");
  }, 30000);

  it("forwards permission requests raw and resolves them", async () => {
    const { track } = await setupRun();
    const { client } = await track();
    expect((await client.command("submit", { text: "ASK_PERMISSION" })).type).toBe("ack");
    const request = await client.take<{
      key: string;
      id: string | number;
      request: { toolCall: { title: string } };
    }>("permission");
    expect(typeof request.key).toBe("string");
    expect(request.request.toolCall.title).toBe("Run command");
    expect(
      (
        await client.command("respond-permission", {
          key: request.key,
          result: { outcome: { outcome: "selected", optionId: "deny-id" } },
        })
      ).type,
    ).toBe("ack");
    expect((await client.take<{ text: string }>("text")).text).toBe(
      'PERMISSION_RESULT:{"outcome":{"outcome":"selected","optionId":"deny-id"}}',
    );
    await client.take("waiting");
    const stale = await client.command("respond-permission", {
      key: request.key,
      result: { outcome: { outcome: "cancelled" } },
    });
    expect(stale.type).toBe("error");
    expect(String(stale.message)).toMatch(/no longer pending/);
  }, 30000);

  it("detach and re-attach continue the same native process", async () => {
    const protoFile = NodePath.join(
      await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devin-host-proto-")),
      "methods.jsonl",
    );
    const { runDir, runId, track } = await setupRun({ DEVIN_TEST_PROTOCOL: protoFile });
    const firstClient = await track();
    expect((await firstClient.client.command("submit", { text: "FIRST" })).type).toBe("ack");
    await firstClient.client.take("waiting");
    firstClient.client.destroy();
    await waitFor(
      async () => (await readJson(runDir, "status.json")).status === "detached",
      "detach",
    );

    const { client, init } = await track();
    const status = init.status as Record<string, unknown>;
    expect(status.phase).toBe("waiting");
    expect(status.promptSent).toBe(true);
    expect(status.sessionId).toBe(`session-${runId}`);
    expect(typeof status.stdoutIdleMs).toBe("number");
    // No waiting replay: init.status already told the client the phase.
    expect((await client.command("submit", { text: "SECOND" })).type).toBe("ack");
    expect((await client.take<{ text: string }>("text")).text).toMatch(/SECOND/);
    const resumed = await client.take<{ promptId: string; sessionId: string }>("waiting");
    expect(resumed.promptId).toBe(`prompt-${runId}`);
    expect(resumed.sessionId).toBe(`session-${runId}`);

    const methods = (await NodeFSP.readFile(protoFile, "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    // One native process (single pid) served both connections, one prompt total.
    expect(new Set(methods.map((entry) => entry.pid)).size).toBe(1);
    expect(methods.filter((entry) => entry.method === "session/prompt")).toHaveLength(1);
  }, 30000);

  it("idle child death respawns without redelivering the completed task", async () => {
    const { runDir, track } = await setupRun();
    const { client } = await track();
    expect((await client.command("submit", { text: "FIRST" })).type).toBe("ack");
    await client.take("text");
    await client.take("waiting");
    const before = await readJson(runDir, "status.json");
    process.kill(before.childPid, "SIGTERM");

    const respawned = await client.take<{
      generation: number;
      redelivered: boolean;
      reason: string;
    }>("respawned");
    expect(respawned.generation).toBe(1);
    expect(respawned.redelivered).toBe(false);
    expect(respawned.reason).toMatch(/SIGTERM/);
    // No task replay: the fresh child bootstraps straight back to waiting.
    await client.take("waiting");
    expect((await client.command("submit", { text: "SECOND" })).type).toBe("ack");
    expect((await client.take<{ text: string }>("text")).text).toMatch(/SECOND/);
    await client.take("waiting");

    const protocol = (await NodeFSP.readFile(NodePath.join(runDir, "protocol.jsonl"), "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    const prompts = protocol.filter(
      (entry) => entry.direction === "sent" && entry.message.method === "session/prompt",
    );
    expect(prompts.map((entry) => entry.generation)).toEqual([0, 1]);
    expect(await NodeFSP.readFile(NodePath.join(runDir, "host.log"), "utf8")).toMatch(
      /code=null signal=SIGTERM/,
    );
  }, 30000);

  it("active child death redelivers the in-flight task once", async () => {
    const { runDir, track } = await setupRun();
    const { client } = await track();
    expect((await client.command("submit", { text: "LONG_TOOL_ISOLATED" })).type).toBe("ack");
    expect((await client.take<{ text: string }>("text")).text).toBe("TOOL_RUNNING");
    process.kill((await readJson(runDir, "status.json")).childPid, "SIGTERM");

    const respawned = await client.take<{ redelivered: boolean; generation: number }>("respawned");
    expect(respawned.redelivered).toBe(true);
    expect(respawned.generation).toBe(1);
    // The redelivered task runs again on the fresh child.
    expect((await client.take<{ text: string }>("text")).text).toBe("TOOL_RUNNING");
    await NodeFSP.writeFile(NodePath.join(runDir, "release-tool"), "");
    expect((await client.take<{ text: string }>("text")).text).toBe("TOOL_EXECUTED");
    await client.take("waiting");
  }, 30000);

  it("a second active death goes terminal with exit evidence", async () => {
    const { runDir, track } = await setupRun();
    const { client } = await track();
    expect((await client.command("submit", { text: "LONG_TOOL_ISOLATED" })).type).toBe("ack");
    expect((await client.take<{ text: string }>("text")).text).toBe("TOOL_RUNNING");
    process.kill((await readJson(runDir, "status.json")).childPid, "SIGTERM");
    const respawned = await client.take<{ redelivered: boolean }>("respawned");
    expect(respawned.redelivered).toBe(true);
    expect((await client.take<{ text: string }>("text")).text).toBe("TOOL_RUNNING");

    process.kill((await readJson(runDir, "status.json")).childPid, "SIGTERM");
    const failed = await client.take<{ message: string }>("failed");
    expect(failed.message).toMatch(/retry already failed/);
    expect(failed.message).toMatch(/SIGTERM/);
    await waitFor(
      async () => (await readJson(runDir, "status.json")).status === "failed",
      "failed status",
    );
  }, 30000);

  it("shutdown stops the child group and marks the run deliberate", async () => {
    const { runDir, track } = await setupRun();
    const { client } = await track();
    expect((await client.command("submit", { text: "FIRST" })).type).toBe("ack");
    await client.take("waiting");
    const before = await readJson(runDir, "status.json");
    expect((await client.command("shutdown")).type).toBe("ack");
    expect((await client.take("bye")).type).toBe("bye");

    await waitFor(
      async () => (await readJson(runDir, "status.json")).status === "ended",
      "ended status",
    );
    const marker = await readJson(runDir, "shutdown.request");
    expect(typeof marker.at).toBe("string");
    await waitFor(async () => {
      try {
        process.kill(before.hostPid, 0);
        return false;
      } catch {
        return true;
      }
    }, "host exit");
    expect(() => process.kill(before.childPid, 0)).toThrow();
  }, 30000);

  it("model drift fails the boot without sending a prompt", async () => {
    const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devin-host-allowed-"));
    const allowedPath = NodePath.join(dir, "allowed.json");
    await NodeFSP.writeFile(allowedPath, '["something-else"]');
    const { runDir } = await setupRun({ DEVIN_TEST_ALLOWED_FILE: allowedPath });
    let claim: { hostPid: number } | undefined;
    await waitFor(async () => {
      try {
        claim = JSON.parse(await NodeFSP.readFile(NodePath.join(runDir, "host.claim"), "utf8"));
        return true;
      } catch {
        return false;
      }
    }, "host claim");
    await waitFor(async () => {
      try {
        process.kill(claim!.hostPid, 0);
        return false;
      } catch {
        return true;
      }
    }, "host exit");
    const protocol = await NodeFSP.readFile(NodePath.join(runDir, "protocol.jsonl"), "utf8");
    expect(protocol).toMatch(/session\/new/);
    expect(protocol).not.toMatch(/session\/prompt/);
    const identity = JSON.parse(
      await NodeFSP.readFile(NodePath.join(runDir, "identity.json"), "utf8"),
    );
    expect(identity.status).toBe("starting");
    await expect(NodeFSP.access(NodePath.join(runDir, "shutdown.request"))).rejects.toThrow();
  }, 30000);

  it("respawn refuses a swapped binary without prompting", async () => {
    const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-devin-host-wrapper-"));
    const wrapper = NodePath.join(dir, "fake-devin");
    const fake = NodeURL.fileURLToPath(new URL("./testFixtures/fakeDevin.mjs", import.meta.url));
    await NodeFSP.writeFile(wrapper, `#!/bin/sh\nexec '${process.execPath}' '${fake}' "$@"\n`, {
      mode: 0o700,
    });
    const { runDir, track } = await setupRun(
      {},
      { binary: wrapper, args: [] as string[], pinBinary: true },
    );
    const { client } = await track();
    expect((await client.command("submit", { text: "FIRST" })).type).toBe("ack");
    await client.take("text");
    await client.take("waiting");
    // Same behavior, new bytes: the pin must catch the swap.
    await NodeFSP.appendFile(wrapper, "\n# v2\n");
    process.kill((await readJson(runDir, "status.json")).childPid, "SIGTERM");
    const failed = await client.take<{ message: string }>("failed");
    expect(failed.message).toMatch(/binary changed/);
    const protocol = await NodeFSP.readFile(NodePath.join(runDir, "protocol.jsonl"), "utf8");
    expect(protocol.match(/"method":"session\/prompt"/g)).toHaveLength(1);
    await waitFor(
      async () => (await readJson(runDir, "status.json")).status === "failed",
      "failed status",
    );
  }, 30000);

  it("wire mismatch is refused without killing the host", async () => {
    const { track } = await setupRun();
    const { client, init } = await track(999);
    expect(init.type).toBe("error");
    expect(String(init.message)).toMatch(/wire/);
    client.destroy();
    const { client: healthy, init: retry } = await track();
    expect(retry.type).toBe("init");
    expect((await healthy.command("ping")).type).toBe("ack");
  }, 30000);
});
