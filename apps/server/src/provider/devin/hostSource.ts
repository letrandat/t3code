// @effect-diagnostics nodeBuiltinImport:off - leaf module: socket paths and the embedded host program, no Effect runtime.
// IPC wire version between DevinDriver/DevinOpenTurn and the lifetime host.
// A stale host keeps serving its thread; the driver only refuses to attach.
export const HOST_WIRE_VERSION = 1;

import * as NodeCrypto from "node:crypto";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

/** The one internal prompt a host ever sends: bootstrap to the waiting Stop hook.
    User tasks arrive through hook releases, never as further prompts. */
export const DEVIN_BOOTSTRAP_PROMPT =
  "Reply READY and let the Stop hook wait. Tasks arrive through that hook. Complete each task, then let the Stop hook wait again. Never start another prompt or poll for work.";

/** Stable socket addresses derived from the run id. macOS caps sun_path at 104
    chars, so run-dir sockets would not bind under deep temp/state dirs. These
    stay short while remaining deterministic: any driver that knows the runId
    recomputes the same paths after a restart or rebuild, which is the property
    the design needs. The run dir keeps a sockets.json copy for forensics. */
export function devinSocketPaths(runId: string): { hookSock: string; hostSock: string } {
  const hash = NodeCrypto.createHash("sha256").update(runId).digest("hex").slice(0, 16);
  return {
    hookSock: NodePath.join(NodeOS.tmpdir(), `t3-devin-hook-${hash}.sock`),
    hostSock: NodePath.join(NodeOS.tmpdir(), `t3-devin-host-${hash}.sock`),
  };
}

// The lifetime host program, written into each run dir as host.mjs and spawned
// detached. Plain node with no dependencies: it must run from a packaged build
// (bundled server, no source tree) via process.execPath, exactly like hook.mjs.
// Plain JS on purpose (no types); keep it free of backticks and ${} so it can
// live in this template literal, and keep it minimal: config, hook script, and
// launch files are all built driver-side. DevinHost.test.ts runs node --check
// over this source plus boots it against the fake ACP peer.
export const hostSource = `// t3 devin lifetime host (wire ${HOST_WIRE_VERSION}). Spawned detached per run;
// survives server restarts, updates, and sleep. Owns one native child, the hook
// socket, and this run dir. argv[2] is the run dir.
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { createInterface } from "node:readline";
import { appendFile, readFile, writeFile, chmod, unlink } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { createHash } from "node:crypto";
import { randomUUID } from "node:crypto";
import { join } from "node:path";

const WIRE = ${HOST_WIRE_VERSION};
const runDir = process.argv[2];
const p = (name) => join(runDir, name);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const record = (value) => (typeof value === "object" && value !== null ? value : {});

const launch = JSON.parse(await readFile(p("launch.json"), "utf8"));
if (
  !launch.runId ||
  !launch.binary ||
  !launch.configPath ||
  !launch.hookToken ||
  !launch.hookSock ||
  !launch.hostSock
) {
  console.error("host: launch.json is missing runId, binary, configPath, hookToken, or sockets.");
  process.exit(1);
}

let phase = "new";
let sessionId;
let promptId;
let promptSent = false;
let firstTask;
let firstTaskRedelivery = false;
let lastDelivered;
let taskSeq = 0;
let redeliveredFor = -1;
let generation = 0;
let spawnCount = 0;
let cancelRequested = false;
let maintenance;
let shuttingDown = false;
let child;
let childPid;
let childEpoch = 0;
let handlingExit = false;
let lastStdoutAt = Date.now();
let bootResolve = () => {};
const booted = new Promise((resolve) => {
  bootResolve = resolve;
});
let hookServer;
let ipcServer;
let client;
let lastExit;
let status = "starting";
const pending = new Map();
const permissions = new Map();
let sequence = 0;
let waiting;

async function log(line) {
  try {
    await appendFile(p("host.log"), new Date().toISOString() + " " + line + "\\n");
  } catch {}
}

function sendEvent(event) {
  if (!client || client.destroyed || !client.writable) return;
  try {
    client.write(JSON.stringify(event) + "\\n");
  } catch {
    client = undefined;
  }
}

function ack(id, extra) {
  sendEvent(Object.assign({ type: "ack", id }, extra));
}

function nack(id, message) {
  sendEvent({ type: "error", id, message });
}

async function writeStatus() {
  const body = {
    wire: WIRE,
    runId: launch.runId,
    threadId: launch.threadId,
    providerInstanceId: launch.providerInstanceId,
    cwd: launch.cwd,
    model: launch.model,
    generation,
    spawnCount,
    hostPid: process.pid,
    childPid,
    sessionId,
    promptId,
    promptSent,
    phase,
    status,
    hookSock: launch.hookSock,
    hostSock: launch.hostSock,
    cancelRequested,
    stdoutIdleMs: Date.now() - lastStdoutAt,
    lastExit,
    updatedAt: new Date().toISOString(),
  };
  try {
    await writeFile(p("status.json"), JSON.stringify(body), { mode: 0o600 });
  } catch {}
}

async function sendChild(message) {
  await appendFile(
    p("protocol.jsonl"),
    JSON.stringify({ generation, direction: "sent", message }) + "\\n",
  );
  if (!child || !child.stdin || !child.stdin.writable)
    throw new Error("Native transport is unavailable. No replacement prompt will be sent.");
  child.stdin.write(JSON.stringify(message) + "\\n");
}

function rpc(method, params) {
  const id = ++sequence;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    sendChild({ jsonrpc: "2.0", id, method, params }).catch((error) => {
      pending.delete(id);
      reject(error);
    });
  });
}

function killGroup(pid, signal) {
  if (!pid) return;
  try {
    process.kill(-pid, signal);
  } catch {}
}

function childAlive() {
  return !!child && child.exitCode === null && child.signalCode === null;
}

function terminal(message) {
  phase = "ended";
  status = "failed";
  for (const request of pending.values()) request.reject(new Error(message));
  pending.clear();
  permissions.clear();
  if (waiting) {
    waiting.destroy();
    waiting = undefined;
  }
  void writeStatus();
  sendEvent({ type: "failed", message });
}

function onHook(socket) {
  const lines = createInterface({ input: socket });
  lines.once("line", (line) => {
    try {
      const envelope = record(JSON.parse(line));
      if (envelope.token !== launch.hookToken) {
        socket.destroy();
        return;
      }
      const data = record(envelope.event);
      if (phase === "ended") return;
      if (data.session_id !== sessionId || !data.prompt_id || (promptId && data.prompt_id !== promptId)) {
        terminal("Native hook identity does not match this run. No continuation sent.");
        return;
      }
      void appendFile(p("hooks.jsonl"), JSON.stringify(data) + "\\n").catch(() =>
        terminal("Hook evidence could not be saved."),
      );
      if (data.hook_event_name === "PreToolUse") {
        const response = cancelRequested
          ? {
              decision: "block",
              reason:
                "Task cancellation requested. Stop work, report partial results, and let the Stop hook wait.",
            }
          : {};
        socket.end(JSON.stringify(response) + "\\n");
        return;
      }
      const id = String(data.prompt_id ?? "");
      if (!id || (promptId && promptId !== id)) {
        terminal("Native prompt identity changed or is missing. Hook remains blocked.");
        return;
      }
      promptId = id;
      if (data.hook_event_name === "PostCompaction") {
        const summary = String(data.summary ?? "");
        sendEvent({ type: "context", mode: maintenance ?? "automatic", summary });
        maintenance = undefined;
        socket.end("{}\\n");
        return;
      }
      if (data.hook_event_name !== "Stop") {
        socket.end("{}\\n");
        return;
      }
      if (waiting) {
        terminal("Unexpected Stop hook; no continuation sent.");
        return;
      }
      waiting = socket;
      if (maintenance) {
        terminal("Stop arrived before context maintenance was confirmed.");
        return;
      }
      phase = "waiting";
      void writeStatus();
      if (firstTask !== undefined) {
        const text = firstTask;
        const redelivery = firstTaskRedelivery;
        firstTask = undefined;
        firstTaskRedelivery = false;
        deliver(text, redelivery);
        return;
      }
      sendEvent({ type: "waiting", promptId, sessionId, cancelled: cancelRequested });
    } catch {
      terminal("Malformed hook event; no continuation sent.");
    }
  });
}

// Release the held Stop hook with a task. Caller validates phase first.
// A redelivery keeps its taskSeq so a second active death goes terminal.
function deliver(text, isRedelivery) {
  phase = "working";
  if (!isRedelivery) {
    taskSeq += 1;
    redeliveredFor = -1;
  }
  lastDelivered = text;
  const socket = waiting;
  waiting = undefined;
  socket.end(JSON.stringify({ decision: "block", reason: text }) + "\\n");
  void writeStatus();
}

function answerPermission(key, result) {
  const entry = permissions.get(key);
  if (!entry) return false;
  permissions.delete(key);
  void sendChild({ jsonrpc: "2.0", id: entry.id, result }).catch((error) => {
    void log("permission reply failed: " + (error instanceof Error ? error.message : String(error)));
  });
  return true;
}

function onChildLine(line) {
  lastStdoutAt = Date.now();
  try {
    const message = record(JSON.parse(line));
    void appendFile(
      p("protocol.jsonl"),
      JSON.stringify({ generation, direction: "received", message }) + "\\n",
    ).catch(() => terminal("Protocol evidence could not be saved."));
    if (typeof message.id === "number" && pending.has(message.id) && !message.method) {
      const request = pending.get(message.id);
      pending.delete(message.id);
      if (message.error) request.reject(new Error(JSON.stringify(message.error)));
      else request.resolve(record(message.result));
    } else if (message.method === "session/update") {
      const params = record(message.params);
      // Devin sends config updates before session/new returns its identity.
      if (!sessionId) return;
      if (params.sessionId !== sessionId) {
        terminal("Native update identity does not match this run.");
        return;
      }
      const update = record(params.update);
      const content = record(update.content);
      if (
        update.sessionUpdate === "agent_message_chunk" &&
        content.type === "text" &&
        phase === "working"
      ) {
        sendEvent({ type: "text", text: String(content.text ?? "") });
      }
    } else if (message.method && message.id !== undefined) {
      if (message.method === "session/request_permission") {
        if (typeof message.id !== "string" && typeof message.id !== "number")
          throw new Error("Invalid permission request ID.");
        const request = record(message.params);
        if (request.sessionId !== sessionId || cancelRequested || phase === "ended") {
          void sendChild({
            jsonrpc: "2.0",
            id: message.id,
            result: { outcome: { outcome: "cancelled" } },
          }).catch((error) => terminal(error instanceof Error ? error.message : String(error)));
        } else {
          // Held while detached; replayed on the next hello. Never auto-sent.
          const key = randomUUID();
          permissions.set(key, { id: message.id, request });
          sendEvent({ type: "permission", key, id: message.id, request });
        }
      } else {
        void sendChild({
          jsonrpc: "2.0",
          id: message.id,
          error: { code: -32601, message: "Unsupported client request" },
        }).catch((error) => terminal(error instanceof Error ? error.message : String(error)));
      }
    }
  } catch {
    void log("malformed native ACP line; treating the child as dead.");
    void onChildDead("Malformed native ACP message.");
  }
}

async function sha256File(path) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

async function spawnChild(first, isRedelivery) {
  spawnCount += 1;
  const epoch = ++childEpoch;
  // Never respawn from swapped bytes: an update that replaces the binary
  // under a live run must surface loudly instead of mixing versions.
  if (launch.binarySha256) {
    let digest = "";
    try {
      digest = await sha256File(launch.binary);
    } catch {}
    if (digest !== launch.binarySha256)
      throw new Error(
        "Native binary changed under this run (expected sha " +
          String(launch.binarySha256).slice(0, 12) +
          "). Stop the thread for a fresh run; no prompt sent.",
      );
  }
  const env = Object.assign({}, launch.env);
  for (const key of Object.keys(env)) {
    if (key.startsWith("DEVIN_CONTROL_")) delete env[key];
  }
  env.DEVIN_CONTROL_DIR = runDir;
  env.DEVIN_CONTROL_RUN_ID = launch.runId;
  env.DEVIN_CONTROL_ABI = "1";
  // exec preserves the shell PID; inherited child settings retain that owner's PID.
  child = spawn(
    "/bin/sh",
    ["-c", 'export DEVIN_CONTROL_OWNER_PID=$$; exec "$@"', "t3-devin", launch.binary].concat(
      launch.args ?? [],
      ["--config", launch.configPath, "acp", "--model", launch.model],
    ),
    { cwd: launch.cwd, detached: true, env, stdio: "pipe" },
  );
  childPid = child.pid;
  phase = "bootstrapping";
  cancelRequested = false;
  sessionId = undefined;
  promptId = undefined;
  if (waiting) {
    waiting.destroy();
    waiting = undefined;
  }
  for (const request of pending.values()) request.reject(new Error("Native child was replaced."));
  pending.clear();
  // The dead child's permission requests can never be answered; the driver
  // resolves its side as cancelled when it sees the respawn event.
  permissions.clear();
  child.on("error", (error) => {
    if (epoch !== childEpoch) return;
    void log("child error: " + (error instanceof Error ? error.message : String(error)));
    void onChildDead(error instanceof Error ? error.message : String(error));
  });
  child.on("exit", (code, signal) => {
    if (epoch !== childEpoch) return;
    void onChildExit(code, signal);
  });
  child.stderr.on("data", (data) => {
    void appendFile(p("stderr.log"), data).catch(() => {});
  });
  createInterface({ input: child.stdout }).on("line", onChildLine);
  await writeFile(
    p("identity.json"),
    JSON.stringify({
      runId: launch.runId,
      threadId: launch.threadId,
      providerInstanceId: launch.providerInstanceId,
      cwd: launch.cwd,
      pid: childPid,
      hostPid: process.pid,
      status: "starting",
    }),
    { mode: 0o600 },
  );
  await writeStatus();
  await rpc("initialize", {
    protocolVersion: 1,
    clientCapabilities: {},
    clientInfo: { name: "t3-devin", version: "0.1" },
  });
  const session = await rpc("session/new", { cwd: launch.cwd, mcpServers: [] });
  // The binary advertises its own models; refuse drift before any prompt.
  const modelOption = (Array.isArray(session.configOptions) ? session.configOptions : [])
    .map(record)
    .find((item) => item.id === "model");
  const advertised = [];
  const walk = (items) => {
    for (const item of items) {
      const entry = record(item);
      if (Array.isArray(entry.options)) walk(entry.options);
      else {
        const value = entry.value ?? entry.modelId;
        if (typeof value === "string" && value) advertised.push(value);
      }
    }
  };
  walk(
    modelOption
      ? Array.isArray(modelOption.options)
        ? modelOption.options
        : []
      : (() => {
          const models = record(session.models);
          return Array.isArray(models.availableModels) ? models.availableModels : [];
        })(),
  );
  if (!advertised.length)
    throw new Error(
      "Devin did not report any allowed models. Retry model discovery in Settings > Providers.",
    );
  if (!advertised.includes(launch.model))
    throw new Error(
      "Selected Devin model is no longer allowed; no prompt sent. Retry discovery.",
    );
  const served = modelOption
    ? String(modelOption.currentValue ?? "")
    : String(record(session.models).currentModelId ?? "");
  if (served !== launch.model)
    throw new Error("Selected model " + served + " differs from " + launch.model + "; no prompt sent.");
  if (typeof session.sessionId !== "string") throw new Error("ACP did not return a session ID.");
  sessionId = session.sessionId;
  await writeFile(
    p("identity.json"),
    JSON.stringify({
      runId: launch.runId,
      threadId: launch.threadId,
      providerInstanceId: launch.providerInstanceId,
      cwd: launch.cwd,
      pid: childPid,
      hostPid: process.pid,
      sessionId,
      status: "connected",
    }),
    { mode: 0o600 },
  );
  await writeStatus();
  // Generation 0 bootstraps lazily on the first submit. A respawned child
  // always needs its own bootstrap to reach the waiting Stop hook, with or
  // without a redelivered task.
  if (first !== undefined || promptSent) {
    promptSent = true;
    firstTask = first;
    firstTaskRedelivery = isRedelivery === true;
    phase = "bootstrapping";
    await writeStatus();
    // A respawn rejects the previous generation's prompt; only this
    // generation's own completion or failure may go terminal.
    const promptEpoch = epoch;
    void rpc("session/prompt", {
      sessionId,
      prompt: [{ type: "text", text: launch.bootstrap }],
    }).then(
      () => {
        if (promptEpoch === childEpoch) terminal("Original native prompt completed. No replacement was sent.");
      },
      (error) => {
        if (promptEpoch !== childEpoch || shuttingDown) return;
        terminal(error instanceof Error ? error.message : String(error));
      },
    );
  }
  // Last: only a fully settled child (bootstrap decision made) may receive
  // clients. An earlier gate lets the first submit race the lazy bootstrap
  // and double-prompt the child.
  bootResolve();
}

async function onChildDead(reason) {
  if (shuttingDown || phase === "ended") return;
  killGroup(childPid, "SIGTERM");
  child = undefined;
  childPid = undefined;
  await onChildExit(null, null, reason);
}

async function onChildExit(code, signal, reason) {
  // Spawn failure fires error AND exit for one death; without this guard the
  // respawn path runs twice and twins the child.
  if (handlingExit) return;
  if (shuttingDown) {
    handlingExit = true;
    await completeShutdown();
    return;
  }
  if (phase === "ended") return;
  handlingExit = true;
  try {
    await onChildExitInner(code, signal, reason);
  } finally {
    handlingExit = false;
  }
}

async function onChildExitInner(code, signal, reason) {
  let tail = "";
  try {
    const stderr = await readFile(p("stderr.log"), "utf8").catch(() => "");
    tail = stderr.slice(-2000);
  } catch {}
  lastExit = {
    code,
    signal,
    reason: reason ?? null,
    stderrTail: tail,
    generation,
    at: new Date().toISOString(),
  };
  await log(
    "child exit generation=" +
      generation +
      " code=" +
      String(code) +
      " signal=" +
      String(signal) +
      (reason ? " reason=" + reason : "") +
      (tail ? " stderr=" + JSON.stringify(tail.slice(-500)) : ""),
  );
  killGroup(childPid, "SIGTERM");
  child = undefined;
  childPid = undefined;
  if (waiting) {
    waiting.destroy();
    waiting = undefined;
  }
  const idle = phase === "waiting" || (!promptSent && phase !== "working");
  // A cancelled task stays cancelled: a crash in the cancel window must not
  // resurrect it via redelivery.
  const cancelled = cancelRequested;
  const redeliver = firstTask ?? lastDelivered;
  if (spawnCount >= 4) {
    terminal(
      "Native process ended " +
        spawnCount +
        " times (last code=" +
        String(code) +
        " signal=" +
        String(signal) +
        "). " +
        (tail ? "Stderr tail: " + tail.slice(-500) : "No stderr was captured.") +
        " No replacement prompt was sent.",
    );
    return;
  }
  if (!idle && !cancelled && redeliveredFor === taskSeq) {
    terminal(
      "Native process ended while a task was running and the retry already failed" +
        " (code=" +
        String(code) +
        " signal=" +
        String(signal) +
        "). " +
        (tail ? "Stderr tail: " + tail.slice(-500) : "No stderr was captured."),
    );
    return;
  }
  generation += 1;
  // Idle death resumes clean: the completed task must NOT be re-delivered.
  // Active death replays the in-flight task exactly once (same taskSeq).
  const resume = idle || cancelled ? undefined : redeliver;
  if (!idle && !cancelled) redeliveredFor = taskSeq;
  try {
    await spawnChild(resume, !idle && !cancelled);
  } catch (error) {
    terminal(error instanceof Error ? error.message : String(error));
    return;
  }
  if (cancelled) cancelRequested = true;
  await writeStatus();
  sendEvent({
    type: "respawned",
    generation,
    sessionId,
    redelivered: !idle && !cancelled,
    reason:
      "Native process ended (code=" +
      String(code) +
      " signal=" +
      String(signal) +
      ")" +
      (tail ? ". Stderr tail: " + tail.slice(-500) : ". No stderr was captured."),
  });
}

async function completeShutdown() {
  if (status === "ended") {
    process.exit(0);
    return;
  }
  status = "ended";
  phase = "ended";
  try {
    await writeFile(p("shutdown.request"), JSON.stringify({ at: new Date().toISOString() }), {
      mode: 0o600,
    });
  } catch {}
  await writeStatus();
  if (client && !client.destroyed) {
    try {
      client.end(JSON.stringify({ type: "bye" }) + "\\n");
    } catch {}
  }
  await sleep(100);
  process.exit(0);
}

async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  for (const key of [...permissions.keys()]) answerPermission(key, { outcome: { outcome: "cancelled" } });
  if (waiting) {
    // Let the child unwind its prompt instead of stranding hooks; the group
    // kill below bounds the wait either way.
    try {
      waiting.end("{}\\n");
    } catch {}
    waiting = undefined;
  }
  async function settled(ms) {
    const deadline = Date.now() + ms;
    while (childAlive() && Date.now() < deadline) await sleep(100);
    return !childAlive();
  }
  if (await settled(2000)) {
    await completeShutdown();
    return;
  }
  killGroup(childPid, "SIGTERM");
  if (await settled(2000)) {
    await completeShutdown();
    return;
  }
  killGroup(childPid, "SIGKILL");
  await sleep(500);
  await completeShutdown();
}

function onClient(socket) {
  let helloed = false;
  const lines = createInterface({ input: socket });
  lines.on("line", (line) => {
    let message;
    try {
      message = record(JSON.parse(line));
    } catch {
      socket.destroy();
      return;
    }
    if (!helloed) {
      if (message.type !== "hello" || message.runId !== launch.runId) {
        socket.destroy();
        return;
      }
      if (message.wire !== WIRE) {
        try {
          socket.end(
            JSON.stringify({
              type: "error",
              id: message.id ?? null,
              message:
                "Host wire " + WIRE + " does not match driver wire " + String(message.wire) + ".",
            }) + "\\n",
          );
        } catch {}
        socket.destroy();
        return;
      }
      helloed = true;
      if (client && client !== socket) {
        try {
          client.destroy();
        } catch {}
      }
      client = socket;
      status = shuttingDown ? status : "attached";
      void writeFile(
        p("host.claim"),
        JSON.stringify({
          hostPid: process.pid,
          wire: WIRE,
          runId: launch.runId,
          startedAt: startedAt,
          clientPid: message.pid ?? null,
          attachedAt: new Date().toISOString(),
        }),
        { mode: 0o600 },
      ).catch(() => {});
      void writeStatus();
      // Never report a session the child has not confirmed yet. The init
      // status carries the current phase; held Stop hooks are NOT replayed as
      // waiting events, which the driver would mistake for a turn completion.
      // Pending permission requests ARE replayed: their tool calls are still
      // blocked and need an answer.
      void booted.then(() => {
        if (socket !== client) return;
        sendEvent({ type: "init", id: message.id ?? null, wire: WIRE, status: statusSnapshot() });
        for (const [key, entry] of permissions) {
          sendEvent({ type: "permission", key, id: entry.id, request: entry.request });
        }
      });
      return;
    }
    if (socket !== client) return;
    const id = message.id ?? null;
    switch (message.type) {
      case "ping":
        ack(id);
        break;
      case "submit": {
        const text = typeof message.text === "string" ? message.text : "";
        if (!text.trim()) {
          nack(id, "Message is empty.");
        } else if (!sessionId) {
          nack(id, "Devin is still starting.");
        } else if (!promptSent) {
          promptSent = true;
          firstTask = text;
          phase = "bootstrapping";
          void writeStatus();
          const promptEpoch = childEpoch;
          void rpc("session/prompt", {
            sessionId,
            prompt: [{ type: "text", text: launch.bootstrap }],
          }).then(
            () => {
              if (promptEpoch === childEpoch)
                terminal("Original native prompt completed. No replacement was sent.");
            },
            (error) => {
              if (promptEpoch !== childEpoch || shuttingDown) return;
              terminal(error instanceof Error ? error.message : String(error));
            },
          );
          ack(id);
        } else if (phase !== "waiting" || !waiting) {
          nack(id, "Devin is busy or its native turn ended.");
        } else {
          cancelRequested = false;
          deliver(text, false);
          ack(id);
        }
        break;
      }
      case "context": {
        if (phase !== "waiting" || !waiting) {
          nack(id, "Context actions require the waiting Stop hook.");
        } else {
          cancelRequested = false;
          maintenance = "compact";
          phase = "context";
          const socket = waiting;
          waiting = undefined;
          void writeFile(p("compact.request"), "").then(
            () => {
              socket.end(
                JSON.stringify({
                  decision: "block",
                  reason:
                    "Perform pending context maintenance, then reply READY and let the Stop hook wait. Do not begin a task.",
                }) + "\\n",
              );
              void writeStatus();
            },
            (error) => terminal(error instanceof Error ? error.message : String(error)),
          );
          ack(id);
        }
        break;
      }
      case "cancel":
        // Gate here (not driver-side) so a late or repeated cancel never
        // invents a cancellation the native run did not apply.
        if ((phase === "working" || phase === "bootstrapping") && !cancelRequested) {
          cancelRequested = true;
          for (const key of [...permissions.keys()])
            answerPermission(key, { outcome: { outcome: "cancelled" } });
          if (phase === "bootstrapping") firstTask = undefined;
          sendEvent({ type: "cancel-requested" });
          void writeStatus();
        }
        ack(id);
        break;
      case "respond-permission":
        if (typeof message.key !== "string" || !record(message.result)) {
          nack(id, "Permission response is missing its key or result.");
        } else if (!answerPermission(message.key, record(message.result))) {
          nack(id, "This approval request is no longer pending.");
        } else {
          ack(id);
        }
        break;
      case "shutdown":
        ack(id);
        void shutdown();
        break;
      default:
        nack(id, "Unknown host command: " + String(message.type) + ".");
        break;
    }
  });
  socket.on("close", () => {
    if (socket === client) {
      client = undefined;
      if (!shuttingDown && status === "attached") {
        status = "detached";
        void writeStatus();
      }
    }
  });
  socket.on("error", () => {
    socket.destroy();
  });
}

function statusSnapshot() {
  return {
    wire: WIRE,
    runId: launch.runId,
    threadId: launch.threadId,
    cwd: launch.cwd,
    model: launch.model,
    generation,
    spawnCount,
    hostPid: process.pid,
    hookSock: launch.hookSock,
    hostSock: launch.hostSock,
    childPid,
    sessionId,
    promptId,
    promptSent,
    phase,
    status,
    cancelRequested,
    stdoutIdleMs: Date.now() - lastStdoutAt,
    pendingPermissions: permissions.size,
    lastExit,
  };
}

const startedAt = new Date().toISOString();
await writeFile(
  p("host.claim"),
  JSON.stringify({ hostPid: process.pid, wire: WIRE, runId: launch.runId, startedAt }),
  { mode: 0o600 },
);
await writeStatus();
async function listenOn(server, path) {
  // Fresh runIds own fresh paths; a leftover file is always a stale socket
  // from a dead run, never a live host.
  await unlink(path).catch(() => {});
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(path, resolve);
  });
  await chmod(path, 0o600);
}
hookServer = createServer(onHook);
await listenOn(hookServer, launch.hookSock);
ipcServer = createServer(onClient);
ipcServer.on("error", (error) => {
  void log("ipc error: " + (error instanceof Error ? error.message : String(error)));
});
await listenOn(ipcServer, launch.hostSock);
process.on("SIGHUP", () => {});
process.on("SIGTERM", () => {
  void log("host received SIGTERM; stopping the child.");
  killGroup(childPid, "SIGTERM");
  status = "failed";
  lastExit = { reason: "host SIGTERM", generation, at: new Date().toISOString() };
  void writeStatus().finally(() => process.exit(143));
});
process.on("SIGINT", () => {
  void log("host received SIGINT; stopping the child.");
  killGroup(childPid, "SIGTERM");
  status = "failed";
  lastExit = { reason: "host SIGINT", generation, at: new Date().toISOString() };
  void writeStatus().finally(() => process.exit(130));
});
const heartbeat = setInterval(() => {
  void writeStatus();
}, 30000);
heartbeat.unref();
await spawnChild(undefined, false);
`;
