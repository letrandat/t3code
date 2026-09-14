// @effect-diagnostics nodeBuiltinImport:off - Effect-free IPC client: raw sockets, process spawn, and file evidence by design.
// @effect-diagnostics globalTimers:off - socket timeouts run on node timers; there is no Effect runtime in this class.
// @effect-diagnostics globalDate:off - same: wall-clock deadlines without a Clock service.
import { HostProcessPlatform, HostProcessArchitecture } from "@t3tools/shared/hostProcess";
import { buildDevinConfig, verifyDevinBinary } from "./DevinLaunchConfig.ts";
import * as NodeNet from "node:net";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import { hookSource } from "./hookSource.ts";
import {
  DEVIN_BOOTSTRAP_PROMPT,
  HOST_WIRE_VERSION,
  devinSocketPaths,
  hostSource,
} from "./hostSource.ts";
import * as NodePath from "node:path";
import * as NodeFSP from "node:fs/promises";
import * as NodeCrypto from "node:crypto";

import * as Schema from "effect/Schema";
import * as AcpSchema from "effect-acp/schema";
import type {
  ProviderApprovalDecision,
  ProviderApprovalOption,
  RuntimeMode,
} from "@t3tools/contracts";

const decodePermissionRequest = Schema.decodeUnknownSync(AcpSchema.RequestPermissionRequest);

export type DevinEvent =
  | {
      type: "permission";
      requestId: string;
      request: AcpSchema.RequestPermissionRequest;
      options: ReadonlyArray<ProviderApprovalOption>;
    }
  | {
      type: "permission-resolved";
      requestId: string;
      request: AcpSchema.RequestPermissionRequest;
      decision: ProviderApprovalDecision;
    }
  | { type: "text"; text: string }
  | { type: "waiting"; promptId: string; sessionId: string; cancelled: boolean }
  | { type: "cancel-requested" }
  | { type: "context"; mode: string; summary: string }
  | {
      type: "respawned";
      generation: number;
      sessionId?: string;
      redelivered: boolean;
      reason: string;
    }
  | { type: "host-lost" }
  | { type: "failed"; message: string };

export type DevinRunSnapshot = {
  runDir: string;
  runId: string;
  sessionId?: string | undefined;
  promptId?: string | undefined;
  generation?: number | undefined;
  phase?: string | undefined;
  stdoutIdleMs?: number | undefined;
};

type RecordValue = Record<string, unknown>;
function record(value: unknown): RecordValue {
  return typeof value === "object" && value !== null ? (value as RecordValue) : {};
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function pidAlive(pid: unknown): boolean {
  if (typeof pid !== "number" || !Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function killBestEffort(pid: unknown, signal: NodeJS.Signals = "SIGTERM") {
  if (typeof pid !== "number" || !Number.isSafeInteger(pid) || pid <= 0) return;
  try {
    process.kill(pid, signal);
  } catch {
    /* Already exited. */
  }
  try {
    process.kill(-pid, signal);
  } catch {
    /* No group or already exited. */
  }
}

/** Thin IPC client for one native run. The detached host owns the child, the
    hook socket, and the run dir; this class never holds the child. Visible
    turns finish when the Stop hook connects. */
export class DevinOpenTurn {
  private socket: NodeNet.Socket | undefined;
  private buffer = "";
  private sequence = 0;
  private commands = new Map<
    number,
    { resolve: (value: RecordValue) => void; reject: (error: Error) => void }
  >();
  private byeWaiters = new Set<() => void>();
  private phase: "new" | "live" | "ended" = "new";
  /** Detach, close, and clean shutdown suppress the host-lost event. */
  private deliberateEnd = false;
  private snapshotState: {
    sessionId?: string;
    promptId?: string;
    generation?: number;
    phase?: string;
    stdoutIdleMs?: number;
  } = {};
  private permissions = new Map<
    string,
    {
      key: string;
      request: AcpSchema.RequestPermissionRequest;
      choices: Map<ProviderApprovalDecision, AcpSchema.PermissionOption>;
    }
  >();
  readonly runId: string;
  private readonly runDir: string;
  private readonly hostSock: string;
  private hookToken = NodeCrypto.randomBytes(32).toString("hex");
  private options: {
    cwd: string;
    runRoot: string;
    threadId: string;
    providerInstanceId: string;
    host?: { platform: NodeJS.Platform; arch: NodeJS.Architecture };
    binary: string;
    model: string;
    environment?: NodeJS.ProcessEnv;
    compactionThresholdTokens?: number | undefined;
    runtimeMode?: RuntimeMode;
    onEvent: (event: DevinEvent) => void;
    /** Test peers use an executable plus arguments; production uses the pinned binary. */
    args?: string[];
    /** Attach to an existing run instead of spawning a host. */
    attachTo?: { runDir: string; runId: string };
  };
  constructor(options: DevinOpenTurn["options"]) {
    this.options = options;
    if (options.attachTo) {
      this.runId = options.attachTo.runId;
      this.runDir = options.attachTo.runDir;
    } else {
      this.runId = NodeCrypto.randomUUID();
      this.runDir = NodePath.join(options.runRoot, this.runId);
    }
    this.hostSock = devinSocketPaths(this.runId).hostSock;
  }

  snapshot(): DevinRunSnapshot {
    return {
      runDir: this.runDir,
      runId: this.runId,
      ...(this.snapshotState.sessionId !== undefined
        ? { sessionId: this.snapshotState.sessionId }
        : {}),
      ...(this.snapshotState.promptId !== undefined
        ? { promptId: this.snapshotState.promptId }
        : {}),
      ...(this.snapshotState.generation !== undefined
        ? { generation: this.snapshotState.generation }
        : {}),
      ...(this.snapshotState.phase !== undefined ? { phase: this.snapshotState.phase } : {}),
      ...(this.snapshotState.stdoutIdleMs !== undefined
        ? { stdoutIdleMs: this.snapshotState.stdoutIdleMs }
        : {}),
    };
  }

  private fail(message: string) {
    if (this.phase === "ended") return;
    this.phase = "ended";
    this.clearPermissions();
    for (const request of this.commands.values()) request.reject(new Error(message));
    this.commands.clear();
    this.options.onEvent({ type: "failed", message });
  }

  private hostLost() {
    if (this.phase === "ended" || this.deliberateEnd) return;
    this.phase = "ended";
    for (const request of this.commands.values())
      request.reject(new Error("The native host is gone."));
    this.commands.clear();
    this.clearPermissions();
    // Best effort: a dead host cannot reap its child group. Only touch pids
    // the dead run named; never touch a live (e.g. wire-mismatched) host.
    void (async () => {
      try {
        const status = record(
          JSON.parse(await NodeFSP.readFile(NodePath.join(this.runDir, "status.json"), "utf8")),
        );
        if (!pidAlive(status.hostPid)) killBestEffort(status.childPid, "SIGKILL");
      } catch {
        /* Evidence already gone. */
      }
    })();
    this.options.onEvent({ type: "host-lost" });
  }

  private onLine(line: string) {
    let message: RecordValue;
    try {
      message = record(JSON.parse(line));
    } catch {
      this.socket?.destroy();
      return;
    }
    if (
      (message.type === "ack" || message.type === "error" || message.type === "init") &&
      typeof message.id === "number" &&
      this.commands.has(message.id)
    ) {
      const request = this.commands.get(message.id)!;
      this.commands.delete(message.id);
      if (message.type === "error")
        request.reject(new Error(String(message.message ?? "Host error.")));
      else request.resolve(message);
      return;
    }
    switch (message.type) {
      case "text":
        this.options.onEvent({ type: "text", text: String(message.text ?? "") });
        break;
      case "waiting": {
        const promptId = String(message.promptId ?? "");
        const sessionId = String(message.sessionId ?? "");
        if (promptId) this.snapshotState.promptId = promptId;
        if (sessionId) this.snapshotState.sessionId = sessionId;
        this.snapshotState.phase = "waiting";
        this.options.onEvent({
          type: "waiting",
          promptId,
          sessionId,
          cancelled: message.cancelled === true,
        });
        break;
      }
      case "cancel-requested":
        this.options.onEvent({ type: "cancel-requested" });
        break;
      case "permission": {
        const key = String(message.key ?? "");
        if (!key) break;
        let request: AcpSchema.RequestPermissionRequest;
        try {
          request = decodePermissionRequest(message.request);
        } catch {
          this.fail("Malformed native permission request.");
          return;
        }
        const autoOption = this.autoApprovedOption(request);
        if (autoOption) {
          // Never fail here: a stale key means the child already moved on
          // (respawn clears host-side requests; the fresh child re-asks),
          // and transport death is reported by the socket close handler.
          void this.command("respond-permission", {
            key,
            result: { outcome: { outcome: "selected", optionId: autoOption.optionId } },
          }).catch(() => {});
          return;
        }
        const requestId = NodeCrypto.randomUUID();
        const choices = new Map<ProviderApprovalDecision, AcpSchema.PermissionOption>();
        for (const option of request.options) {
          const decision =
            option.kind === "allow_once"
              ? "accept"
              : option.kind === "allow_always"
                ? "acceptAlways"
                : option.kind === "reject_once"
                  ? "decline"
                  : undefined;
          if (decision && option.optionId.trim() && !choices.has(decision))
            choices.set(decision, option);
        }
        this.permissions.set(requestId, { key, request, choices });
        this.options.onEvent({
          type: "permission",
          requestId,
          request,
          options: [
            ...Array.from(choices, ([decision, option]) => ({
              decision,
              label: option.name.trim() || decision,
            })),
            { decision: "cancel", label: "Cancel" },
          ],
        });
        break;
      }
      case "context":
        this.options.onEvent({
          type: "context",
          mode: String(message.mode ?? "automatic"),
          summary: String(message.summary ?? ""),
        });
        break;
      case "respawned": {
        if (typeof message.sessionId === "string") this.snapshotState.sessionId = message.sessionId;
        if (typeof message.generation === "number")
          this.snapshotState.generation = message.generation;
        this.snapshotState.phase = "bootstrapping";
        // The dead child's requests can never resolve; the fresh child re-asks.
        this.clearPermissions();
        this.options.onEvent({
          type: "respawned",
          generation: typeof message.generation === "number" ? message.generation : 0,
          ...(typeof message.sessionId === "string" ? { sessionId: message.sessionId } : {}),
          redelivered: message.redelivered === true,
          reason: String(message.reason ?? ""),
        });
        break;
      }
      case "failed":
        this.fail(String(message.message ?? "Native run failed."));
        break;
      case "bye":
        for (const waiter of this.byeWaiters) waiter();
        this.byeWaiters.clear();
        break;
      default:
        break;
    }
  }

  private command(type: string, extra: RecordValue = {}): Promise<RecordValue> {
    if (!this.socket || this.socket.destroyed || this.phase !== "live")
      return Promise.reject(new Error("The native run is no longer connected."));
    return this.request(type, extra, 10000, "The native host did not answer.");
  }

  /** One correlated request over whatever socket is currently attached. Shared
      by hello and every command so the ack/timeout shape stays identical. */
  private request(
    type: string,
    extra: RecordValue,
    timeoutMs: number,
    timeoutMessage: string,
  ): Promise<RecordValue> {
    const socket = this.socket;
    if (!socket || socket.destroyed)
      return Promise.reject(new Error("The native run is no longer connected."));
    const id = ++this.sequence;
    return new Promise<RecordValue>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.commands.delete(id);
        reject(new Error(timeoutMessage));
      }, timeoutMs);
      timer.unref?.();
      this.commands.set(id, {
        resolve: (value) => {
          clearTimeout(timer);
          resolve(value);
        },
        reject: (error) => {
          clearTimeout(timer);
          reject(error);
        },
      });
      try {
        socket.write(JSON.stringify({ type, id, ...extra }) + "\n");
      } catch (error) {
        this.commands.delete(id);
        clearTimeout(timer);
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  private async connectHello(timeoutMs: number): Promise<RecordValue> {
    const deadline = Date.now() + timeoutMs;
    let lastError: unknown = new Error("unreachable");
    while (Date.now() < deadline) {
      try {
        const socket = await new Promise<NodeNet.Socket>((resolve, reject) => {
          const attempt = NodeNet.connect(this.hostSock);
          attempt.once("connect", () => resolve(attempt));
          attempt.once("error", reject);
        });
        this.socket = socket;
        socket.on("data", (chunk: Buffer) => {
          this.buffer += chunk.toString("utf8");
          const lines = this.buffer.split("\n");
          this.buffer = lines.pop() ?? "";
          for (const line of lines) {
            if (line.trim()) this.onLine(line);
          }
        });
        socket.on("error", () => socket.destroy());
        socket.on("close", () => {
          if (this.socket === socket) this.socket = undefined;
          this.hostLost();
        });
        return await this.request(
          "hello",
          { wire: HOST_WIRE_VERSION, pid: process.pid, runId: this.runId },
          10000,
          "The native host did not answer hello.",
        );
      } catch (error) {
        lastError = error;
        this.socket?.destroy();
        this.socket = undefined;
        await sleep(150);
      }
    }
    throw lastError instanceof Error ? lastError : new Error(String(lastError));
  }

  /** Command-path liveness probe. The hello handshake already proves the
      socket is up; this proves the dispatch path a submit will use. */
  async ping(): Promise<void> {
    await this.command("ping");
  }

  async start() {
    if (
      this.options.compactionThresholdTokens !== undefined &&
      (!Number.isSafeInteger(this.options.compactionThresholdTokens) ||
        this.options.compactionThresholdTokens <= 0)
    )
      throw new Error("Compaction threshold must be a positive whole number.");
    if (this.phase !== "new") throw new Error("This native run cannot be restarted.");
    // Pin the exact verified bytes: a respawn after an in-place binary swap
    // must fail loudly instead of silently mixing versions. Test peers skip
    // verification and pin nothing.
    const binarySha256 = this.options.args
      ? undefined
      : await verifyDevinBinary(
          this.options.binary,
          this.options.host ?? {
            platform: HostProcessPlatform.defaultValue(),
            arch: HostProcessArchitecture.defaultValue(),
          },
        );
    // Never share control state or infer run identity from the project directory.
    if (!NodePath.isAbsolute(this.options.runRoot))
      throw new Error("Devin run root must be absolute.");
    await NodeFSP.mkdir(this.options.runRoot, { recursive: true, mode: 0o700 });
    await NodeFSP.mkdir(this.runDir, { mode: 0o700 });
    const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";
    const { hookSock, hostSock } = devinSocketPaths(this.runId);
    const command = [
      process.execPath,
      NodePath.join(this.runDir, "hook.mjs"),
      hookSock,
      this.hookToken,
    ]
      .map(quote)
      .join(" ");
    await NodeFSP.writeFile(NodePath.join(this.runDir, "hook.mjs"), hookSource, { mode: 0o600 });
    const configPath = NodePath.join(this.runDir, "config.json");
    await NodeFSP.writeFile(
      configPath,
      JSON.stringify(
        await buildDevinConfig({
          cwd: this.options.cwd,
          environment: this.options.environment ?? process.env,
          hookCommand: command,
          model: this.options.model,
          compactionThresholdTokens: this.options.compactionThresholdTokens,
        }),
      ),
      { mode: 0o600 },
    );
    await NodeFSP.writeFile(NodePath.join(this.runDir, "host.mjs"), hostSource, { mode: 0o600 });
    await NodeFSP.writeFile(
      NodePath.join(this.runDir, "sockets.json"),
      JSON.stringify({ hookSock, hostSock }),
      {
        mode: 0o600,
      },
    );
    await NodeFSP.writeFile(
      NodePath.join(this.runDir, "launch.json"),
      JSON.stringify({
        wire: HOST_WIRE_VERSION,
        runId: this.runId,
        threadId: this.options.threadId,
        providerInstanceId: this.options.providerInstanceId,
        cwd: this.options.cwd,
        binary: this.options.binary,
        ...(this.options.args ? { args: this.options.args } : {}),
        ...(binarySha256 ? { binarySha256 } : {}),
        model: this.options.model,
        configPath,
        hookToken: this.hookToken,
        hookSock,
        hostSock,
        env: this.options.environment ?? process.env,
        bootstrap: DEVIN_BOOTSTRAP_PROMPT,
      }),
      { mode: 0o600 },
    );
    // Detached: server shutdown or crash never signals this group. stdio is
    // ignored so the host never holds the server's pipes open.
    const spawned = NodeChildProcess.spawn(
      process.execPath,
      [NodePath.join(this.runDir, "host.mjs"), this.runDir],
      { detached: true, stdio: "ignore" },
    );
    spawned.unref();
    try {
      const init = await this.connectHello(15000);
      this.applyInit(record(init.status));
    } catch (cause) {
      // A host we just spawned that never answers is broken, not stale: reap
      // it (its SIGTERM handler stops its child group) instead of orphaning.
      if (spawned.pid) killBestEffort(spawned.pid, "SIGTERM");
      this.socket?.destroy();
      this.socket = undefined;
      let evidence = "";
      try {
        const log = await NodeFSP.readFile(NodePath.join(this.runDir, "host.log"), "utf8");
        if (log.trim()) evidence = ` Host log: ${log.slice(-500)}`;
      } catch {
        /* No log; the cause below is the evidence. */
      }
      throw new Error(
        `Native host did not come up: ${cause instanceof Error ? cause.message : String(cause)}${evidence}`,
        { cause },
      );
    }
    this.phase = "live";
  }

  private applyInit(status: RecordValue) {
    if (typeof status.sessionId === "string") this.snapshotState.sessionId = status.sessionId;
    if (typeof status.promptId === "string") this.snapshotState.promptId = status.promptId;
    if (typeof status.generation === "number") this.snapshotState.generation = status.generation;
    if (typeof status.phase === "string") this.snapshotState.phase = status.phase;
    if (typeof status.stdoutIdleMs === "number")
      this.snapshotState.stdoutIdleMs = status.stdoutIdleMs;
  }

  /** Attach to a run a previous server detached from. Never spawns, never
      prompts, never kills: failures report and leave the run untouched. */
  static async attach(
    options: DevinOpenTurn["options"] & { attachTo: { runDir: string; runId: string } },
  ) {
    const runDir = NodePath.resolve(options.attachTo.runDir);
    const runRoot = NodePath.resolve(options.runRoot);
    if (!runDir.startsWith(runRoot + NodePath.sep))
      throw new Error("Cannot reattach: the run directory is outside the Devin run root.");
    if (NodePath.basename(runDir) !== options.attachTo.runId)
      throw new Error("Cannot reattach: the run directory does not match its run id.");
    const readJson = async (name: string) =>
      record(JSON.parse(await NodeFSP.readFile(NodePath.join(runDir, name), "utf8")));
    let shutdown = false;
    try {
      await NodeFSP.access(NodePath.join(runDir, "shutdown.request"));
      shutdown = true;
    } catch {
      /* No marker. */
    }
    if (shutdown)
      throw new Error(
        "Cannot reattach: this native run was shut down deliberately. Start a new thread for a fresh run.",
      );
    let status: RecordValue;
    try {
      status = await readJson("status.json");
    } catch {
      throw new Error(
        "Cannot reattach: the native run has no status. No replacement prompt was sent.",
      );
    }
    if (status.runId !== options.attachTo.runId)
      throw new Error(
        "Cannot reattach: run status does not match this run. No replacement prompt was sent.",
      );
    if (status.wire !== HOST_WIRE_VERSION)
      throw new Error(
        `Cannot reattach: host wire ${String(status.wire)} does not match driver wire ${HOST_WIRE_VERSION}. ` +
          "The run was left untouched; finish or stop the thread from a matching T3.",
      );
    if (status.status === "ended" || status.status === "failed") {
      const lastExit = record(status.lastExit);
      const detail =
        (typeof lastExit.reason === "string" && lastExit.reason) ||
        (typeof lastExit.stderrTail === "string" && lastExit.stderrTail.slice(-300)) ||
        `code=${String(lastExit.code)} signal=${String(lastExit.signal)}`;
      throw new Error(
        `Cannot reattach: the native run ${status.status === "ended" ? "ended" : "failed"}${detail ? ` (${detail})` : ""}. No replacement prompt was sent.`,
      );
    }
    let claim: RecordValue;
    try {
      claim = await readJson("host.claim");
    } catch {
      throw new Error(
        "Cannot reattach: the native run was never claimed by a host. No replacement prompt was sent.",
      );
    }
    if (claim.runId !== options.attachTo.runId || claim.wire !== HOST_WIRE_VERSION)
      throw new Error(
        "Cannot reattach: host claim does not match this run. No replacement prompt was sent.",
      );
    if (!pidAlive(claim.hostPid))
      throw new Error(
        "Cannot reattach: the native host process is gone. No replacement prompt was sent.",
      );
    const runtime = new DevinOpenTurn(options);
    try {
      const init = await runtime.connectHello(10000);
      const live = record(init.status);
      if (live.runId !== options.attachTo.runId)
        throw new Error("Host answered for a different run.");
      runtime.applyInit(live);
    } catch (cause) {
      runtime.socket?.destroy();
      runtime.socket = undefined;
      throw new Error(
        `Cannot reattach to the native run: ${cause instanceof Error ? cause.message : String(cause)} The run was left untouched; no replacement prompt was sent.`,
        { cause },
      );
    }
    runtime.phase = "live";
    return runtime;
  }

  async submit(text: string) {
    if (!text.trim()) throw new Error("Message is empty.");
    if (this.phase !== "live") throw new Error("Devin is busy or its native turn ended.");
    await this.command("submit", { text });
  }

  async context(mode: "compact") {
    if (this.phase !== "live") throw new Error("Context actions require the waiting Stop hook.");
    await this.command("context", { mode });
  }

  private autoApprovedOption(request: AcpSchema.RequestPermissionRequest) {
    const mode = this.options.runtimeMode;
    if (mode !== "full-access" && mode !== "auto-accept-edits") return undefined;
    if (mode === "auto-accept-edits") {
      const kind = request.toolCall.kind;
      if (kind !== "edit" && kind !== "delete" && kind !== "move") return undefined;
    }
    return (
      request.options.find((option) => option.kind === "allow_always" && option.optionId.trim()) ??
      request.options.find((option) => option.kind === "allow_once" && option.optionId.trim())
    );
  }

  /** Sync the permission mode without touching the native run. The mode is
      read per permission request, so switches apply to later requests live. */
  setRuntimeMode(mode: RuntimeMode) {
    this.options.runtimeMode = mode;
  }

  async respondToPermission(requestId: string, decision: ProviderApprovalDecision) {
    const pending = this.permissions.get(requestId);
    if (!pending) throw new Error("This approval request is no longer pending.");
    const option = pending.choices.get(decision);
    if (decision !== "cancel" && !option)
      throw new Error("Devin did not offer this permission choice.");
    this.permissions.delete(requestId);
    try {
      await this.command("respond-permission", {
        key: pending.key,
        result: {
          outcome: option
            ? { outcome: "selected", optionId: option.optionId }
            : { outcome: "cancelled" },
        },
      });
    } catch (error) {
      this.options.onEvent({
        type: "permission-resolved",
        requestId,
        request: pending.request,
        decision: "cancel",
      });
      // A rejected response never kills the run: the request is already gone
      // (stale key after a respawn or cancel) or the transport is dead, which
      // the socket close handler reports as host-lost on its own.
      const message = error instanceof Error ? error.message : String(error);
      if (
        message === "The native run is no longer connected." ||
        message === "The native host is gone."
      )
        this.hostLost();
      throw error;
    }
    this.options.onEvent({
      type: "permission-resolved",
      requestId,
      request: pending.request,
      decision,
    });
  }

  private clearPermissions() {
    for (const [requestId, pending] of this.permissions) {
      this.options.onEvent({
        type: "permission-resolved",
        requestId,
        request: pending.request,
        decision: "cancel",
      });
    }
    this.permissions.clear();
  }

  softCancel() {
    if (this.phase !== "live") return;
    // Resolve the UI side first: every open approval is dead whatever the
    // host does. The host gates the native side: a cancel that lands while
    // idle answers ack without inventing a cancellation event.
    this.clearPermissions();
    void this.command("cancel").catch(() => {});
  }

  /** This process attached last: no other server has adopted the run since. */
  private async lastClaimantIsSelf(): Promise<boolean> {
    try {
      const claim = record(
        JSON.parse(await NodeFSP.readFile(NodePath.join(this.runDir, "host.claim"), "utf8")),
      );
      return claim.clientPid === process.pid;
    } catch {
      return false;
    }
  }

  /** Deliberate shutdown: stop the host and its child group, mark the run.
      Reconnects first when this instance detached or the socket dropped, but
      only while this process is still the run's last claimant: a run another
      server adopted is not ours to kill. */
  async close() {
    if (this.phase === "ended" && !this.socket && !(await this.lastClaimantIsSelf())) return;
    this.deliberateEnd = true;
    this.clearPermissions();
    if ((!this.socket || this.socket.destroyed) && !(await this.lastClaimantIsSelf())) {
      this.phase = "ended";
      this.socket?.destroy();
      this.socket = undefined;
      return;
    }
    if (!this.socket || this.socket.destroyed) {
      try {
        await this.connectHello(5000);
        // Reconnected only to shut down; allow the shutdown command through.
        this.phase = "live";
      } catch {
        /* Fall through to the file fallback below. */
      }
    }
    try {
      if (!this.socket || this.socket.destroyed) throw new Error("no connection");
      await this.command("shutdown");
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("no bye")), 10000);
        timer.unref?.();
        this.byeWaiters.add(() => {
          clearTimeout(timer);
          resolve();
        });
      });
    } catch {
      // Host unreachable or refusing: the marker below still records intent,
      // and the group kill reaps what the host cannot.
      try {
        const status = record(
          JSON.parse(NodeFS.readFileSync(NodePath.join(this.runDir, "status.json"), "utf8")),
        );
        killBestEffort(status.childPid, "SIGTERM");
        killBestEffort(status.hostPid, "SIGTERM");
        await sleep(2000);
        killBestEffort(status.childPid, "SIGKILL");
        killBestEffort(status.hostPid, "SIGKILL");
      } catch {
        /* Evidence already gone. */
      }
      try {
        await NodeFSP.writeFile(
          NodePath.join(this.runDir, "shutdown.request"),
          JSON.stringify({ at: new Date().toISOString(), by: "driver" }),
          { mode: 0o600 },
        );
      } catch {
        /* Run dir already gone. */
      }
    } finally {
      this.phase = "ended";
      this.socket?.destroy();
      this.socket = undefined;
    }
  }

  /** Drop the IPC connection and leave the host and child alive. The next
      server re-attaches by run directory; nothing here may kill or prompt. */
  detach() {
    this.deliberateEnd = true;
    this.phase = "ended";
    for (const request of this.commands.values())
      request.reject(new Error("Detached from the native run."));
    this.commands.clear();
    this.permissions.clear();
    this.socket?.destroy();
    this.socket = undefined;
  }
}
