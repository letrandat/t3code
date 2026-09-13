import { connect } from "node:net";
let input = "";
for await (const chunk of process.stdin) input += chunk;
const socket = connect(process.argv[2]);
socket.on("connect", () => socket.write(input.trim() + "\n"));
let response = "";
let blocked = false;
const keepBlocked = () => {
  if (!blocked) {
    blocked = true;
    setInterval(() => {}, 60_000);
  }
};
socket.on("data", (chunk) => {
  response += chunk;
});
// On bridge loss, keep the hook blocked instead of returning success to Devin.
socket.on("error", keepBlocked);
socket.on("end", () => {
  try {
    if (blocked || !response.endsWith("\n")) return keepBlocked();
    const parsed = JSON.parse(response);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return keepBlocked();
    process.stdout.write(response, () => process.exit(0));
  } catch {
    keepBlocked();
  }
});
