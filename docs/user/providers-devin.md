# Devin

This first release supports a separately built private-control Devin binary on macOS ARM64. Build Devin 3000.10.21 with `patch_t3.py` from [devin-continuous](https://github.com/letrandat/devin-continuous), keeping its generated manifest next to the binary. In **Settings > Providers**, add Devin, select that binary path and model, and enable native prompts. The existing pool installation can stay in place.

Each T3 thread starts its own Devin run in the actual project directory. Replies reuse that run's open native prompt. Two threads can share a project, but they still edit the same files. Their control files, hooks and logs live in separate folders under the T3 environment's state directory.

Use **Stop** to request a soft cancel. Devin finishes a running tool and blocks the next tool call before returning to its waiting hook. Plain Escape retains its normal composer behavior. Send another message to continue the same native prompt. Send `/compact` while waiting to compact that prompt. `/clear`, attachments, model changes and permission-request UI are not supported in this version. Permission requests are denied, never silently approved.

T3 copies ordinary user settings into each run's configuration and replaces user hooks with its managed hooks. It leaves original settings and `AGENTS.md` files unchanged. Projects with Devin hooks in `.devin/config.json`, `.devin/config.local.json` or `.devin/hooks.v1.json`, including ancestor directories, are refused before a prompt starts. Project hook replacement is outside this first release. Do not enable additional plugin or organization hooks for this provider; those hook sources are not covered by the startup check. Separate MCP configuration remains subject to Devin's normal discovery.

T3 checks the binary manifest and checksum before launch. A failed or ended native run is not silently replaced. This version cannot recover its waiting connection after a T3 server restart or update. Finish active work first; [restart recovery is tracked separately](https://github.com/letrandat/t3code/issues/1).
