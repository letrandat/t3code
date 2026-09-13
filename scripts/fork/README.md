# Install the personal T3 fork

This package is for Apple Silicon Macs. It includes the fork's server and web
app, Node, native dependencies, and the matching T3 ABI 1 patched Devin binary.
Devin must already be installed and signed in on the destination Mac. No login
credentials or source-machine settings are included.

Download `install-t3-fork.sh` from a specific fork release. Then run:

```sh
bash ~/Downloads/install-t3-fork.sh
~/.local/share/t3-fork/bin/t3-fork
```

The second command opens T3 in your browser. Choose a project and a model, then
send a message. Installing or opening T3 does not send a Devin prompt.
Devin's path is filled in automatically. Automatic compaction defaults to
240,000 tokens. Change it in Settings → Providers if needed.

The fork uses `~/.local/share/t3-fork/data`, separate from the normal T3 install.
It leaves stock Devin untouched. Set `T3_FORK_ROOT` when installing if you want
a different directory, then use the launcher printed by the installer.
Keep the terminal open while using T3. `--no-browser` prints a pairing URL
without opening a browser. Standard server flags such as `--port 13780` work.

## Updates

Run the installer for a newer release. It verifies the download and installs
into a new version directory. It never stops a server or overwrites an existing
version. Running processes continue using their original files. The next launch
uses the selected release; it refuses to start if this fork's server is still
running. Stop that server deliberately when ready to change versions. Restart
survival for an existing Devin session is not provided by this installer.

Existing provider settings are preserved, except the installer's `devin_fork`
provider path follows the selected release on startup. Other provider instances
are not changed. Keep custom Devin binaries in a separate provider instance.
Rerunning the same installer is safe. It verifies the existing release instead
of replacing it. There are no automatic background updates or login services.

## Build a release

On an Apple Silicon build Mac with this checkout's dependencies installed:

```sh
vp run --filter t3 build
vp run build:resource-monitor
node scripts/fork/build.mjs t3-fork-VERSION /absolute/path/devin-patched /absolute/output
```

Use a unique release tag beginning with `t3-fork-`. The Devin executable must
come from `patch_t3.py`, with its `.manifest.json` beside it. The pool's separate
`devin-continuous` executable is not the T3 ABI 1 build. Use an official standalone
Node 24 runtime on the build Mac; the packager includes its executable and license.
Building the resource monitor requires Rust 1.95 or newer. These build tools
are not needed on the destination Mac.

Test the generated archive outside the checkout: extract it, set `T3_FORK_ROOT`
to a temporary directory, and run its `runtime/bin/node install.mjs`. Launch
with `--no-browser` and check HTTP, provider discovery, and the served web files.
Do not send a paid test prompt unless that test is approved.

Publish the archive, `SHA256SUMS`, and generated `install-t3-fork.sh` together as
assets on the matching `letrandat/t3code` release tag. The generated installer
contains that version's exact URL and archive hash. Do not upload the template
installer or reuse a release version with different bytes. First-time setup
requires downloading the installer; a release can also show a `curl -fL ... -o
/tmp/install-t3-fork.sh && bash /tmp/install-t3-fork.sh` command for convenience.

The installer and build scripts are separate from T3's normal release workflows.
Publishing a fork archive does not publish to npm or change upstream T3 releases.
