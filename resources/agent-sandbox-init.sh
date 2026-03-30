#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# agent-sandbox-init.sh
#
# Launches a bubblewrap (bwrap) sandbox for running AI coding agents
# (e.g. OpenAI Codex, Claude Code) on a project directory.
#
# The sandbox:
#   - Starts from an empty tmpfs root and mounts only what's needed
#   - Shares the host network (for API calls)
#   - Gives the agent read-write access to the project and a persistent home
#   - Keeps the host filesystem read-only everywhere else
#
# CONFIGURATION: adjust the paths in the section below to match your machine.
###############################################################################


# --- HOW TO SECURELY RUN AGENTS ----------------------------------------------
# Note: we will not mount the configuration of your host's coding agents. This
# avoids that your real config breaks if something happens inside the sandbox.
# Instead, you'll have to re-enter your auth token to use the agents inside
# the sandbox.
#
# CODEX
# Just install it normally on your host. Then, follow the comments of this
# script.
#
# CLAUDE CODE
# Same as codex.


# -- Paths to configure -------------------------------------------------------

# Your real home directory on the host
HOST_HOME="/home/s3tuit"

# The project/repo the agent will work on. It will appear as /work inside the
# sandbox with full read-write access.
WORKDIR="$HOST_HOME/devspace/c_prj/ai-db-explorer"

# Runtime directory for the adbxplorer MCP server.
# The /run subdirectory is mounted read-write (so the sandbox can talk to the
# broker); the /secret subdirectory is mounted read-only (it holds a token that
# must not be modified from inside the sandbox).
# To discover it, run 'adbxplorer -which-config'
ADBX_APPDIR="/run/user/1000/adbxplorer"

# Persistent sandbox home on the host. Survives across sandbox restarts so that
# agent config, caches and installed tools are not lost.
SANDBOX_HOME="${PWD}/agent-sandbox-home"

# Where npm-global binaries live on the host (codex, etc.).
# You may discover this running 'which codex'.
# Mount the full .node/ dir
NPM_GLOBAL="$HOST_HOME/.node"

# Extra host-side binaries to expose read-only inside the sandbox.
HOST_LOCAL_BIN="$HOST_HOME/.local/bin"

# -- End of configuration -----------------------------------------------------

USER_NAME="${USER:-}"
if [ -z "$USER_NAME" ]; then
  echo "ERROR: USER environment variable is not set." >&2
  exit 1
fi

UID_HOST="$(id -u)"
GID_HOST="$(id -g)"

mkdir -p "$SANDBOX_HOME"/{.codex,.cache,.config,.local/share,.local/state,.local/bin,.npm,.ssh,.node/bin}

args=(
  # -- Security: unshare everything, then selectively re-share ----------------
  --unshare-all
  --share-net
  --die-with-parent
  --uid "$UID_HOST"
  --gid "$GID_HOST"

  # -- Root filesystem --------------------------------------------------------
  --tmpfs /
  --dir /home
  --bind "$SANDBOX_HOME" "$HOME"

  # -- Kernel/device filesystems ----------------------------------------------
  --proc /proc
  --dev /dev
  --ro-bind /sys /sys
  --tmpfs /tmp

  # -- Host OS (read-only) ----------------------------------------------------
  --ro-bind /usr /usr
  --ro-bind /lib /lib
  --ro-bind /lib64 /lib64
  --ro-bind /etc /etc
  --ro-bind /bin /bin
  --ro-bind /sbin /sbin

  # -- DNS resolution ---------------------------------------------------------
  # Only expose systemd-resolved's stub, not the entire /run tree.
  # If your system doesn't use systemd-resolved, replace this with whatever
  # provides /etc/resolv.conf on your distro (or remove it entirely).
  --ro-bind /run/systemd/resolve /run/systemd/resolve

  # -- User binaries ----------------------------------------------------------
  --ro-bind "$HOST_LOCAL_BIN" "$HOME/.local/bin"
  --ro-bind "$NPM_GLOBAL" "$HOME/.node"

  # -- Project (read-write) ---------------------------------------------------
  --bind "$WORKDIR" /work

  # -- Claude Code ------------------------------------------------------------
  # Usually, 'claude' is a symlink on your host, so we need to mount the 
  # absolute path too. From the host, run 'ls -l $(which claude)'.
  # Then mount the .../share/claude/... dir.
  # Example:
  # if the absolute path is ~/.local/share/claude/versions/2.1.87, use this:
  --ro-bind "$HOST_HOME/.local/share/claude" "$HOME/.local/share/claude"

  # -- Codex ------------------------------------------------------------------
  # Codex should already be handled inside $NPM_GLOBAL. Both its symlink and
  # real .js file should be inside $HOST_HOME/.node/bin/ or equivalent.

  # -- adbxplorer MCP server --------------------------------------------------
  --bind    "$ADBX_APPDIR/run"    /apps/adbxplorer/run
  --ro-bind "$ADBX_APPDIR/secret" /apps/adbxplorer/secret
)

exec bwrap "${args[@]}" \
  --chdir /work \
  env -i \
    HOME="$HOME" \
    USER="$USER_NAME" \
    LOGNAME="$USER_NAME" \
    LANG="${LANG:-C.UTF-8}" \
    PATH="$HOME/.node/bin:/usr/sbin:/usr/bin:/sbin:/bin:$HOME/.local/bin" \
    TERM="${TERM:-xterm-256color}" \
    XDG_CACHE_HOME="$HOME/.cache" \
    XDG_CONFIG_HOME="$HOME/.config" \
    XDG_DATA_HOME="$HOME/.local/share" \
    XDG_STATE_HOME="$HOME/.local/state" \
    npm_config_cache="$HOME/.npm" \
    bash -i


# NOTE:
# If you think the sandbox looks ugly once inside it, just
# 'cd agent-sandbox-home' from your host and create a .bashrc where you'll set
# terminal color:
#
# # change terminal appearance
# PS1="\[\e[32m\]<agent@\h \w>\$\[\033[0m\] "
