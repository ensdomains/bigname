#!/usr/bin/env bash
# Read-only Bash gate for the verification-reviewer subagent — the Claude-side
# counterpart of `sandbox_mode = "read-only"` in .codex/agents/verification-reviewer.toml.
# Wired as a PreToolUse hook in .claude/agents/verification-reviewer.md; exit 2 blocks
# the command. Allowlist-first and fail-closed: this guards against a permissive
# session (acceptEdits/bypassPermissions) letting the reviewer mutate the tree it is
# reviewing, not against a determined adversary.
set -u

INPUT="$(cat)"
if command -v jq >/dev/null 2>&1; then
  CMD="$(printf '%s' "$INPUT" | jq -r '.tool_input.command // empty')"
elif command -v python3 >/dev/null 2>&1; then
  CMD="$(printf '%s' "$INPUT" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("tool_input",{}).get("command",""))')"
else
  echo "verification-reviewer read-only gate: jq or python3 required to validate; blocking" >&2
  exit 2
fi

[ -z "$CMD" ] && exit 0

block() {
  echo "verification-reviewer is read-only (codex parity: sandbox_mode=\"read-only\"): blocked non-inspection command: $1" >&2
  exit 2
}

# No file-writing redirects or write-capable flags anywhere in the command.
SCRUBBED="${CMD//2>&1/}"
SCRUBBED="${SCRUBBED//2>\/dev\/null/}"
SCRUBBED="${SCRUBBED//>\/dev\/null/}"
case "$SCRUBBED" in *">"*) block "output redirection" ;; esac
case "$CMD" in *--output*|*--fix*) block "write-capable flag" ;; esac

# Every stage of a pipeline / &&, ||, ; chain must start with an allowlisted
# inspection command.
while IFS= read -r SEG; do
  SEG="${SEG#"${SEG%%[![:space:]]*}"}"
  [ -z "$SEG" ] && continue
  case "$SEG" in
    git\ status*|git\ diff*|git\ log*|git\ show*|git\ rev-parse*|git\ ls-files*|git\ blame*|git\ grep*|git\ shortlog*|git\ describe*) ;;
    git\ branch|git\ branch\ --list*|git\ branch\ -a*|git\ branch\ -r*|git\ branch\ -v*) ;;
    cargo\ check*|cargo\ clippy*|cargo\ metadata*|cargo\ tree*|cargo\ fmt\ --check*) ;;
    cd\ *|pwd|ls|ls\ *|cat\ *|head|head\ *|tail|tail\ *|wc|wc\ *|rg\ *|grep\ *|tree|tree\ *|stat\ *|file\ *|du|du\ *|sort|sort\ *|uniq|uniq\ *|cut\ *|column*|nl|nl\ *|jq\ *|diff\ *|echo\ *|printf\ *|which\ *|sha256sum\ *) ;;
    find\ *) case "$SEG" in *-delete*|*-exec*|*-ok*|*-fprint*) block "$SEG" ;; esac ;;
    *) block "$SEG" ;;
  esac
done < <(printf '%s\n' "$CMD" | sed -E 's/\|\|/\n/g; s/&&/\n/g; s/;/\n/g; s/\|/\n/g')

exit 0
