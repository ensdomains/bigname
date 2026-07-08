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

# No substitution constructs anywhere: they nest commands the per-segment
# allowlist below cannot see.
case "$CMD" in
  *'$('*|*'`'*|*'<('*|*'>('*) block "command/process substitution" ;;
esac

# No file-writing redirects or write-capable flags anywhere in the command.
SCRUBBED="${CMD//2>&1/}"
SCRUBBED="${SCRUBBED//2>\/dev\/null/}"
SCRUBBED="${SCRUBBED//>\/dev\/null/}"
case "$SCRUBBED" in *">"*) block "output redirection" ;; esac
case "$CMD" in *--output*|*--fix*) block "write-capable flag" ;; esac

# Every stage of a pipeline / &&, ||, ;, & chain must start with an allowlisted
# inspection command. awk string escapes are portable (GNU and BSD alike), so
# the operator-to-newline splitting behaves the same on Linux and macOS; the
# stderr-merge tokens were already scrubbed above, so a remaining single "&"
# is a chain operator.
while IFS= read -r SEG; do
  SEG="${SEG#"${SEG%%[![:space:]]*}"}"
  SEG="${SEG%"${SEG##*[![:space:]]}"}"
  [ -z "$SEG" ] && continue
  case "$SEG" in
    git\ status|git\ status\ *|git\ diff|git\ diff\ *|git\ log|git\ log\ *|git\ show|git\ show\ *|git\ rev-parse\ *|git\ ls-files|git\ ls-files\ *|git\ blame\ *|git\ shortlog|git\ shortlog\ *|git\ describe|git\ describe\ *) ;;
    git\ grep\ *)
      case "$SEG" in *--open-files-in-pager*|*\ -O*) block "$SEG" ;; esac ;;
    # git branch: exact listing forms only — `git branch [<options>] <name>` is
    # the branch-creation form, so anything with free arguments is blocked.
    git\ branch|git\ branch\ -a|git\ branch\ -r|git\ branch\ -v|git\ branch\ -vv|git\ branch\ -av|git\ branch\ -avv|git\ branch\ --all|git\ branch\ --verbose|git\ branch\ --list|git\ branch\ --show-current) ;;
    # cargo: only lockfile-asserting invocations — plain check/clippy/metadata
    # can rewrite Cargo.lock when manifests changed in the diff under review.
    cargo\ check*|cargo\ clippy*|cargo\ metadata*|cargo\ tree*)
      case "$SEG" in *--locked*|*--frozen*) ;; *) block "cargo without --locked/--frozen (may rewrite Cargo.lock): $SEG" ;; esac ;;
    cargo\ fmt\ --check*) ;;
    # sort/tree: block -o output bundles (long --output is blocked globally).
    sort|sort\ *|tree|tree\ *)
      for TOK in ${SEG#* }; do case "$TOK" in --*) ;; -*o*) block "$SEG" ;; esac; done ;;
    # uniq: second positional argument is an output file.
    uniq|uniq\ *)
      NPOS=0
      for TOK in ${SEG#uniq}; do case "$TOK" in -*) ;; *) NPOS=$((NPOS+1)) ;; esac; done
      [ "$NPOS" -le 1 ] || block "$SEG" ;;
    find\ *)
      case "$SEG" in *-delete*|*-exec*|*-ok*|*-fprint*|*-fls*) block "$SEG" ;; esac ;;
    cd\ *|pwd|ls|ls\ *|cat\ *|head|head\ *|tail|tail\ *|wc|wc\ *|rg\ *|grep\ *|stat\ *|file\ *|du|du\ *|cut\ *|column*|nl|nl\ *|jq\ *|diff\ *|echo\ *|printf\ *|which\ *|sha256sum\ *) ;;
    *) block "$SEG" ;;
  esac
done < <(printf '%s\n' "$SCRUBBED" | awk '{ gsub(/\|\|/, "\n"); gsub(/&&/, "\n"); gsub(/;/, "\n"); gsub(/\|/, "\n"); gsub(/&/, "\n"); print }')

exit 0
