#!/usr/bin/env bash
# Read-only Bash gate for the verification-reviewer subagent — the Claude-side
# counterpart of `sandbox_mode = "read-only"` in .codex/agents/verification-reviewer.toml.
# Wired as a PreToolUse hook in .claude/agents/verification-reviewer.md; exit 2 blocks
# the command. Allowlist-first and fail-closed: this guards against a permissive
# session (acceptEdits/bypassPermissions) letting the reviewer mutate the tree it is
# reviewing, not against a determined adversary.
#
# Threat model (ratified on PR #118): accident-protection parity on a trusted
# checkout. Vectors that require hostile local configuration — e.g. git
# diff-driver/textconv helpers reachable via --ext-diff/--textconv, or a
# RIPGREP_CONFIG_PATH file injecting rg options — are accepted risk here; the
# gate additionally requires `rg --no-config` to neutralize the latter in the
# common case. Compilation-executing cargo commands (check/clippy/build/test)
# are excluded outright: build.rs runs arbitrary code at compile time.
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
  # git --no-optional-locks prefix: strip it for subcommand matching. It is
  # required for every git inspection command — porcelain like status/diff
  # otherwise refreshes and rewrites the .git/index stat cache and contends
  # on .git/index.lock.
  NOLOCK=0
  case "$SEG" in git\ --no-optional-locks\ *) NOLOCK=1; SEG="git ${SEG#git --no-optional-locks }" ;; esac
  case "$SEG" in
    git\ status|git\ status\ *|git\ diff|git\ diff\ *|git\ log|git\ log\ *|git\ show|git\ show\ *|git\ rev-parse\ *|git\ ls-files|git\ ls-files\ *|git\ blame\ *|git\ shortlog|git\ shortlog\ *|git\ describe|git\ describe\ *)
      [ "$NOLOCK" = 1 ] || block "git inspection may write .git/index; use git --no-optional-locks ${SEG#git }" ;;
    git\ grep\ *)
      [ "$NOLOCK" = 1 ] || block "git inspection may write .git/index; use git --no-optional-locks ${SEG#git }"
      case "$SEG" in *--open-files-in-pager*|*\ -O*) block "$SEG" ;; esac ;;
    # git branch: exact listing forms only — `git branch [<options>] <name>` is
    # the branch-creation form, so anything with free arguments is blocked.
    git\ branch|git\ branch\ -a|git\ branch\ -r|git\ branch\ -v|git\ branch\ -vv|git\ branch\ -av|git\ branch\ -avv|git\ branch\ --all|git\ branch\ --verbose|git\ branch\ --list|git\ branch\ --show-current)
      [ "$NOLOCK" = 1 ] || block "git inspection may write .git/index; use git --no-optional-locks ${SEG#git }" ;;
    # cargo: resolution/formatting only, with the lockfile asserted. Commands
    # that compile (check/clippy/build/test/run/doc/bench) execute build.rs —
    # arbitrary code — and are not inspection; they are blocked entirely.
    cargo\ metadata*|cargo\ tree*)
      case "$SEG" in *--locked*|*--frozen*) ;; *) block "cargo without --locked/--frozen (may rewrite Cargo.lock): $SEG" ;; esac ;;
    cargo\ fmt\ --check*) ;;
    cargo\ *)
      block "cargo compilation commands execute build.rs; out of scope for the reviewer shell: $SEG" ;;
    # rg: --pre runs an arbitrary preprocessor per searched file, and a
    # RIPGREP_CONFIG_PATH config can inject it invisibly — require --no-config.
    rg\ *)
      case "$SEG" in *--pre*) block "$SEG" ;; esac
      case "$SEG" in *--no-config*) ;; *) block "rg without --no-config (RIPGREP_CONFIG_PATH may inject --pre): $SEG" ;; esac ;;
    # sort/tree: block -o output bundles (long --output is blocked globally)
    # and sort's --compress-program, which executes a helper for temp files.
    sort|sort\ *|tree|tree\ *)
      case "$SEG" in *--compress-program*) block "$SEG" ;; esac
      for TOK in ${SEG#* }; do case "$TOK" in --*) ;; -*o*) block "$SEG" ;; esac; done ;;
    # uniq: second positional argument is an output file; after `--` every
    # token is positional, even dash-prefixed ones.
    uniq|uniq\ *)
      NPOS=0; DASHDASH=0
      for TOK in ${SEG#uniq}; do
        if [ "$DASHDASH" = 1 ]; then NPOS=$((NPOS+1)); continue; fi
        case "$TOK" in --) DASHDASH=1 ;; -*) ;; *) NPOS=$((NPOS+1)) ;; esac
      done
      [ "$NPOS" -le 1 ] || block "$SEG" ;;
    find\ *)
      case "$SEG" in *-delete*|*-exec*|*-ok*|*-fprint*|*-fls*) block "$SEG" ;; esac ;;
    cd\ *|pwd|ls|ls\ *|cat\ *|head|head\ *|tail|tail\ *|wc|wc\ *|grep\ *|stat\ *|file\ *|du|du\ *|cut\ *|column*|nl|nl\ *|jq\ *|diff\ *|echo\ *|printf\ *|which\ *|sha256sum\ *) ;;
    *) block "$SEG" ;;
  esac
done < <(printf '%s\n' "$SCRUBBED" | awk '{ gsub(/\|\|/, "\n"); gsub(/&&/, "\n"); gsub(/;/, "\n"); gsub(/\|/, "\n"); gsub(/&/, "\n"); print }')

exit 0
