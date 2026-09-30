#!/usr/bin/env sh
set -e

REPO_URL="https://github.com/park-peter/airflow-to-dabs.git"
SKILL_NAME="airflow-to-dabs"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

die() { printf "Error: %s\n" "$1" >&2; exit 1; }
info() { printf "  %s\n" "$1"; }
warn() { printf "  [warn] %s\n" "$1"; }

usage() {
  printf "Usage: install.sh [--platform <claude|agents|all>] [--scope <global|project>] [--uninstall]\n\n"
  printf "Platforms:\n"
  printf "  claude   Claude Code             ~/.claude/skills/%s  or  .claude/skills/%s\n" "$SKILL_NAME" "$SKILL_NAME"
  printf "  agents   Codex, Cursor, VS Code  ~/.agents/skills/%s  or  .agents/skills/%s\n" "$SKILL_NAME" "$SKILL_NAME"
  printf "           Copilot, and other agents that read .agents/skills (aliases: codex, cursor, copilot)\n"
  printf "  all      Both of the above\n\n"
  printf "No flags = interactive mode.\n"
}

# Skill directory for a platform and scope.
skill_dir() {
  case "$1:$2" in
    claude:global)  printf "%s" "$HOME/.claude/skills/$SKILL_NAME" ;;
    claude:project) printf "%s" ".claude/skills/$SKILL_NAME" ;;
    agents:global)  printf "%s" "$HOME/.agents/skills/$SKILL_NAME" ;;
    agents:project) printf "%s" ".agents/skills/$SKILL_NAME" ;;
    *) die "Unsupported combination: $1 / $2" ;;
  esac
}

# Clone the skill into $1, or fast-forward an existing clone.
install_skill() {
  _target="$1"
  mkdir -p "$(dirname "$_target")"
  if [ -d "$_target/.git" ]; then
    info "Updating $_target ..."
    git -C "$_target" pull --ff-only --quiet
  elif [ -e "$_target" ]; then
    die "$_target exists and is not a git clone of $SKILL_NAME. Remove it and re-run."
  else
    info "Cloning into $_target ..."
    git clone --quiet "$REPO_URL" "$_target"
  fi
  info "Installed $_target"
}

# Remove $1. Only the four known skill directories are accepted.
uninstall_skill() {
  _target="$1"
  case "$_target" in
    "$HOME/.claude/skills/$SKILL_NAME" | ".claude/skills/$SKILL_NAME" | \
    "$HOME/.agents/skills/$SKILL_NAME" | ".agents/skills/$SKILL_NAME") ;;
    *) die "Refusing to remove unexpected path: $_target" ;;
  esac
  if [ -d "$_target" ]; then
    rm -rf -- "$_target"
    info "Removed $_target"
  else
    warn "$_target does not exist, nothing to remove."
  fi
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

PLATFORM=""
SCOPE=""
UNINSTALL=false

while [ $# -gt 0 ]; do
  case "$1" in
    --platform)
      [ $# -ge 2 ] || die "Option --platform requires a value: claude|agents|all."
      PLATFORM="$2"; shift 2 ;;
    --scope)
      [ $# -ge 2 ] || die "Option --scope requires a value: global|project."
      SCOPE="$2"; shift 2 ;;
    --uninstall) UNINSTALL=true; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "Unknown option: $1. Use --help for usage." ;;
  esac
done

# ---------------------------------------------------------------------------
# Interactive prompts (when flags are missing)
# ---------------------------------------------------------------------------

if [ -z "$PLATFORM" ]; then
  printf "\nairflow-to-dabs skill installer\n\n"
  [ "$UNINSTALL" = true ] && printf "(uninstall mode)\n\n"
  printf "Install for:\n"
  printf "  1) Claude Code\n"
  printf "  2) Codex, Cursor, VS Code Copilot, and other agents that read .agents/skills\n"
  printf "  3) Both\n"
  printf "Choice [1-3]: "
  read -r _choice
  case "$_choice" in
    1) PLATFORM="claude" ;;
    2) PLATFORM="agents" ;;
    3) PLATFORM="all" ;;
    *) die "Invalid choice: $_choice" ;;
  esac
fi

if [ -z "$SCOPE" ]; then
  printf "\nScope:\n"
  printf "  1) Global (all projects)\n"
  printf "  2) Project (current directory only)\n"
  printf "Choice [1-2]: "
  read -r _choice
  case "$_choice" in
    1) SCOPE="global" ;;
    2) SCOPE="project" ;;
    *) die "Invalid choice: $_choice" ;;
  esac
fi

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

command -v git >/dev/null 2>&1 || die "git is required but not installed."

case "$PLATFORM" in
  claude) PLATFORMS="claude" ;;
  agents|codex|cursor|copilot) PLATFORMS="agents" ;;
  all) PLATFORMS="claude agents" ;;
  *) die "Invalid platform: $PLATFORM. Must be claude, agents, or all." ;;
esac

case "$SCOPE" in
  global|project) ;;
  *) die "Invalid scope: $SCOPE. Must be global or project." ;;
esac

if [ "$SCOPE" = "project" ] && [ ! -d .git ]; then
  warn "Current directory is not a git repository root. Installing here anyway."
fi

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

for _p in $PLATFORMS; do
  _dir=$(skill_dir "$_p" "$SCOPE")
  if [ "$UNINSTALL" = true ]; then
    uninstall_skill "$_dir"
  else
    install_skill "$_dir"
  fi
done

printf "\nDone.\n"
