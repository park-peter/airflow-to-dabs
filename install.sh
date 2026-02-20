#!/usr/bin/env sh
set -e

REPO_URL="https://github.com/park-peter/airflow-to-dabs.git"
SKILL_NAME="airflow-to-dabs"
BEGIN_MARK="<!-- BEGIN ${SKILL_NAME} -->"
END_MARK="<!-- END ${SKILL_NAME} -->"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

die() { printf "Error: %s\n" "$1" >&2; exit 1; }
info() { printf "  %s\n" "$1"; }
warn() { printf "  [warn] %s\n" "$1"; }

check_deps() {
  command -v git >/dev/null 2>&1 || die "git is required but not installed."
  command -v awk >/dev/null 2>&1 || die "awk is required but not installed."
}

make_tempdir() {
  if command -v mktemp >/dev/null 2>&1; then
    mktemp -d 2>/dev/null && return
  fi
  _td="${TMPDIR:-/tmp}/${SKILL_NAME}.$$"
  mkdir -p "$_td" && printf "%s" "$_td"
}

# Clone or pull the skill repo into $1.
clone_or_pull() {
  _target="$1"
  _parent="$(dirname "$_target")"
  mkdir -p "$_parent"
  if [ -d "$_target/.git" ]; then
    info "Updating existing clone in $_target ..."
    git -C "$_target" pull --ff-only
  else
    info "Cloning into $_target ..."
    git clone "$REPO_URL" "$_target"
  fi
}

# Count occurrences of a fixed string in a file. Prints the count.
count_occurrences() {
  _file="$1"; _pattern="$2"
  _count=$(grep -cF "$_pattern" "$_file" 2>/dev/null) || _count=0
  printf "%s" "$_count"
}

# Validate marker state in a file. Returns 0 if safe to proceed.
# Sets MARKER_STATE to "none", "valid", or dies on malformed.
validate_markers() {
  _file="$1"
  if [ ! -f "$_file" ]; then
    MARKER_STATE="none"
    return 0
  fi
  _begin_count=$(count_occurrences "$_file" "$BEGIN_MARK")
  _end_count=$(count_occurrences "$_file" "$END_MARK")
  if [ "$_begin_count" -eq 0 ] && [ "$_end_count" -eq 0 ]; then
    MARKER_STATE="none"
  elif [ "$_begin_count" -eq 1 ] && [ "$_end_count" -eq 1 ]; then
    _begin_line=$(grep -nF "$BEGIN_MARK" "$_file" | head -n 1 | cut -d: -f1)
    _end_line=$(grep -nF "$END_MARK" "$_file" | head -n 1 | cut -d: -f1)
    case "$_begin_line:$_end_line" in
      *[!0-9:]* | :* | *:) die "Malformed marker block in $_file (unable to determine marker line numbers)." ;;
    esac
    if [ "$_begin_line" -ge "$_end_line" ]; then
      die "Malformed marker block in $_file (END appears before BEGIN). Please manually fix marker ordering and re-run."
    fi
    MARKER_STATE="valid"
  else
    die "Malformed marker block in $_file (found ${_begin_count}x BEGIN, ${_end_count}x END). Please manually remove stale <!-- BEGIN/END ${SKILL_NAME} --> lines and re-run."
  fi
}

# Strip the marker-delimited block from a file, write result to same file.
strip_marker_block() {
  _file="$1"
  awk -v begin="$BEGIN_MARK" -v end="$END_MARK" '
    $0 == begin { skip=1; next }
    $0 == end   { skip=0; next }
    !skip       { print }
  ' "$_file" > "${_file}.tmp"
  mv "${_file}.tmp" "$_file"
}

# Append the marker-delimited block from $2 (source file) into $1 (target file).
append_marker_block() {
  _target_file="$1"; _source_file="$2"
  {
    [ -s "$_target_file" ] && printf "\n"
    printf "%s\n" "$BEGIN_MARK"
    cat "$_source_file"
    printf "\n%s\n" "$END_MARK"
  } >> "$_target_file"
}

# Back up a file with a timestamped suffix. Prints the backup path.
backup_file() {
  _file="$1"
  [ -f "$_file" ] || return 0
  _bak="${_file}.bak.$(date +%Y%m%d%H%M%S)"
  cp "$_file" "$_bak"
  printf "%s" "$_bak"
}

# Safety-checked rm -rf. Only removes paths under an allowed prefix.
safe_rmrf() {
  _dir="$1"
  [ -n "$_dir" ] || die "Refusing to remove: empty path."
  [ "$_dir" != "/" ] || die "Refusing to remove /."
  case "$_dir" in
    "$HOME/.cursor/skills/$SKILL_NAME"  | \
    "$HOME/.claude/skills/$SKILL_NAME"  | \
    "$HOME/.codex/skills/$SKILL_NAME"   | \
    .cursor/skills/"$SKILL_NAME"        | \
    .claude/skills/"$SKILL_NAME"        | \
    .codex/skills/"$SKILL_NAME"         )
      rm -rf -- "$_dir"
      ;;
    *)
      die "Refusing to remove unexpected path: $_dir"
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Install / uninstall per platform
# ---------------------------------------------------------------------------

install_cursor_claude() {
  _target="$1"
  clone_or_pull "$_target"
  info "Done. Skill installed at $_target"
}

uninstall_cursor_claude() {
  _target="$1"
  if [ -d "$_target" ]; then
    safe_rmrf "$_target"
    info "Removed $_target"
  else
    warn "$_target does not exist, nothing to remove."
  fi
}

install_codex() {
  _skills_dir="$1"; _agents_file="$2"
  clone_or_pull "$_skills_dir"
  _source="${_skills_dir}/AGENTS.md"
  [ -f "$_source" ] || die "AGENTS.md not found in cloned repo at $_source"

  touch "$_agents_file"
  validate_markers "$_agents_file"

  _bak=$(backup_file "$_agents_file")
  [ -n "$_bak" ] && info "Backed up $_agents_file -> $_bak"

  if [ "$MARKER_STATE" = "valid" ]; then
    strip_marker_block "$_agents_file"
  fi
  append_marker_block "$_agents_file" "$_source"
  info "Done. Instructions appended to $_agents_file"
}

uninstall_codex() {
  _skills_dir="$1"; _agents_file="$2"
  if [ -d "$_skills_dir" ]; then
    safe_rmrf "$_skills_dir"
    info "Removed $_skills_dir"
  else
    warn "$_skills_dir does not exist, nothing to remove."
  fi
  if [ -f "$_agents_file" ]; then
    validate_markers "$_agents_file"
    if [ "$MARKER_STATE" = "valid" ]; then
      _bak=$(backup_file "$_agents_file")
      [ -n "$_bak" ] && info "Backed up $_agents_file -> $_bak"
      strip_marker_block "$_agents_file"
      info "Removed skill block from $_agents_file"
    elif [ "$MARKER_STATE" = "none" ]; then
      warn "No skill block found in $_agents_file, nothing to remove."
    fi
  fi
}

install_copilot() {
  _tmpdir=$(make_tempdir)
  info "Cloning into temp directory ..."
  if ! git clone "$REPO_URL" "$_tmpdir"; then
    die "Failed to clone $REPO_URL into $_tmpdir."
  fi
  _source="${_tmpdir}/copilot-instructions.md"
  [ -f "$_source" ] || die "copilot-instructions.md not found in cloned repo"

  mkdir -p .github
  _target=".github/copilot-instructions.md"
  touch "$_target"
  validate_markers "$_target"

  _bak=$(backup_file "$_target")
  [ -n "$_bak" ] && info "Backed up $_target -> $_bak"

  if [ "$MARKER_STATE" = "valid" ]; then
    strip_marker_block "$_target"
  fi
  append_marker_block "$_target" "$_source"
  rm -rf -- "$_tmpdir"
  info "Done. Instructions appended to $_target"
}

uninstall_copilot() {
  _target=".github/copilot-instructions.md"
  if [ ! -f "$_target" ]; then
    warn "$_target does not exist, nothing to remove."
    return
  fi
  validate_markers "$_target"
  if [ "$MARKER_STATE" = "valid" ]; then
    _bak=$(backup_file "$_target")
    [ -n "$_bak" ] && info "Backed up $_target -> $_bak"
    strip_marker_block "$_target"
    # Remove file if empty (only whitespace remains)
    if ! grep -q '[^[:space:]]' "$_target" 2>/dev/null; then
      rm -f "$_target"
      info "Removed empty $_target"
    else
      info "Removed skill block from $_target"
    fi
  elif [ "$MARKER_STATE" = "none" ]; then
    warn "No skill block found in $_target, nothing to remove."
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
      [ $# -ge 2 ] || die "Option --platform requires a value: cursor|claude|codex|copilot."
      case "$2" in --*) die "Option --platform requires a value: cursor|claude|codex|copilot." ;; esac
      PLATFORM="$2"
      shift 2
      ;;
    --scope)
      [ $# -ge 2 ] || die "Option --scope requires a value: global|project."
      case "$2" in --*) die "Option --scope requires a value: global|project." ;; esac
      SCOPE="$2"
      shift 2
      ;;
    --uninstall) UNINSTALL=true; shift ;;
    --help|-h)
      printf "Usage: install.sh [--platform <cursor|claude|codex|copilot>] [--scope <global|project>] [--uninstall]\n"
      printf "\nNo flags = interactive mode.\n"
      exit 0
      ;;
    *) die "Unknown option: $1. Use --help for usage." ;;
  esac
done

# ---------------------------------------------------------------------------
# Interactive prompts (when flags are missing)
# ---------------------------------------------------------------------------

if [ -z "$PLATFORM" ]; then
  printf "\nAirflow-to-DABs Skill Installer\n"
  printf "================================\n\n"
  if [ "$UNINSTALL" = true ]; then
    printf "(uninstall mode)\n\n"
  fi
  printf "Select platform:\n"
  printf "  1) Cursor\n"
  printf "  2) Claude Code\n"
  printf "  3) Codex CLI\n"
  printf "  4) VS Code + Copilot\n"
  printf "Choice [1-4]: "
  read -r _choice
  case "$_choice" in
    1) PLATFORM="cursor"  ;;
    2) PLATFORM="claude"  ;;
    3) PLATFORM="codex"   ;;
    4) PLATFORM="copilot" ;;
    *) die "Invalid choice: $_choice" ;;
  esac
fi

if [ "$PLATFORM" = "copilot" ]; then
  if [ "$SCOPE" = "global" ]; then
    die "VS Code + Copilot supports project scope only."
  fi
  SCOPE="project"
  if [ -z "$SCOPE" ] 2>/dev/null; then :; fi  # already set above
  info "VS Code + Copilot is project-scoped only. Using current directory."
elif [ -z "$SCOPE" ]; then
  printf "\nSelect scope:\n"
  printf "  1) Global (all projects)\n"
  printf "  2) Project (current directory only)\n"
  printf "Choice [1-2]: "
  read -r _choice
  case "$_choice" in
    1) SCOPE="global"  ;;
    2) SCOPE="project" ;;
    *) die "Invalid choice: $_choice" ;;
  esac
fi

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

check_deps

case "$PLATFORM" in
  cursor|claude|codex|copilot) ;;
  *) die "Invalid platform: $PLATFORM. Must be cursor, claude, codex, or copilot." ;;
esac

case "$SCOPE" in
  global|project) ;;
  *) die "Invalid scope: $SCOPE. Must be global or project." ;;
esac

if [ "$SCOPE" = "project" ] && [ ! -d .git ] && [ ! -d src ] && [ ! -f README.md ]; then
  warn "Current directory does not look like a project root. Proceeding anyway."
fi

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

ACTION="install"
[ "$UNINSTALL" = true ] && ACTION="uninstall"

printf "\n[%s] %s / %s\n\n" "$ACTION" "$PLATFORM" "$SCOPE"

case "${PLATFORM}:${SCOPE}:${ACTION}" in

  cursor:global:install)   install_cursor_claude "$HOME/.cursor/skills/$SKILL_NAME" ;;
  cursor:project:install)  install_cursor_claude ".cursor/skills/$SKILL_NAME" ;;
  cursor:global:uninstall)   uninstall_cursor_claude "$HOME/.cursor/skills/$SKILL_NAME" ;;
  cursor:project:uninstall)  uninstall_cursor_claude ".cursor/skills/$SKILL_NAME" ;;

  claude:global:install)   install_cursor_claude "$HOME/.claude/skills/$SKILL_NAME" ;;
  claude:project:install)  install_cursor_claude ".claude/skills/$SKILL_NAME" ;;
  claude:global:uninstall)   uninstall_cursor_claude "$HOME/.claude/skills/$SKILL_NAME" ;;
  claude:project:uninstall)  uninstall_cursor_claude ".claude/skills/$SKILL_NAME" ;;

  codex:global:install)    install_codex "$HOME/.codex/skills/$SKILL_NAME" "$HOME/.codex/AGENTS.md" ;;
  codex:project:install)   install_codex ".codex/skills/$SKILL_NAME" "./AGENTS.md" ;;
  codex:global:uninstall)    uninstall_codex "$HOME/.codex/skills/$SKILL_NAME" "$HOME/.codex/AGENTS.md" ;;
  codex:project:uninstall)   uninstall_codex ".codex/skills/$SKILL_NAME" "./AGENTS.md" ;;

  copilot:project:install)   install_copilot ;;
  copilot:project:uninstall) uninstall_copilot ;;

  *) die "Unsupported combination: ${PLATFORM} / ${SCOPE} / ${ACTION}" ;;
esac

printf "\nAll done.\n"
