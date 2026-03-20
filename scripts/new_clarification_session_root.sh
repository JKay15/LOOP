#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

task_slug=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --task-type)
      if [[ $# -lt 2 ]]; then
        echo "new_clarification_session_root.sh: --task-type requires a value" >&2
        exit 2
      fi
      shift 2
      ;;
    --task-slug)
      if [[ $# -lt 2 ]]; then
        echo "new_clarification_session_root.sh: --task-slug requires a value" >&2
        exit 2
      fi
      task_slug="$2"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      if [[ -z "${task_slug}" ]]; then
        task_slug="$1"
        shift
      else
        echo "new_clarification_session_root.sh: unknown argument: $1" >&2
        exit 2
      fi
      ;;
  esac
done

if [[ -z "${task_slug}" ]]; then
  echo "new_clarification_session_root.sh: missing required --task-slug" >&2
  exit 2
fi

sanitized_slug="$(
  printf '%s' "${task_slug}" \
    | tr '[:upper:]' '[:lower:]' \
    | tr -cs '[:alnum:]' '_' \
    | sed -e 's/^_\\+//' -e 's/_\\+$//'
)"
if [[ -z "${sanitized_slug}" ]]; then
  sanitized_slug="clarification"
fi

base_dir="${repo_root}/.cache/endpoint_clarification"
mkdir -p "${base_dir}"
session_root="$(mktemp -d "${base_dir}/${sanitized_slug}_XXXXXX")"

case "${session_root}" in
  "${repo_root}"/*)
    printf '%s\n' "${session_root#${repo_root}/}"
    ;;
  *)
    printf '%s\n' "${session_root}"
    ;;
esac
