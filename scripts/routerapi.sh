#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

command="${1:-}"
router_db=""
shifted=0
for arg in "$@"; do
  if [[ "${shifted}" -eq 1 ]]; then
    router_db="${arg}"
    shifted=0
    continue
  fi
  case "${arg}" in
    --router-db)
      shifted=1
      ;;
    --router-db=*)
      router_db="${arg#--router-db=}"
      ;;
  esac
done

if uv run --project "${repo_root}" --locked python -m loop.router_api "$@"; then
  status=0
else
  status=$?
fi

if [[ "${status}" -eq 0 && "${command}" == "resume" && -n "${router_db}" ]]; then
  leanatlas_root="${repo_root}/../leanatlas"
  supervise_script="${leanatlas_root}/tools/loop/router_supervise.py"
  if [[ -f "${supervise_script}" ]]; then
    nohup python3 "${supervise_script}" \
      --router-db "${router_db}" \
      --hold-open-on-terminal \
      >/dev/null 2>&1 &
  fi
fi

exit "${status}"
