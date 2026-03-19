#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
shared_uv_cache="${repo_root}/.cache/uv"

cd "${repo_root}"
export UV_CACHE_DIR="${UV_CACHE_DIR:-${shared_uv_cache}}"
mkdir -p "${UV_CACHE_DIR}"
if [[ "$#" -ne 2 || "$1" != "--result-ref" ]]; then
  echo "usage: $0 --result-ref <implementer_result_json>" >&2
  exit 2
fi
exec uv run --locked python -m loop_product.runtime.lifecycle publish-external-result --result-ref "$2"
