#!/usr/bin/env python3
"""Regression: whole-paper source parser must ignore commented envs and preserve labeled equations."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-whole-paper-source-parser-hygiene][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    sys.path.insert(0, str(ROOT))
    try:
        from loop_product.dispatch.bootstrap import _parse_whole_paper_source_tex
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    tex = r"""
\title{Synthetic}
\section{Main}
% \begin{theorem}
% \label{thm:commented}
% This commented theorem must not be extracted.
% \end{theorem}
\begin{equation}
x = 1
\label{eq:one}
\end{equation}
\begin{theorem}
\label{thm:uses_eq}
Under \eqref{eq:one}, we conclude the claim.
\end{theorem}
"""
    with tempfile.TemporaryDirectory(prefix="whole-paper-parser-hygiene-") as tmp:
        tex_path = Path(tmp) / "main.tex"
        tex_path.write_text(tex, encoding="utf-8")
        parsed = _parse_whole_paper_source_tex(tex_path)

    node_ids = [str(item.get("node_id") or "") for item in parsed["theorem_items"]]
    if "thm:commented" in node_ids:
        return _fail("commented theorem environments must not be extracted into theorem_items")
    if "eq:one" not in node_ids:
        return _fail("labeled equation environments must be extracted into theorem_items for reference closure")
    if "thm:uses_eq" not in node_ids:
        return _fail("active theorem must still be extracted")

    edges = {(str(edge.get("from_node_id") or ""), str(edge.get("to_ref") or "")) for edge in parsed["edges"]}
    if ("thm:uses_eq", "eq:one") not in edges:
        return _fail("internal dependency graph must preserve theorem-to-equation references")

    print("[loop-system-whole-paper-source-parser-hygiene] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
