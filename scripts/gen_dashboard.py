#!/usr/bin/env python3
"""Gera o .lvdash.json do dashboard de observabilidade por ambiente.

Le o template .tpl, substitui {{SCHEMA_PREFIX}} pelo prefixo de schema
do target informado e escreve o .lvdash.json final.

Uso:
    python scripts/gen_dashboard.py <target>

Targets validos: dev, prod, test
"""

import json
import sys
from pathlib import Path

TARGETS = {
    "dev": "proj_cvm_dev",
    "prod": "proj_cvm_prod",
    "test": "proj_cvm_test",
}

ROOT = Path(__file__).resolve().parent.parent
TPL_PATH = ROOT / "resources/dashboards/painel_observabilidade.lvdash.json.tpl"
OUT_PATH = ROOT / "resources/dashboards/painel_observabilidade.lvdash.json"


def main():
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <target>")
        print(f"Valid targets: {list(TARGETS)}")
        sys.exit(1)

    target = sys.argv[1]
    if target not in TARGETS:
        print(f"Unknown target: {target}. Valid: {list(TARGETS)}")
        sys.exit(1)

    schema_prefix = TARGETS[target]

    template = TPL_PATH.read_text(encoding="utf-8")
    generated = template.replace("{{SCHEMA_PREFIX}}", schema_prefix)

    json.loads(generated)

    OUT_PATH.write_text(generated, encoding="utf-8")
    print(f"Generated {OUT_PATH.relative_to(ROOT)} for target={target} schema_prefix={schema_prefix}")


if __name__ == "__main__":
    main()
