#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
    echo "Uso: bash scripts/run.sh {s0|s1|s2|cliente|consola|build}"
    echo "Executa um processo no terminal atual (Linux ou WSL)."
}

if [[ $# -ne 1 ]]; then
    usage >&2
    exit 1
fi

case "$1" in
    s0) module=server; args="9000 0 A" ;;
    s1) module=server; args="9000 1 A" ;;
    s2) module=server; args="9000 2 A" ;;
    cliente) module=app; args="1 localhost 9000 A" ;;
    consola) module=console; args="localhost 9000 A" ;;
    build) module=.; args= ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 1 ;;
esac

source "$ROOT/dev_env/env.sh"
cd -- "$ROOT/$module"

if [[ "$1" == build ]]; then
    exec mvn clean install
fi

exec mvn exec:java "-Dexec.args=$args"
