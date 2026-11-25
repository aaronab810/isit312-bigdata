#!/usr/bin/env bash
set -euo pipefail
if [[ $# -eq 3 && $1 == getconf && $2 == -confKey && $3 == fs.defaultFS ]]; then
  printf '%s\n' "${MOCK_DEFAULT_FS:-hdfs://localhost:8020}"
  exit 0
fi
[[ $1 == dfs ]]; shift
cmd=$1; shift
case "$cmd" in
  -mkdir) [[ ${1:-} != -p ]] || shift; mkdir -p "$@" ;;
  -test) flag=$1; shift; test "$flag" "$1" ;;
  -stat) shift; wc -c < "$1" | tr -d ' ' ;;
  -cat) cat "$1" ;;
  -copyFromLocal) cp "$1" "$2" ;;
  -mv) mv "$1" "$2" ;;
  -rm) rm -- "$1" ;;
  -touchz) touch "$1" ;;
  -find) find "$1" -type f ;;
  -ls) [[ "$1" == -R ]]; shift; find "$1" -type f -exec printf '%s\n' '-rw-r--r-- 1 user group 0 date time {}' \; ;;
  *) echo "Unsupported mock operation $cmd" >&2; exit 2 ;;
esac
