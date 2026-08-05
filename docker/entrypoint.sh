#!/bin/sh
set -eu

command="${1:-api}"

case "$command" in
  -*)
    exec bigname-api "$@"
    ;;
esac

case "$command" in
  api)
    shift
    exec bigname-api serve "$@"
    ;;
  phases)
    shift
    exec phase-runner run "$@"
    ;;
  phases-migrate)
    shift
    exec phase-runner init-schema "$@"
    ;;
  worker)
    shift
    exec bigname-worker run "$@"
    ;;
  migrate)
    shift
    exec bigname-worker migrate "$@"
    ;;
  bigname-api | phase-runner | bigname-worker)
    exec "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
