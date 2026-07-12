#!/usr/bin/env sh
set -eu

# Architecture documentation task runner for the Event Processing Lab.
#
# By default every task runs inside a pinned docs-as-code-toolkit `docs-toolbox`
# container image, so local runs and CI use the same reproducible toolchain
# (Ruby, Node.js, Asciidoctor, PlantUML, Graphviz). The reproducibility
# guarantee holds only in this container mode.
#
# This wraps the architecture validators and generators vendored from the
# architecture-knowledge-toolkit (metamodel/, scripts/, templates/). It does NOT
# build the Java modules: the docs-toolbox image has no JDK. Use the Gradle
# wrapper (./gradlew) for the Java build and tests.
#
# Set DOCS_TOOLBOX_LOCAL=1 to run a task against the host toolchain instead
# (whatever Ruby/Node is installed locally). If no container engine is available
# and local mode was not requested, the task aborts rather than silently using
# an unpinned host toolchain. Each task maps to a documented local command.
#
# Modeled on the toolkit's own reference build.sh:
# https://github.com/docs-as-code-toolkit/architecture-knowledge-toolkit/blob/main/build.sh

COMMAND="${1:-build}"
if [ "$#" -gt 0 ]; then
  shift
fi

# Pin the image tag for reproducibility. Override with DOCS_TOOLBOX_IMAGE, for
# example to a digest for an even stricter pin.
DOCS_TOOLBOX_IMAGE="${DOCS_TOOLBOX_IMAGE:-ghcr.io/docs-as-code-toolkit/docs-toolbox:v1.3.1}"
BUILD_DIR="${BUILD_DIR:-build/architecture}"
SOURCE_DOC="${SOURCE_DOC:-src/docs/doc-001-arc42.adoc}"

usage() {
  cat <<'USAGE'
Usage: ./build.sh <task> [args]

Tasks:
  validate         Validate architecture artifact metadata and relations.
  generate         Validate, then generate derived AsciiDoc fragments/indexes.
  adapters         Regenerate the agent adapters from local skills / the toolkit.
  check-adapters   Fail if the generated agent adapters are out of date.
  build            Generate fragments and render the architecture HTML.
  all              Run check-adapters and build (build also validates+generates).
  clean            Remove local architecture build output.
  help             Show this help.

Execution modes:
  Container (default)  Runs inside the pinned docs-toolbox image (Docker/Podman).
                       This is the reproducible mode.
  Local                Set DOCS_TOOLBOX_LOCAL=1 to run against the host toolchain.
                       Not reproducible; uses whatever Ruby/Node is installed.

With neither a container engine nor DOCS_TOOLBOX_LOCAL=1, the task aborts.
Override the image with DOCS_TOOLBOX_IMAGE. The Java modules are built with
./gradlew, not this script (docs-toolbox has no JDK).
USAGE
}

find_engine() {
  if command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
    printf '%s\n' "podman"
  elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    printf '%s\n' "docker"
  else
    printf '%s\n' ""
  fi
}

run_local_validate() {
  ruby scripts/validate-metamodel.rb
}

run_local_generate() {
  ruby scripts/validate-metamodel.rb --generate
}

run_local_adapters() {
  node scripts/build-agent-adapters.js
}

run_local_check_adapters() {
  node scripts/check-agent-adapters.js
}

run_local_build() {
  run_local_generate
  mkdir -p "$BUILD_DIR"
  asciidoctor \
    -r asciidoctor-diagram \
    -r asciidoctor-diagram/plantuml \
    --failure-level=ERROR \
    -a skip-front-matter \
    -a toc=left \
    -a sectanchors \
    -a icons=font \
    -a imagesdir=. \
    -a imagesoutdir="$BUILD_DIR" \
    -D "$BUILD_DIR" \
    -o index.html \
    "$SOURCE_DOC"
  echo "Built architecture HTML: $BUILD_DIR/index.html"
}

# `build` already runs generate (and therefore validate), so `all` does not
# validate separately to avoid a redundant pass.
run_local_all() {
  run_local_check_adapters
  run_local_build
}

run_local() {
  case "$COMMAND" in
    validate) run_local_validate ;;
    generate) run_local_generate ;;
    adapters) run_local_adapters ;;
    check-adapters) run_local_check_adapters ;;
    build) run_local_build ;;
    all) run_local_all ;;
    clean)
      rm -rf "$BUILD_DIR"
      echo "Removed $BUILD_DIR"
      ;;
    help|-h|--help) usage ;;
    *)
      usage
      exit 2
      ;;
  esac
}

# Run the task in the container, mapping file ownership so generated files stay
# owned by the invoking user on native Linux. Docker runs as root by default, so
# pass an explicit user plus a writable HOME. Rootless Podman maps container root
# to the host user with --userns=keep-id.
run_in_container() {
  ENGINE="$1"
  shift

  case "$ENGINE" in
    podman)
      podman run --rm \
        --userns=keep-id \
        -e EVENT_PROCESSING_LAB_IN_CONTAINER=1 \
        -e BUILD_DIR="$BUILD_DIR" \
        -e SOURCE_DOC="$SOURCE_DOC" \
        -e HOME=/tmp \
        -v "$PWD":/app \
        -w /app \
        "$DOCS_TOOLBOX_IMAGE" \
        sh ./build.sh "$COMMAND" "$@"
      ;;
    docker)
      docker run --rm \
        --user "$(id -u):$(id -g)" \
        -e EVENT_PROCESSING_LAB_IN_CONTAINER=1 \
        -e BUILD_DIR="$BUILD_DIR" \
        -e SOURCE_DOC="$SOURCE_DOC" \
        -e HOME=/tmp \
        -v "$PWD":/app \
        -w /app \
        "$DOCS_TOOLBOX_IMAGE" \
        sh ./build.sh "$COMMAND" "$@"
      ;;
  esac
}

# Inside the container: always run locally against the image toolchain.
if [ "${EVENT_PROCESSING_LAB_IN_CONTAINER:-}" = "1" ]; then
  run_local "$@"
  exit 0
fi

case "$COMMAND" in
  clean|help|-h|--help)
    run_local "$@"
    ;;
  validate|generate|adapters|check-adapters|build|all)
    if [ "${DOCS_TOOLBOX_LOCAL:-}" = "1" ]; then
      echo "DOCS_TOOLBOX_LOCAL=1: running '$COMMAND' against the host toolchain (not reproducible)." >&2
      run_local "$@"
    else
      ENGINE="$(find_engine)"
      if [ -n "$ENGINE" ]; then
        run_in_container "$ENGINE" "$@"
      else
        echo "No running container engine (Docker or Podman) found." >&2
        echo "Start one to run '$COMMAND' in the reproducible docs-toolbox image," >&2
        echo "or set DOCS_TOOLBOX_LOCAL=1 to run against the host toolchain." >&2
        exit 1
      fi
    fi
    ;;
  *)
    usage
    exit 2
    ;;
esac
