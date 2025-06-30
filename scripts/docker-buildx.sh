#!/bin/bash -e
WORKSPACE="$(git rev-parse --show-toplevel)"
CHANNEL="dev"
REGISTRY="docker.io"
RH_REGISTRY="registry.access.redhat.com"
TAG_LATEST=false
TAG=""
PLATFORMS=""


POSITIONAL_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
  --channel)
    CHANNEL="$2"
    shift
    shift
    ;;
  --tag)
    TAG="$2"
    shift
    shift
    ;;
  --tag-latest)
    TAG_LATEST="$2"
    shift
    ;;
  --platforms)
    PLATFORMS="$2"
    PLATFORMS="$(echo "$PLATFORMS" | xargs)"
    if [[ "$PLATFORMS" == *" "* ]]; then
        PLATFORMS="${PLATFORMS// /,}"
    fi
    shift
    shift
    ;;
  --registry)
    REGISTRY="$2"
    shift
    shift
    ;;
  --rh-registry)
    RH_REGISTRY="$2"
    shift
    shift
    ;;
  -* | --*)
    echo "Unknown option $1"
    exit 1
    ;;
  *)
    POSITIONAL_ARGS+=("$1") # save positional arg
    shift                   # past argument
    ;;
  esac
done

set -- "${POSITIONAL_ARGS[@]}"


if [ "$CHANNEL" == "dev" ]; then
  HUB="aerospike.jfrog.io/ecosystem-container-dev-local"
elif [ "$CHANNEL" == "stage" ]; then
  HUB="aerospike.jfrog.io/ecosystem-container-stage-local"
elif [ "$CHANNEL" == "prod" ]; then
  HUB="aerospike.jfrog.io/ecosystem-container-prod-local"
else
  echo "Unknown channel"
  exit 1
fi

docker login aerospike.jfrog.io -u "$DOCKER_USERNAME" -p "$DOCKER_PASSWORD"

GO_VERSION="$(curl -s 'https://go.dev/dl/?mode=json' | \
  jq -r --arg ver "go$(grep '^go ' "$WORKSPACE/go.mod" | cut -d ' ' -f2 | cut -d. -f1,2)" \
    '.[] | select(.version | startswith($ver)) | .version' | \
  sort -V | \
  tail -n1 | \
  cut -c3- | \
  tr -d '\n')"

PLATFORMS="$PLATFORMS" \
TAG="$TAG" \
HUB="$HUB" \
REGISTRY="$REGISTRY" \
RH_REGISTRY="$RH_REGISTRY" \
LATEST="$TAG_LATEST" \
GIT_BRANCH="$(git rev-parse --abbrev-ref HEAD)" \
GIT_COMMIT_SHA="$(git rev-parse HEAD)" \
VERSION="$(cat "$WORKSPACE/VERSION")" \
GO_VERSION="$GO_VERSION" \
ISO8601="$(LC_TIME=en_US.UTF-8 date "+%Y-%m-%dT%H:%M:%S%z")" \
CONTEXT="$WORKSPACE" \
docker buildx bake \
--allow=fs.read="$WORKSPACE" \
default \
--progress plain \
--file "$WORKSPACE/scripts/docker-bake.hcl"
