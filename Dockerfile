# syntax=docker/dockerfile:1.12.0

ARG GO_VERSION=1.23.10
ARG REGISTRY="docker.io"

FROM --platform=$BUILDPLATFORM ${REGISTRY}/tonistiigi/xx AS xx
FROM --platform=$BUILDPLATFORM ${REGISTRY}/golang:${GO_VERSION} AS builder

ARG TARGETOS
ARG TARGETARCH

COPY --from=xx / /

WORKDIR /backup-go

COPY . .

RUN <<-EOF
    xx-go --wrap
    OS=${TARGETOS} ARCH=${TARGETARCH} make build
    xx-verify /backup-go/target/asbackup_${TARGETOS}_${TARGETARCH}
    xx-verify /backup-go/target/asrestore_${TARGETOS}_${TARGETARCH}
EOF

FROM ${REGISTRY}/alpine:latest

ARG TARGETOS
ARG TARGETARCH

RUN apk update &&  \
    apk upgrade --no-cache

RUN apk add --no-cache shadow && \
    addgroup -g 65532 -S abgroup && \
    adduser -S -u 65532 -G abgroup -h /home/abuser abuser

COPY --chown=abuser:abgroup --chmod=0755 --from=builder \
    /backup-go/target/asrestore_${TARGETOS}_${TARGETARCH} \
    /usr/bin/asrestore

COPY --chown=abuser:abgroup --chmod=0755 --from=builder \
    /backup-go/target/asbackup_${TARGETOS}_${TARGETARCH} \
    /usr/bin/asbackup

RUN <<-EOF
	#!/bin/sh
	set -e

	cat <<-'SCRIPT' > /usr/bin/entrypoint.sh
	#!/bin/sh
	set -e

	exec "$@"
	SCRIPT

	chmod 0755 /usr/bin/entrypoint.sh
	chown abuser:abgroup /usr/bin/entrypoint.sh
EOF

USER abuser

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
