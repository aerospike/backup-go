# syntax=docker/dockerfile:1.12.0
ARG GO_VERSION=1.23.9
ARG REGISTRY="docker.io"
ARG RH_REGISTRY="registry.access.redhat.com"

FROM --platform=$BUILDPLATFORM ${REGISTRY}/tonistiigi/xx AS xx
FROM --platform=$BUILDPLATFORM ${REGISTRY}/golang:${GO_VERSION} AS builder

ARG TARGETOS
ARG TARGETARCH

COPY --from=xx / /

WORKDIR /app/backup-go

COPY . .

RUN <<-EOF
    xx-go --wrap
    OS=${TARGETOS} ARCH=${TARGETARCH} make build
    xx-verify /app/backup-go/target/asbackup_${TARGETOS}_${TARGETARCH}
    xx-verify /app/backup-go/target/asrestore_${TARGETOS}_${TARGETARCH}
EOF

FROM ${RH_REGISTRY}/ubi9/ubi-minimal:latest
ARG TARGETOS
ARG TARGETARCH

RUN microdnf install -y shadow-utils && \
    microdnf update -y && \
    microdnf -y clean all && rm -rf /var/cache/yum && \
    groupadd --system --gid 65532 nonroot && \
    useradd --no-log-init --no-user-group --system --uid 65532 --gid 65532 --create-home nonroot

COPY --chown=nonroot:65532 --chmod=0755 --from=builder \
    /app/backup-go/target/asrestore_${TARGETOS}_${TARGETARCH} \
    /usr/bin/asrestore

COPY --chown=nonroot:65532 --chmod=0755 --from=builder \
    /app/backup-go/target/asbackup_${TARGETOS}_${TARGETARCH} \
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
	chown 65532:65532 /usr/bin/entrypoint.sh
EOF

USER nonroot
ENTRYPOINT ["/usr/bin/entrypoint.sh"]
