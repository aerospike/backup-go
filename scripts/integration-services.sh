#!/usr/bin/env bash
# Start or stop the Docker services used by integration tests.
# Image versions and setup match .github/workflows/tests.yml.
set -euo pipefail

readonly AEROSPIKE_IMAGE="aerospike/aerospike-server-enterprise:8.1.2.4"
readonly AZURITE_IMAGE="mcr.microsoft.com/azure-storage/azurite:3.35.0"
readonly FAKE_GCS_IMAGE="fsouza/fake-gcs-server:1.52.2"
readonly MINIO_IMAGE="minio/minio:RELEASE.2025-09-07T16-13-09Z"

readonly AEROSPIKE_NAME="backup-go-aerospike"
readonly AZURITE_NAME="backup-go-azurite"
readonly FAKE_GCS_NAME="backup-go-fake-gcs"
readonly MINIO_NAME="backup-go-minio"

usage() {
	cat <<'EOF'
Usage: scripts/integration-services.sh <command>

Commands:
  up       Start all integration-test services (default)
  down     Stop and remove the containers
  status   Show whether each service is running
  restart  down, then up

Then run:
  make test-integration
  make coverage
EOF
}

require_docker() {
	if ! command -v docker >/dev/null 2>&1; then
		echo "error: docker is not installed or not on PATH" >&2
		exit 1
	fi
}

container_running() {
	local name="$1"
	docker ps --format '{{.Names}}' | grep -qx "$name"
}

container_exists() {
	local name="$1"
	docker ps -a --format '{{.Names}}' | grep -qx "$name"
}

start_container() {
	local name="$1"
	shift

	if container_running "$name"; then
		echo "$name: already running"
		return 0
	fi

	if container_exists "$name"; then
		echo "$name: starting existing container"
		docker start "$name" >/dev/null
		return 0
	fi

	echo "$name: creating container"
	docker run -d --name "$name" "$@" >/dev/null
}

wait_for_azurite() {
	echo "Waiting for Azurite..."
	# Azurite returns 400/403 on a bare account URL when it is up; do not use curl -f
	# (that treats 4xx as failure and the loop never exits).
	until curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:10000/devstoreaccount1 | grep -qE '400|403'; do
		sleep 1
	done
	echo "Azurite: ready"
}

wait_for_minio() {
	echo "Waiting for MinIO..."
	until curl -sfI http://127.0.0.1:9000/minio/health/live 2>/dev/null | grep -q "OK"; do
		sleep 3
	done
	echo "MinIO: ready"
}

configure_minio_buckets() {
	# Idempotent: ignore errors if buckets or policies already exist.
	docker exec "$MINIO_NAME" sh -c '
		mc alias set myminio http://127.0.0.1:9000 minioadmin minioadminpassword
		mc mb myminio/backup 2>/dev/null || true
		mc mb myminio/asbackup 2>/dev/null || true
		mc anonymous set upload myminio/backup
		mc anonymous set download myminio/backup
		mc anonymous set public myminio/backup
		mc anonymous set upload myminio/asbackup
		mc anonymous set download myminio/asbackup
		mc anonymous set public myminio/asbackup
	'
	echo "MinIO: buckets backup and asbackup configured"
}

start_aerospike() {
	start_container "$AEROSPIKE_NAME" \
		-p 3000-3002:3000-3002 \
		"$AEROSPIKE_IMAGE"
	echo "Aerospike: ready (ports 3000-3002)"
}

start_azurite() {
	start_container "$AZURITE_NAME" \
		-p 10000:10000 \
		"$AZURITE_IMAGE" \
		azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck
	wait_for_azurite
}

start_fake_gcs() {
	start_container "$FAKE_GCS_NAME" \
		-p 4443:4443 \
		--entrypoint sh \
		"$FAKE_GCS_IMAGE" \
		-c "/bin/fake-gcs-server -data /data -scheme http -public-host 127.0.0.1:4443"
	echo "fake-gcs: ready (port 4443)"
}

start_minio() {
	start_container "$MINIO_NAME" \
		-p 9000:9000 \
		-e "MINIO_ROOT_USER=minioadmin" \
		-e "MINIO_ROOT_PASSWORD=minioadminpassword" \
		-e "MINIO_ACCESS_KEY=minioadmin" \
		-e "MINIO_SECRET_KEY=minioadminpassword" \
		-e "MINIO_BROWSER=off" \
		"$MINIO_IMAGE" server /data
	wait_for_minio
	configure_minio_buckets
}

cmd_up() {
	require_docker
	start_aerospike
	start_azurite
	start_fake_gcs
	start_minio
	echo
	echo "All integration services are up. Run: make test-integration"
}

cmd_down() {
	require_docker
	for name in "$MINIO_NAME" "$FAKE_GCS_NAME" "$AZURITE_NAME" "$AEROSPIKE_NAME"; do
		if container_exists "$name"; then
			echo "$name: removing"
			docker rm -f "$name" >/dev/null
		else
			echo "$name: not found"
		fi
	done
}

cmd_status() {
	require_docker
	printf "%-22s %s\n" "SERVICE" "STATUS"
	for name in "$AEROSPIKE_NAME" "$AZURITE_NAME" "$FAKE_GCS_NAME" "$MINIO_NAME"; do
		if container_running "$name"; then
			printf "%-22s %s\n" "$name" "running"
		elif container_exists "$name"; then
			printf "%-22s %s\n" "$name" "stopped"
		else
			printf "%-22s %s\n" "$name" "missing"
		fi
	done
}

main() {
	local cmd="${1:-up}"
	case "$cmd" in
	up) cmd_up ;;
	down) cmd_down ;;
	status) cmd_status ;;
	restart) cmd_down; cmd_up ;;
	-h | --help | help) usage ;;
	*)
		echo "error: unknown command: $cmd" >&2
		usage >&2
		exit 1
		;;
	esac
}

main "$@"
