# Aerospike Backup Tools

## Build and Install
To build and install Aerospike Backup Tools from source:

```bash
make build && make install
```

To uninstall the tools:
```bash
make uninstall
```

## Build Docker Image
Build and push a multi-platform Docker image:
```bash
DOCKER_USERNAME="<jfrog-username>" DOCKER_PASSWORD="<jfrog-password>" TAG="<tag>" make docker-buildx 
```

Build a Docker image for local use:
```bash
TAG="<tag>" make docker-build
```

A single docker image, including both tools, will be created.

Usage example:

```bash
docker run --rm aerospike-backup-tools:<tag> asrestore --help
docker run --rm aerospike-backup-tools:<tag> asbackup --help
```

## Build Linux Packages
To generate `.rpm` and `.deb` packages for supported Linux architectures (`linux/amd64`, `linux/arm64`):
```bash
make packages
```
The generated packages and their `sha256` checksum files will be located in the `/target` directory.
