# Aerospike Backup Tools

The Aerospike Backup Tools project is planned to be moved to a separate repository. For now, 
these tools are built from the root directory of the `backup-go` repository.

## Usage Guide

### Build and Install

To **build and install** `aerospike-backup-tools` from source, run:

```bash
make build && make install
```

To **uninstall** the tools, run:
```bash
make uninstall
```
### Build Docker Images
**Build and push a multi-platform Docker image:**
```bash
DOCKER_USERNAME="<jforg-username>" DOCKER_PASSWORD="<jfrog-password>" TAG="<tag>" make docker-buildx 
```

**Build a Docker image for local use:**
```bash
TAG="<tag>" make docker-build
```

### Build Linux Packages
To generate `.rpm` and `.deb` packages for supported Linux architectures (`linux/amd64`,`linux/arm64`) run:
```bash
make packages
```
The generated packages and their `sha256` checksum files will be located in the `/target` directory.