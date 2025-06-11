group default {
  targets = [
    "aerospike-backup-tools"
  ]
}

variable CONTEXT {
  default = null
}

variable LATEST {
  default = false
}

variable TAG {
  default = ""
}

variable GIT_BRANCH {
  default = null
}

variable GIT_COMMIT_SHA {
  default = null
}

variable VERSION {
  default = null
}

variable ISO8601 {
  default = null
}

variable HUB {
  default = "aerospike.jfrog.io/ecosystem-container-dev-local"
}

variable PLATFORMS {
  default = "linux/amd64,linux/arm64"
}

variable REGISTRY {
  default = "docker.io"
}

variable GO_VERSION {
  default = "1.23.10"
}

variable RH_REGISTRY {
  default = "registry.access.redhat.com"
}

function tags {
  params = [service]
  result = LATEST == true ? [
    "${HUB}/${service}:${TAG}",
    "${HUB}/${service}:latest"
  ] : ["${HUB}/${service}:${TAG}"]
}

target aerospike-backup-tools {
  labels = {
    "org.opencontainers.image.title"         = "Aerospike Backup Tools"
    "org.opencontainers.image.description"   = "Tools for backing up and restoring Aerospike data"
    "org.opencontainers.image.documentation" = "https://github.com/aerospike/backup-go?tab=readme-ov-file#backup-go"
    "org.opencontainers.image.base.name"     = "registry.access.redhat.com/ubi9/ubi-minimal"
    "org.opencontainers.image.source"        = "https://github.com/aerospike/backup-go/tree/${GIT_BRANCH}"
    "org.opencontainers.image.vendor"        = "Aerospike"
    "org.opencontainers.image.version"       = "${VERSION}"
    "org.opencontainers.image.url"           = "https://github.com/aerospike/backup-go"
    "org.opencontainers.image.licenses"      = "Apache-2.0"
    "org.opencontainers.image.revision"      = "${GIT_COMMIT_SHA}"
    "org.opencontainers.image.created"       = "${ISO8601}"
  }

  args = {
    GO_VERSION = "${GO_VERSION}"
    REGISTRY = "${REGISTRY}"
    RH_REGISTRY = "${RH_REGISTRY}"
  }

  context    = "${CONTEXT}"
  dockerfile = "Dockerfile"
  platforms = split(",", "${PLATFORMS}")

  tags = tags("aerospike-backup-tools")
  output = ["type=image,push=true"]
}
