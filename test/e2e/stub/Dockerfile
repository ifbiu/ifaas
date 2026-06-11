# syntax=docker/dockerfile:1
#
# Stub workload image used by the e2e suite. The Go binary is compiled on
# the host (using the developer/CI's existing Go toolchain + module cache)
# and only the resulting static binary lands in the image. This avoids
# `go mod download` calls during `docker build`, which is critical in
# air-gapped clusters that cannot reach proxy.golang.org or docker.io.
#
# Build context is the directory containing this Dockerfile, not the
# repo root, so the project-wide .dockerignore (which excludes bin/)
# does not apply here.
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY scaledownz-stub /scaledownz-stub
USER 65532:65532
ENTRYPOINT ["/scaledownz-stub"]