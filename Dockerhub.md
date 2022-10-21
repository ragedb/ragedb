### Dockerhub Instructions


# AMD64

    docker build -t ragedb/ragedb:manifest-amd64 --build-arg ARCH=amd64/ .
    docker push ragedb/ragedb:manifest-amd64

# ARM64V8

    docker build -t ragedb/ragedb:manifest-arm64 --build-arg ARCH=arm64v8/ ./ARMDockerfile
    docker push ragedb/ragedb:manifest-arm64

# Push to Dockerhub

    docker manifest create ragedb/ragedb:manifest-latest --amend ragedb/ragedb:manifest-amd64 --amend ragedb/ragedb:manifest-arm64
    docker manifest push ragedb/ragedb:manifest-latest
