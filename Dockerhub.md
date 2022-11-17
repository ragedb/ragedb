### Dockerhub Instructions


# AMD64

    docker build -t ragedb/ragedb:latest-amd64 --build-arg .
    docker push ragedb/ragedb:latest-amd64

# ARM64V8

    docker build -t ragedb/ragedb:latest-arm64 --build-arg ARCH=arm64v8/ -f Dockerfile-arm64 .
    docker push ragedb/ragedb:latest-arm64

# Push to Dockerhub

    docker manifest create ragedb/ragedb:latest  ragedb/ragedb:latest-amd64  ragedb/ragedb:latest-arm64
    docker manifest push ragedb/ragedb:latest
