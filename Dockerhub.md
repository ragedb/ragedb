### Dockerhub Instructions

If updating Seastar code, use no cache in docker build command:

     docker build ... --no-cache

If updating Conan dependencies or RageDB code, use an argument in the Dockerfile before new code:  

    ARG CACHEBUST=1

# AMD64

    docker build -t ragedb/ragedb:latest-amd64 --build-arg ARCH=amd64/ .
    docker push ragedb/ragedb:latest-amd64

# ARM64V8

    docker build -t ragedb/ragedb:latest-arm64 --build-arg ARCH=arm64v8/ -f Dockerfile-arm64 .
    docker push ragedb/ragedb:latest-arm64

# Push to Dockerhub

    docker manifest rm ragedb/ragedb:latest
    docker manifest create ragedb/ragedb:latest  ragedb/ragedb:latest-amd64  ragedb/ragedb:latest-arm64
    docker manifest push ragedb/ragedb:latest
