FROM ubuntu:latest as build
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get -qq update && apt-get -qq install -y apt-utils && apt-get -qq install -y sudo software-properties-common
RUN sudo add-apt-repository 'deb http://mirrors.kernel.org/ubuntu hirsute main universe'
RUN apt-get -qq update
RUN apt-get -qq install -y build-essential git sudo pkg-config ccache python3-pip luajit luajit-5.1-dev
RUN apt-get -qq install -y valgrind libfmt-dev gcc-11 g++-11 ninja-build ragel libhwloc-dev libnuma-dev libpciaccess-dev libcrypto++-dev libboost-all-dev libxml2-dev xfslibs-dev libgnutls28-dev liblz4-dev libsctp-dev gcc make libprotobuf-dev protobuf-compiler python3 systemtap-sdt-dev libtool cmake libyaml-cpp-dev libc-ares-dev stow
RUN sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 60 --slave /usr/bin/g++ g++ /usr/bin/g++-11
RUN sudo update-alternatives --auto gcc
RUN pip install --user conan
RUN sudo ln -s ~/.local/bin/conan /usr/bin/conan
RUN git clone https://github.com/scylladb/seastar.git /data/seastar
WORKDIR /data/seastar
RUN ./configure.py --mode=release --prefix=/usr/local
RUN sudo ninja -C build/release install
RUN rm -rf /data/seastar/*
RUN git clone https://github.com/ragedb/ragedb.git /data/rage
RUN mkdir /data/rage/build
WORKDIR /data/rage/build
RUN cmake .. -DCMAKE_BUILD_TYPE=Release
RUN cmake --build . --target ragedb

FROM ubuntu:latest
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get -qq update && apt-get -qq install -y apt-utils && apt-get -qq install -y sudo software-properties-common
RUN sudo add-apt-repository 'deb http://mirrors.kernel.org/ubuntu hirsute main universe'
RUN apt-get -qq update
RUN apt-get -qq update
RUN apt-get -qq install -y luajit luajit-5.1-dev libfmt-dev libhwloc-dev libnuma-dev libpciaccess-dev libcrypto++-dev libboost-all-dev libboost-program-options-dev libxml2-dev xfslibs-dev libgnutls28-dev liblz4-dev libsctp-dev libprotobuf-dev protobuf-compiler systemtap-sdt-dev libtool libyaml-cpp-dev libc-ares-dev stow
WORKDIR /ragedb
COPY --from=build /data/rage/build/bin/ragedb ragedb
EXPOSE 7243/tcp
ENTRYPOINT ./ragedb