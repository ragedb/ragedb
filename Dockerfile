ARG ARCH=
FROM ${ARCH}ubuntu:24.04 AS build
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get -qq update -y && apt-get -qq dist-upgrade -y
RUN apt install -y build-essential git sudo pkg-config ccache python3-pip \
    valgrind libfmt-dev ninja-build ragel libhwloc-dev libnuma-dev libpciaccess-dev libcrypto++-dev libboost-all-dev \
    libxml2-dev xfslibs-dev libgnutls28-dev liblz4-dev libsctp-dev gcc make libprotobuf-dev protobuf-compiler python3 systemtap-sdt-dev \
    libtool cmake libyaml-cpp-dev libc-ares-dev stow openssl liburing-dev doxygen
RUN pip install --break-system-packages --user conan -v "conan>=2.0"
RUN ln -s ~/.local/bin/conan /usr/bin/conan
RUN git clone https://github.com/scylladb/seastar.git /data/seastar
WORKDIR /data/seastar
RUN git checkout seastar-25.05.0
RUN ./configure.py --mode=release --prefix=/usr/local --without-tests --without-apps --without-demos
RUN ninja -C build/release install
RUN rm -rf /data/seastar/*
RUN git clone https://github.com/ragedb/ragedb.git /data/rage
WORKDIR /data/rage
RUN conan profile detect --force
RUN conan install . --output-folder=build --build=missing -s compiler.cppstd=23 -s build_type=Release
WORKDIR /data/rage/build
RUN cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DWARNINGS_AS_ERRORS=OFF
RUN cmake --build . --target ragedb

ARG ARCH=
FROM ${ARCH}ubuntu:24.04
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get -qq update -y && apt-get -qq dist-upgrade -y
COPY --from=build /lib/x86_64-linux-gnu/libboost_program_options.so.1.83.0 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libboost_thread.so.1.83.0 /lib/x86_64-linux-gnu/
COPY --from=build "/lib/x86_64-linux-gnu/libcrypto++.so.8" /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libfmt.so.9 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libgnutls.so.30 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libyaml-cpp.so.0.8 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libhwloc.so.15 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/liburing.so.2 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libnuma.so.1 /lib/x86_64-linux-gnu/
COPY --from=build "/lib/x86_64-linux-gnu/libstdc++.so.6" /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libm.so.6 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libgcc_s.so.1 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libp11-kit.so.0 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libidn2.so.0 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libunistring.so.5 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libtasn1.so.6 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libnettle.so.8 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libhogweed.so.6 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libgmp.so.10 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libudev.so.1 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libffi.so.8 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libc.so.6 /lib/x86_64-linux-gnu/
RUN apt-get install -y libluajit-5.1-2
WORKDIR /ragedb
COPY --from=build /data/rage/build/ragedb ragedb
COPY --from=build /data/rage/build/bin/public public
EXPOSE 7243/tcp
ENTRYPOINT ["./ragedb"]
