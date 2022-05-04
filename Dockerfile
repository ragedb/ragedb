FROM ubuntu:22.04 as build
ARG DEBIAN_FRONTEND=noninteractive
RUN sudo rm -r /var/lib/apt/lists/*
RUN apt-get -qq update -y && apt-get -qq dist-upgrade -y
RUN apt-get install -y build-essential git sudo pkg-config ccache python3-pip \
    valgrind libfmt-dev gcc-11 g++-11 ninja-build ragel libhwloc-dev libnuma-dev libpciaccess-dev libcrypto++-dev libboost-all-dev \
    libxml2-dev xfslibs-dev libgnutls28-dev liblz4-dev libsctp-dev gcc make libprotobuf-dev protobuf-compiler python3 systemtap-sdt-dev \
    libtool cmake libyaml-cpp-dev libc-ares-dev stow
RUN pip install --user conan
RUN ln -s ~/.local/bin/conan /usr/bin/conan
RUN git clone https://github.com/scylladb/seastar.git /data/seastar
WORKDIR /data/seastar
RUN git checkout 16b0942bb84e2329866c99913f37525289cd47f5
RUN ./configure.py --mode=release --prefix=/usr/local --c++-dialect=gnu++20 --without-tests --without-apps --without-demos
RUN ninja -C build/release install
RUN rm -rf /data/seastar/*
RUN git clone https://github.com/ragedb/luajit-recipe.git /data/luajit
WORKDIR /data/luajit
RUN conan create . 2.1.0-beta3-2022-3-20@
RUN git clone https://github.com/ragedb/sol2-recipe.git /data/sol2
WORKDIR /data/sol2
RUN conan create . 3.2.3-luajit@
RUN git clone https://github.com/ragedb/ragedb.git /data/rage
RUN mkdir /data/rage/build
WORKDIR /data/rage/build
RUN cmake .. -DCMAKE_BUILD_TYPE=Release
RUN cmake --build . --target ragedb

FROM ubuntu:22.04
COPY --from=build /lib/x86_64-linux-gnu/libboost_program_options.so.1.74.0 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libboost_thread.so.1.74.0 /lib/x86_64-linux-gnu/
COPY --from=build "/lib/x86_64-linux-gnu/libcrypto++.so.8" /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libfmt.so.8 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libgnutls.so.30 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libyaml-cpp.so.0.7 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libhwloc.so.15 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libnuma.so.1 /lib/x86_64-linux-gnu/
COPY --from=build "/lib/x86_64-linux-gnu/libstdc++.so.6" /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libm.so.6 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libgcc_s.so.1 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libp11-kit.so.0 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libidn2.so.0 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libunistring.so.2 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libtasn1.so.6 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libnettle.so.8 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libhogweed.so.6 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libgmp.so.10 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libudev.so.1 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libffi.so.8 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libc.so.6 /lib/x86_64-linux-gnu/
WORKDIR /ragedb
COPY --from=build /data/rage/build/bin/ragedb ragedb
COPY --from=build /data/rage/build/bin/public public
EXPOSE 7243/tcp
ENTRYPOINT ./ragedb
