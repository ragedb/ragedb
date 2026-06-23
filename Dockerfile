ARG ARCH=
FROM ${ARCH}ubuntu:24.04 AS build
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get -qq update -y && apt-get -qq dist-upgrade -y
RUN apt install -y build-essential git sudo pkg-config ccache python3-pip \
    valgrind libfmt-dev ninja-build ragel libhwloc-dev libnuma-dev libpciaccess-dev libcrypto++-dev libboost-all-dev \
    libxml2-dev xfslibs-dev libgnutls28-dev liblz4-dev libsctp-dev gcc make libprotobuf-dev protobuf-compiler python3 systemtap-sdt-dev \
    libtool cmake libyaml-cpp-dev libc-ares-dev stow openssl liburing-dev doxygen wget lsb-release gnupg software-properties-common meson
RUN wget https://apt.llvm.org/llvm.sh && chmod +x llvm.sh && ./llvm.sh 23
RUN ln -s /usr/bin/clang-23 /usr/bin/clang && ln -s /usr/bin/clang++-23 /usr/bin/clang++
RUN pip install --break-system-packages --user conan -v "conan>=2.0"
RUN ln -s ~/.local/bin/conan /usr/bin/conan
RUN git clone https://github.com/scylladb/seastar.git /data/seastar
WORKDIR /data/seastar
RUN git checkout seastar-25.05.0
RUN ./configure.py --mode=release --prefix=/usr/local --without-tests --without-apps --without-demos --cflags="-march=x86-64"
RUN ninja -C build/release install
RUN rm -rf /data/seastar/*
COPY . /data/rage
WORKDIR /data/rage
RUN conan config home && \
    mkdir -p ~/.conan2/profiles && \
    printf "[settings]\narch=x86_64\nbuild_type=Release\ncompiler=clang\ncompiler.cppstd=23\ncompiler.libcxx=libstdc++11\ncompiler.version=23\nos=Linux\n" > ~/.conan2/profiles/default && \
    sed -i 's/"18", "19", "20", "21", "22"/"18", "19", "20", "21", "22", "23"/' ~/.conan2/settings.yml && \
    printf "\n[replace_requires]\nfmt/*: fmt/11.0.2\n" >> ~/.conan2/profiles/default
RUN sed -i '/benchmark\/1.8.2/d' conanfile.txt
RUN CC=clang-23 CXX=clang++-23 CXXFLAGS="-Wno-error=c2y-extensions" conan install . --output-folder=build --build=missing -s compiler.cppstd=23 -s build_type=Release -c tools.build:jobs=4
RUN python3 -c "import glob; f = glob.glob('/root/.conan2/p/**/optional_implementation.hpp', recursive=True)[0]; c = open(f).read(); pos = c.find('T& emplace(Args&&... args) noexcept'); target = 'this->construct(std::forward<Args>(args)...);'; idx = c.find(target, pos); c = c[:idx] + '::new (static_cast<void*>(this)) optional(std::forward<Args>(args)...);\n\t\t\treturn *m_value;' + c[idx + len(target):]; open(f, 'w').write(c)"
WORKDIR /data/rage/build
RUN CC=clang-23 CXX=clang++-23 cmake -G Ninja .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DWARNINGS_AS_ERRORS=OFF -DUSE_IPO=OFF -DUseIPO=OFF -DPORTABLE=ON -DTARGET_ARCHITECTURE=generic -DBUILD_BENCHMARKS=OFF
RUN sed -i 's/isymbols_ = impl.isymbols_ ? impl.isymbols_->Copy() : nullptr;/isymbols_.reset(impl.isymbols_ ? impl.isymbols_->Copy() : nullptr);/g' _deps/iresearch-src/external/openfst/fst/fst.h
RUN sed -i 's/osymbols_ = impl.osymbols_ ? impl.osymbols_->Copy() : nullptr;/osymbols_.reset(impl.osymbols_ ? impl.osymbols_->Copy() : nullptr);/g' _deps/iresearch-src/external/openfst/fst/fst.h
RUN sed -i 's/maker_t::template make(std::forward<Args>(args)...);/maker_t::template make<Args...>(std::forward<Args>(args)...);/g' _deps/iresearch-src/core/utils/memory.hpp
RUN sed -i 's/selector_(table.s_)/selector_(table.selector_)/g' _deps/iresearch-src/external/openfst/fst/bi-table.h
RUN cmake --build . --target ragedb --parallel 4

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
COPY --from=build /lib/x86_64-linux-gnu/libcares.so.2 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libatomic.so.1 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libsctp.so.1 /lib/x86_64-linux-gnu/
COPY --from=build /lib/x86_64-linux-gnu/libprotobuf.so.32 /lib/x86_64-linux-gnu/
RUN apt-get install -y libluajit-5.1-2
WORKDIR /ragedb
COPY --from=build /data/rage/build/bin/ragedb ragedb
COPY --from=build /data/rage/build/bin/public public
EXPOSE 7243/tcp
ENTRYPOINT ["./ragedb"]
