#!/usr/bin/env bash

set -euo pipefail

# Print help message
show_help() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --install-deps   Install system packages, LLVM Clang 23, and Conan 2"
    echo "  --seastar        Download, build, and install Seastar 25.05.0"
    echo "  --ragedb         Configure and build RageDB (default)"
    echo "  --all            Run all of the above steps"
    echo "  -h, --help       Show this help message"
}

# Default flags
INSTALL_DEPS=false
BUILD_SEASTAR=false
BUILD_RAGEDB=true  # Default to building ragedb if no other flags are set

# Parse arguments
if [ "$#" -gt 0 ]; then
    BUILD_RAGEDB=false
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --install-deps) INSTALL_DEPS=true ;;
            --seastar) BUILD_SEASTAR=true ;;
            --ragedb) BUILD_RAGEDB=true ;;
            --all)
                INSTALL_DEPS=true
                BUILD_SEASTAR=true
                BUILD_RAGEDB=true
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
        shift
    done
fi

# Step 1: Install System Dependencies
if [ "$INSTALL_DEPS" = true ]; then
    echo "=== Installing system packages, LLVM 23, and Conan ==="
    export DEBIAN_FRONTEND=noninteractive
    sudo apt-get update
    sudo apt-get install -y build-essential git sudo pkg-config ccache python3-pip \
        valgrind libfmt-dev ninja-build ragel libhwloc-dev libnuma-dev libpciaccess-dev libcrypto++-dev libboost-all-dev \
        libxml2-dev xfslibs-dev libgnutls28-dev liblz4-dev libsctp-dev gcc make libprotobuf-dev protobuf-compiler python3 systemtap-sdt-dev \
        libtool cmake libyaml-cpp-dev libc-ares-dev stow openssl liburing-dev doxygen wget lsb-release gnupg software-properties-common meson

    echo "=== Installing LLVM/Clang 23 ==="
    wget https://apt.llvm.org/llvm.sh
    chmod +x llvm.sh
    sudo ./llvm.sh 23
    rm llvm.sh
    sudo ln -sf /usr/bin/clang-23 /usr/bin/clang
    sudo ln -sf /usr/bin/clang++-23 /usr/bin/clang++

    echo "=== Installing Conan 2.0+ ==="
    pip install --break-system-packages --user conan -v "conan>=2.0"
    sudo ln -sf ~/.local/bin/conan /usr/bin/conan
fi

# Step 2: Build Seastar
if [ "$BUILD_SEASTAR" = true ]; then
    echo "=== Building and installing Seastar 25.05.0 ==="
    SEASTAR_DIR=$(mktemp -d -t seastar-build-XXXXXX)
    git clone https://github.com/scylladb/seastar.git "$SEASTAR_DIR"
    cd "$SEASTAR_DIR"
    git checkout seastar-25.05.0
    ./configure.py --mode=release --prefix=/usr/local --without-tests --without-apps --without-demos
    ninja -C build/release install
    cd - > /dev/null
    rm -rf "$SEASTAR_DIR"
fi

# Step 3: Build RageDB
if [ "$BUILD_RAGEDB" = true ]; then
    echo "=== Building RageDB ==="
    
    # 1. Conan configuration
    CC=clang-23 CXX=clang++-23 conan profile detect --force
    sed -i 's/"18", "19", "20", "21", "22"/"18", "19", "20", "21", "22", "23"/' ~/.conan2/settings.yml
    if ! grep -q "replace_requires" ~/.conan2/profiles/default; then
        printf "\n[replace_requires]\nfmt/*: fmt/11.0.2\n" >> ~/.conan2/profiles/default
    fi

    # 2. Conan dependencies installation
    CC=clang-23 CXX=clang++-23 CXXFLAGS="-Wno-error=c2y-extensions" conan install . --output-folder=build --build=missing -s compiler.cppstd=23 -s build_type=Release

    # 3. Patch optional_implementation.hpp conan package
    python3 -c "
from pathlib import Path
import glob
matches = glob.glob(str(Path.home() / '.conan2/p/**/optional_implementation.hpp'), recursive=True)
if matches:
    f = matches[0]
    c = open(f).read()
    pos = c.find('T& emplace(Args&&... args) noexcept')
    target = 'this->construct(std::forward<Args>(args)...);'
    idx = c.find(target, pos)
    if idx != -1:
        c = c[:idx] + '::new (static_cast<void*>(this)) optional(std::forward<Args>(args)...);\n\t\t\treturn *m_value;' + c[idx + len(target):]
        open(f, 'w').write(c)
        print('Patched optional_implementation.hpp')
"

    # 4. CMake configuration
    mkdir -p build
    cd build
    CC=clang-23 CXX=clang++-23 cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DWARNINGS_AS_ERRORS=OFF -DUSE_IPO=OFF -DUseIPO=OFF

    # 5. Apply OpenFST and IResearch compiler patches
    sed -i 's/isymbols_ = impl.isymbols_ ? impl.isymbols_->Copy() : nullptr;/isymbols_.reset(impl.isymbols_ ? impl.isymbols_->Copy() : nullptr);/g' _deps/iresearch-src/external/openfst/fst/fst.h
    sed -i 's/osymbols_ = impl.osymbols_ ? impl.osymbols_->Copy() : nullptr;/osymbols_.reset(impl.osymbols_ ? impl.osymbols_->Copy() : nullptr);/g' _deps/iresearch-src/external/openfst/fst/fst.h
    sed -i 's/maker_t::template make(std::forward<Args>(args)...);/maker_t::template make<Args...>(std::forward<Args>(args)...);/g' _deps/iresearch-src/core/utils/memory.hpp
    sed -i 's/selector_(table.s_)/selector_(table.selector_)/g' _deps/iresearch-src/external/openfst/fst/bi-table.h

    # 6. Build target
    cmake --build . --target ragedb
    echo "=== Build completed! Binary is located at build/bin/ragedb ==="
fi
