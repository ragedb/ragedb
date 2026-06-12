# RageDB

[![Documentation](https://img.shields.io/badge/documentation-black)](https://ragedb.com)
[![Slack](https://img.shields.io/badge/slack-purple)](https://ragedb.slack.com)
[![Twitter](https://img.shields.io/twitter/follow/rage_database.svg?style=social&label=Follow)](https://twitter.com/intent/follow?screen_name=rage_database)
![C++](https://github.com/ragedb/ragedb/workflows/Coverage/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/ragedb/ragedb/badge.svg?branch=main)](https://coveralls.io/github/ragedb/ragedb?branch=main)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=ragedb_ragedb&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=ragedb_ragedb)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=ragedb_ragedb&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=ragedb_ragedb)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=ragedb_ragedb&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=ragedb_ragedb)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=ragedb_ragedb&metric=bugs)](https://sonarcloud.io/summary/new_code?id=ragedb_ragedb)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=ragedb_ragedb&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=ragedb_ragedb)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=ragedb_ragedb&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=ragedb_ragedb)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=ragedb_ragedb&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=ragedb_ragedb)

[<p align="center"><img src="https://raw.githubusercontent.com/ragedb/ragedb.github.io/main/images/mascot-logo.webp" alt="ragedb mascot" width="660"/></p>](https://ragedb.com)
<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=53459147-2981-4e5a-97bd-d40d0faa0954" />

In Memory Property Graph Server using the Shared Nothing design from [Seastar](https://seastar.io).

[RageDB](https://ragedb.com):

- Faster than a speeding bullet train
- Connect from anywhere via HTTP
- Use the Lua programming language to query
- Apache License, version 2.0

Bring up the main website and documentation [ragedb.com](https://ragedb.com) while you spin up an instance on docker right now:


## Docker

    docker pull dockerhub.ragedb.com/ragedb/ragedb
    docker run -u 0 -p 127.0.0.1:7243:7243 --name ragedb -t dockerhub.ragedb.com/ragedb/ragedb:latest --cap-add=sys_nice

If you are running Docker on a Mac or Windows Host, you may see this error message:

    WARNING: unable to mbind shard memory; performance may suffer:

Run Docker on a Linux host for the best performance.

## Terraform

- This is work in progress, if you can make this better please help!
- Generate an SSH key if you don't already have one with `ssh-keygen -t rsa -b 4096`.
- [Install Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli)
- [Install the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html).
- [Configure the AWS CLI with an access key ID and secret access key](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html).

### Variables

#### your_region
- Which AWS Region. The options are [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions).
- E.g. `eu-west-2`.

#### your_public_key
- This will be in `~/.ssh/id_rsa.pub` by default.

### Steps
- Run `terraform init`.
- Run `terraform apply`.
- Wait a few minutes for the code to compile and the server to spin up.
- Copy the IP output by the previous command into your browser http://x.x.x.x:/7243
- Do Graphy Stuff.
- Irrecoverably shut everything down with `terraform destroy`.

This will bring up an [r5.2xlarge](https://aws.amazon.com/ec2/instance-types/r5/) with 4 cores set to 1 thread per core with 100 GB of space.


## HTTP API

RageDB exposes a rich HTTP API for schema management, node and relationship CRUD operations, path traversing, degree and neighbor queries, and Lua scripting. For the complete HTTP API endpoint reference and usage examples, see [API.md](API.md).


## Building

RageDB utilizes the high-performance Seastar framework and C++23. It runs natively on Linux (Ubuntu 24.04 recommended) and compiles using Clang 23.

### EC2 Setup (Optional)

If setting up on AWS EC2, choose an **Ubuntu Server 24.04 LTS (HVM)** instance (e.g. `r5.2xlarge`) configured with 100 GiB of storage. For optimal performance, specify CPU options with **Threads per core = 1** (disabling Hyper-Threading).

### Build Instructions

Once connected to your server, follow these steps to install the toolchain, dependencies, Seastar, and compile RageDB:

#### 1. Install System Dependencies

```bash
sudo apt-get update && sudo apt-get dist-upgrade -y
sudo apt-get install -y build-essential git sudo pkg-config ccache python3-pip \
    valgrind libfmt-dev ninja-build ragel libhwloc-dev libnuma-dev libpciaccess-dev libcrypto++-dev libboost-all-dev \
    libxml2-dev xfslibs-dev libgnutls28-dev liblz4-dev libsctp-dev gcc make libprotobuf-dev protobuf-compiler python3 systemtap-sdt-dev \
    libtool cmake libyaml-cpp-dev libc-ares-dev stow openssl liburing-dev doxygen wget lsb-release gnupg software-properties-common meson
```

#### 2. Install LLVM Clang 23 Compiler

```bash
wget https://apt.llvm.org/llvm.sh && chmod +x llvm.sh && sudo ./llvm.sh 23
sudo ln -sf /usr/bin/clang-23 /usr/bin/clang && sudo ln -sf /usr/bin/clang++-23 /usr/bin/clang++
```

#### 3. Install and Configure Conan 2

```bash
pip install --break-system-packages --user conan -v "conan>=2.0"
sudo ln -sf ~/.local/bin/conan /usr/bin/conan
```

#### 4. Compile and Install Seastar

```bash
git clone https://github.com/scylladb/seastar.git /tmp/seastar
cd /tmp/seastar
git checkout seastar-25.05.0
./configure.py --mode=release --prefix=/usr/local --without-tests --without-apps --without-demos
ninja -C build/release install
rm -rf /tmp/seastar
cd ~
```

#### 5. Clone and Build RageDB

```bash
git clone --recursive https://github.com/ragedb/ragedb.git
cd ragedb

# Detect Conan profile with Clang 23
CC=clang-23 CXX=clang++-23 conan profile detect --force
sed -i 's/"18", "19", "20", "21", "22"/"18", "19", "20", "21", "22", "23"/' ~/.conan2/settings.yml
printf "\n[replace_requires]\nfmt/*: fmt/11.0.2\n" >> ~/.conan2/profiles/default

# Install package dependencies via Conan
CC=clang-23 CXX=clang++-23 CXXFLAGS="-Wno-error=c2y-extensions" conan install . --output-folder=build --build=missing -s compiler.cppstd=23 -s build_type=Release

# Patch conan dependencies for modern compiler compatibility
python3 -c "from pathlib import Path; import glob; f = glob.glob(str(Path.home() / '.conan2/p/**/optional_implementation.hpp'), recursive=True)[0]; c = open(f).read(); pos = c.find('T& emplace(Args&&... args) noexcept'); target = 'this->construct(std::forward<Args>(args)...);'; idx = c.find(target, pos); c = c[:idx] + '::new (static_cast<void*>(this)) optional(std::forward<Args>(args)...);\n\t\t\treturn *m_value;' + c[idx + len(target):]; open(f, 'w').write(c)"

# Configure with CMake (disabling LTO/IPO for dependencies compiling under Clang 23)
cd build
CC=clang-23 CXX=clang++-23 cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DWARNINGS_AS_ERRORS=OFF -DUSE_IPO=OFF -DUseIPO=OFF

# Apply OpenFST and IResearch compilation patches
sed -i 's/isymbols_ = impl.isymbols_ ? impl.isymbols_->Copy() : nullptr;/isymbols_.reset(impl.isymbols_ ? impl.isymbols_->Copy() : nullptr);/g' _deps/iresearch-src/external/openfst/fst/fst.h
sed -i 's/osymbols_ = impl.osymbols_ ? impl.osymbols_->Copy() : nullptr;/osymbols_.reset(impl.osymbols_ ? impl.osymbols_->Copy() : nullptr);/g' _deps/iresearch-src/external/openfst/fst/fst.h
sed -i 's/maker_t::template make(std::forward<Args>(args)...);/maker_t::template make<Args...>(std::forward<Args>(args)...);/g' _deps/iresearch-src/core/utils/memory.hpp
sed -i 's/selector_(table.s_)/selector_(table.selector_)/g' _deps/iresearch-src/external/openfst/fst/bi-table.h

# Build RageDB
cmake --build . --target ragedb
```

#### 6. Start the Server

```bash
cd bin
./ragedb
```

### Troubleshooting

If you get errors regarding conan locks, run:

    conan remove --locks

If you get errors like:
    
    Creation of perf_event based stall detector failed, falling back to posix timer: std::system_error

Then this should fix it: 

    echo 0 > /proc/sys/kernel/perf_event_paranoid

To make it permanent edit /etc/sysctl.conf by adding:

    kernel.perf_event_paranoid = 0

If you get errors about aio-max-nr you'll want to increase it:

    sudo echo 88208 > /proc/sys/fs/aio-max-nr

We allocate 11026 aio slots per shard (10000 + 1024 + 2) so 8 shards = 88208

To make it permanent edit /etc/sysctl.conf by adding:

    fs.aio-max-nr = 88208

### Todos

    - Command Logging (started)
    - Recovering (started)
    - Snapshots
    - Replication
    - Metrics
    - Everthing Else
    - Take over the world

### Reactor Stalls

    - When running KHop queries on combining the RoaringBitmaps using |=
    - When importing data on the power of two growth of the tsl sparse_map
    - In GetToKeysFromRelationshipsInCSV
    - In PartitionRelationshipsInCSV 


### Missing Features that can be added "easily"
    - Allow Node and Relationship Type handlers to take a json map defining the property keys and data types
    - Allow additional data types: 8, 16 and 32 bit integers, 32 bit floats, byte, list of bytes, nested types 
    - NodeTypes and RelationshipTypes should allow type deletion and type id reuse
    - Allow property type conversions (int to double, string to int, int to int array, etc).

### PVS Commands

    pvs-studio-analyzer trace -- make
    pvs-studio-analyzer analyze
    plog-converter -a GA:1,2 -t tasklist  PVS-Studio.log
