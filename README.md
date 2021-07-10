# RageDB

In Memory Property Graph Server using the Shared Nothing design from Seastar.

## Building

RageDB uses Seastar which only runs on *nix servers (no windows or mac) so use your local linux desktop or use EC2.

On EC2 launch an instance:

    Step 1: Choose an Amazon Machine Image
    Ubuntu Server 20.04 LTS(HVM), SSD Volume Type - ami-09e67e426f25ce0d7

    Step 2: Choose Instance Type
    r5.2xlarge

    Step 3: Configure Instance
    Specify CPU options
    Threads per core = 1

    Step 4: Add Storage
    100 GiB

    Launch

Once the instance is running, connect to it and start a "screen" session, then follow these steps:

First let's update and upgrade to the latest versions of local software:

    sudo apt-get update && sudo apt-get dist-upgrade

Install Seastar (this will take a while, that's why we are using screen):

    git clone https://github.com/scylladb/seastar.git
    cd seastar
    sudo ./install_dependencies.sh
    ./configure.py --mode=release --prefix=/usr/local
    sudo ninja -C build/release install

Install Additional Dependencies

    sudo apt-get install -y ccache python3-pip

Install conan

    pip install --user conan
    sudo ln -s ~/.local/bin/conan /usr/bin/conan

Install LuaJIT

    sudo apt-get install -y luajit luajit-5.1-dev

### Troubleshooting

If you get errors regarding conan locks, run:

    conan remove --locks

### Technical Debt

    - NodeTypes and RelationshipTypes should allow type deletion and type id reuse