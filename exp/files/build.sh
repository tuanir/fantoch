#!/usr/bin/env bash

# - needed for RUN_MODE="leak"
RUST_NIGHTLY="true"
# - needed for RUN_MODE="flamegraph"
FLAMEGRAPH="true"
# flag indicating whether we should just remove planet_sim folder
NUKE_PLANET_SIM="false"

# maximum number of open files
MAX_OPEN_FILES=10000

if [ $# -ne 1 ]; then
    echo "usage: build.sh branch"
    exit 1
fi

# get branch
branch=$1

# install rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
# shellcheck disable=SC1090
source "${HOME}/.cargo/env"

# check for rust updates (in case it was already installed)
rustup update

if [ "${RUST_NIGHTLY}" == "true" ]; then
    # install nightly
    rustup toolchain install nightly
fi

if [ "${FLAMEGRAPH}" == "true" ]; then
    # install perf:
    # - this command seems to be debian specific
    sudo apt-get update
    sudo apt-get install -y perf-tools-unstable

    # give permissions to perf by setting "kernel.perf_event_paranoid = -1" in "/etc/sysctl.conf"
    # - first delete current setting, if any
    sudo sed -i '/^kernel.perf_event_paranoid.*/d' /etc/sysctl.conf
    # - then append correct setting
    echo "kernel.perf_event_paranoid = -1" | sudo tee -a /etc/sysctl.conf

    # install flamegraph
    sudo apt-get install -y linux-tools-common linux-tools-generic
    cargo install flamegraph
fi

# increase maximum number of open files by changing "sys.fs.file-max"
# - first delete current setting, if any
sudo sed -i '/^sys.fs.file-max.*/d' /etc/sysctl.conf
# - then append correct setting
echo "sys.fs.file-max = ${MAX_OPEN_FILES}" | sudo tee -a /etc/sysctl.conf

# reload system configuration so that previous changes  take place
sudo sysctl --system

# install dstat
sudo apt-get install -y dstat

# maybe remove planet_sim folder
if [ "${NUKE_PLANET_SIM}" == "true" ]; then
    rm -rf planet_sim/
fi

# clone the repository if dir does not exist
if [[ ! -d planet_sim ]]; then
    git clone https://github.com/vitorenesduarte/planet_sim -b "${branch}"
fi

# pull recent changes in ${branch}
cd planet_sim/ || {
    echo "planet_sim/ directory must exist after clone"
    exit 1
}
# stash before checkout to make sure checkout will succeed
git stash
git checkout "${branch}"
git pull
