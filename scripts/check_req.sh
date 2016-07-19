#!/bin/bash

set -e

echo "Checking requirements..."
mkdir -p ${DEPS_PATH}
cd $DEPS_PATH

export PATH=$PATH:$GOROOT/bin

SUDO=
if which sudo > /dev/null; then 
    SUDO=sudo
fi

function get_linux_platform {
    if [ -f /etc/redhat-release ]; then 
        # For CentOS or redhat, we treat all as CentOS.
        echo "CentOS"
    elif [ -f /etc/lsb-release ]; then
        DIST=`cat /etc/lsb-release | grep '^DISTRIB_ID' | awk -F=  '{ print $2 }'`
        echo "$DIST"
    else
        echo "Unknown"
    fi 
}

function install_go {
    echo "Intall go ..."
    case "$OSTYPE" in 
        linux*) 
            if [ ! -d $GOROOT ]; then
                curl -L https://storage.googleapis.com/golang/go1.6.2.linux-amd64.tar.gz -o golang.tar.gz
                tar -C ${DEPS_PATH}/ -xzf golang.tar.gz
            fi
            # `go get` needs git and hg.
            dist=$(get_linux_platform)
            case $dist in
                Ubuntu)
                    ${SUDO} apt-get install -y mercurial git
                ;;
                CentOS)
                    ${SUDO} yum install -y hg git
                ;;
                *)
                    echo "unsupported platform $dist, you may install Go manually"
                    exit 1
                ;;
            esac
        ;;

        darwin*)
            brew update
            brew install go
            brew install git
            brew install mercurial
            echo 'export GOVERSION=$(brew list go | head -n 1 | cut -d '/' -f 6)' >> ${HOME}/.bashrc 
            echo 'export GOROOT=$(brew --prefix)/Cellar/go/${GOVERSION}/libexec' >> ${HOME}/.bashrc
            echo 'export PATH=$PATH:$GOROOT/bin' >> ${HOME}/.bashrc
        ;;

        *)
            echo "unsupported $OSTYPE"
            exit 1
        ;;
    esac
}

function install_gpp {
    echo "Install g++ ..."
    case "$OSTYPE" in 
        linux*) 
            dist=$(get_linux_platform)
            case $dist in
                Ubuntu)
                    ${SUDO} apt-get install -y g++
                ;;
                CentOS)
                    ${SUDO} yum install -y gcc-c++
                ;;
                *)
                    echo "unsupported platform $dist, you may install RocksDB manually"
                    exit 1
                ;;
            esac
        ;;

        darwin*)
            # refer to https://github.com/facebook/rocksdb/blob/master/INSTALL.md
            xcode-select --install
            brew update
            brew tap homebrew/versions
            brew install gcc48 --use-llvm
        ;;

        *)
            echo "unsupported $OSTYPE"
            exit 1
        ;;
    esac
}

# Check rust
if ! which cargo > /dev/null; then
    curl -sSf https://static.rust-lang.org/rustup.sh | sh -s -- --channel=nightly
fi

# Check go
if which go > /dev/null; then
    # requires go >= 1.5
    GO_VER_1=`go version | awk 'match($0, /([0-9])+(\.[0-9])+/) { ver = substr($0, RSTART, RLENGTH) ; ver = split(ver, n, ".") ; print n[1]}'`
    GO_VER_2=`go version | awk 'match($0, /([0-9])+(\.[0-9])+/) { ver = substr($0, RSTART, RLENGTH) ; ver = split(ver, n, ".") ; print n[2]}'`
    if [[ (($GO_VER_1 -eq 1 && $GO_VER_2 -lt 5)) || (($GO_VER_1 -lt 1)) ]]; then
        echo "Please upgrade Go to 1.5 or later."
        exit 1
    fi
else
    install_go
fi

# Check g++
if which g++ > /dev/null; then
    # Check g++ version, RocksDB requires >= 4.7
    G_VER_1=`g++ --version | awk 'match($0, /([0-9])+(\.[0-9])+/) { ver = substr($0, RSTART, RLENGTH) ; ver = split(ver, n, ".") ; print n[1]}'`
    G_VER_2=`g++ --version | awk 'match($0, /([0-9])+(\.[0-9])+/) { ver = substr($0, RSTART, RLENGTH) ; ver = split(ver, n, ".") ; print n[2]}'`
    if [[ (($G_VER_1 -eq 4 && $G_VER_2 -lt 7)) || (($G_VER_1 -lt 4)) ]]; then
        echo "Please upgrade g++ to 4.7 or later."
        exit 1
    fi
else
    install_gpp
fi

echo OK
