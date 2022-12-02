# Garner

Hierarchical validation in Silo-flavor OCC on a B+-tree index.

## Build

Install dependencies and `gcc` 11.x for full C++-20 support:

```bash
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt update
sudo apt upgrade

sudo apt install build-essential gcc-11 g++-11 cpp-11 cmake clang-format

sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 100
sudo update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-11 100
```

TODO
