#!/bin/bash
git submodule update --init --recursive
rm -rf build && mkdir -p build && pushd build
cmake ..
make
popd
cp build/lib/libpmedis.so test/
