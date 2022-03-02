#!/bin/bash
pushd kvdk
rm -rf build && mkdir -p build && pushd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DCHECK_CPP_STYLE=ON -DBUILD_TESTING=OFF && make -j`nproc` && popd
popd
rm -rf build && mkdir -p build && pushd build
cmake .. && make -j`nproc` && popd
cp kvdk/build/libengine.so test/ && cp build/lib/libpmedis.so test/
