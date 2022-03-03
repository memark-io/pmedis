#!/bin/bash
pushd kvdk
rm -rf build && mkdir -p build && pushd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DCHECK_CPP_STYLE=OFF -DBUILD_TESTING=OFF && make -j`nproc` && popd
popd
rm -rf build && mkdir -p build && pushd build
cmake .. && make -j`nproc` && popd
rm -rf test/*.so && cp kvdk/build/*.so test/ && cp build/lib/*.so test/
