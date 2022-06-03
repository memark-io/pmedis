# Copyright 2022 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash
pushd kvdk
rm -rf build && mkdir -p build && pushd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DCHECK_CPP_STYLE=OFF -DBUILD_TESTING=OFF && make -j`nproc` && popd
popd
rm -rf build && mkdir -p build && pushd build
cmake -DCMAKE_BUILD_TYPE=Release .. && make -j`nproc` && popd
rm -rf test/*.so && cp kvdk/build/*.so test/ && cp build/lib/*.so test/
