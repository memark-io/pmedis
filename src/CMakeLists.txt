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

set(PMEDIS_SRC t_phash.c t_plist.c t_pstring.c t_pset.c t_pzset.c pmedis.c dummy.c util.c kvdkenv.c pmedis.h tpool.c)
add_library(pmedis SHARED ${PMEDIS_SRC})
target_link_directories(pmedis PUBLIC ${PROJECT_SOURCE_DIR}/kvdk/build)
target_link_libraries(pmedis pthread engine pmem gflags hwloc atomic)
