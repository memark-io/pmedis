set(PMEDIS_SRC pmedis.c dummy.c kvdkenv.c pmedis.h)
add_library(pmedis SHARED ${PMEDIS_SRC})
target_link_directories(pmedis PUBLIC ${PROJECT_SOURCE_DIR}/kvdk/build)
target_link_libraries(pmedis pthread engine pmem gflags hwloc atomic)
