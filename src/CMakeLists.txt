set(PMEDIS_SRC t_pstring.c pmedis.c dummy.c util.c kvdkenv.c pmedis.h tpool.c)
add_library(pmedis SHARED ${PMEDIS_SRC})
target_link_directories(pmedis PUBLIC ${PROJECT_SOURCE_DIR}/kvdk/build)
target_link_libraries(pmedis pthread engine pmem gflags hwloc atomic)
