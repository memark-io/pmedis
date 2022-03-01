set(PMEDIS_SRC pmedis.c)
add_library(pmedis SHARED ${PMEDIS_SRC})
set_target_properties(pmedis PROPERTIES LINK_LIBRARIES -pthread)
