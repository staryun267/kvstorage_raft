get_filename_component(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}" ABSOLUTE)

aux_source_directory(${SRC_DIR} SRC_LIST)

add_library(fiber_lib ${SRC_LIST})
target_link_libraries(fiber_lib -ldl)
set(src_fiber ${SRC_LIST} CACHE INTERNAL "Description of the variable")