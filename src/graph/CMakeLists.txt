set(HEADER_FILES
        Graph.h
        Shard.h
        Types.h)

set(SOURCE_FILES
        Graph.cpp
        Shard.cpp
        Types.cpp)

add_library(Graph ${SOURCE_FILES} ${HEADER_FILES})