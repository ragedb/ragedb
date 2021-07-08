set(HEADER_FILES
        Graph.h
        Shard.h
        )

set(SOURCE_FILES
        Graph.cpp
        Shard.cpp
        )

add_library(Graph ${SOURCE_FILES} ${HEADER_FILES})