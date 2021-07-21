set(HEADER_FILES
        Graph.h
        Shard.h
        Group.h
        Link.h
        Node.h
        Relationship.h
        NodeTypes.h
        RelationshipTypes.h
        Properties.h
        Direction.h)

set(SOURCE_FILES
        Graph.cpp
        Shard.cpp
        Group.cpp
        Link.cpp
        Node.cpp
        Relationship.cpp
        NodeTypes.cpp
        RelationshipTypes.cpp
        Properties.cpp )

add_library(Graph ${SOURCE_FILES} ${HEADER_FILES})