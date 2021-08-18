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
        Direction.h
        Operation.h
        Expression.h)

set(SOURCE_FILES
        Graph.cpp
        Shard.cpp
        Group.cpp
        Link.cpp
        Node.cpp
        Relationship.cpp
        NodeTypes.cpp
        RelationshipTypes.cpp
        Properties.cpp
        shard/Ids.cpp shard/Types.cpp shard/Nodes.cpp shard/Relationships.cpp shard/Traversing.cpp shard/All.cpp shard/Degrees.cpp shard/Find.cpp shard/Helpers.cpp
        peered/Types.cpp peered/Nodes.cpp peered/Relationships.cpp peered/Traversing.cpp peered/All.cpp peered/Degrees.cpp peered/Neighbors.cpp peered/Find.cpp
        lua/Types.cpp lua/Nodes.cpp lua/Relationships.cpp lua/Degrees.cpp lua/Traversing.cpp lua/Neighbors.cpp lua/All.cpp)

add_library(Graph ${SOURCE_FILES} ${HEADER_FILES})