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
        PropertyType.h
        Direction.h
        Operation.h
        Expression.h
        Lua.h
        Roar.h
        eve/CollectIndexes.h)

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
        Roar.cpp
        node/Find.cpp relationship/Find.cpp shard/Find.cpp peered/Find.cpp lua/Find.cpp
        node/Filter.cpp relationship/Filter.cpp shard/Filter.cpp peered/Filter.cpp lua/Filter.cpp
        shard/Ids.cpp shard/Types.cpp shard/Node.cpp shard/Nodes.cpp shard/Relationship.cpp shard/Relationships.cpp shard/Traversing.cpp shard/All.cpp shard/Degrees.cpp shard/Helpers.cpp shard/Bulk.cpp
        peered/Types.cpp peered/Node.cpp peered/Nodes.cpp peered/Relationship.cpp peered/Relationships.cpp peered/Traversing.cpp peered/All.cpp peered/Degrees.cpp peered/Neighbors.cpp peered/Bulk.cpp peered/Restore.cpp peered/Connected.cpp peered/Helpers.cpp
        lua/Types.cpp lua/Node.cpp lua/Nodes.cpp lua/Relationship.cpp lua/Relationships.cpp lua/Degrees.cpp lua/Traversing.cpp lua/Neighbors.cpp lua/All.cpp lua/Bulk.cpp lua/Helpers.cpp
        )

add_library(Graph ${SOURCE_FILES} ${HEADER_FILES})