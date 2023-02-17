set(HEADER_FILES
        Direction.h
        Expression.h
        Graph.h
        Group.h
        Link.h
        Lua.h
        Node.h
        NodeTypes.h
        Operation.h
        Order.h
        Permission.h
        Properties.h
        PropertyType.h
        Relationship.h
        RelationshipTypes.h
        Roar.h
        Sandbox.h
        Shard.h
        Uniqueness.h
        types/Date.h
        eve/CollectIndexes.h Sort.h ../Templates.h
        paths/WeightedLink.h)

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
        Sandbox.cpp
        Load.cpp
        lua/All.cpp peered/All.cpp shard/All.cpp
        lua/Bulk.cpp peered/Bulk.cpp shard/Bulk.cpp
        lua/Connected.cpp peered/Connected.cpp # no shard
        lua/Degrees.cpp peered/Degrees.cpp shard/Degrees.cpp
        lua/Difference.cpp peered/Difference.cpp # no shard
        lua/Filter.cpp peered/Filter.cpp shard/Filter.cpp
        lua/Find.cpp peered/Find.cpp shard/Find.cpp
        lua/Helpers.cpp peered/Helpers.cpp shard/Helpers.cpp
        lua/Intersect.cpp peered/Intersect.cpp # no shard
        lua/Neighbors.cpp peered/Neighbors.cpp shard/Neighbors.cpp
        lua/Node.cpp peered/Node.cpp shard/Node.cpp
        lua/Nodes.cpp peered/Nodes.cpp shard/Nodes.cpp
        lua/Relationship.cpp peered/Relationship.cpp shard/Relationship.cpp
        lua/Relationships.cpp peered/Relationships.cpp shard/Relationships.cpp
        lua/Traversing.cpp peered/Traversing.cpp shard/Traversing.cpp
        lua/Types.cpp peered/Types.cpp shard/Types.cpp
        lua/LoadCSV.cpp peered/LoadCSV.cpp shard/LoadCSV.cpp

        node/Find.cpp relationship/Find.cpp
        node/Filter.cpp relationship/Filter.cpp

        shard/Ids.cpp
        peered/Restore.cpp
        types/Date.cpp shard/NodeLinks.cpp lua/KHop.cpp peered/KHop.cpp
        paths/WeightedLink.cpp)

add_library(Graph ${SOURCE_FILES} ${HEADER_FILES})