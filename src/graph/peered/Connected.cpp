/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "../Shard.h"

namespace ragedb {

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(const std::string& type1, const std::string& key1, const std::string& type2, const std::string& key2) {
    uint16_t shard_id1 = CalculateShardId(type1, key1);
    uint16_t shard_id2 = CalculateShardId(type2, key2);

    // Shortcut if the shards are the same
    if (shard_id1 == shard_id2) {
      return container().invoke_on(shard_id1, [type1, key1, type2, key2, this](Shard &local_shard) {
        uint64_t node_id2 = local_shard.NodeGetID(type2, key2);
        std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2);
        return RelationshipsGetPeered(links);
      });

    }
    // Nodes are on different Shards, so get the node id first, then check for it
    return container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) { return local_shard.NodeGetID(type2, key2); }).then([this, shard_id1, type1, key1](uint64_t node_id2) {
      return container().invoke_on(shard_id1, [type1, key1, node_id2, this](Shard &local_shard) {
        std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2);
        return RelationshipsGetPeered(links);
      });
    });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(const std::string& type1, const std::string& key1, const std::string& type2, const std::string& key2, const std::string& rel_type) {
    uint16_t shard_id1 = CalculateShardId(type1, key1);
    uint16_t shard_id2 = CalculateShardId(type2, key2);

    // Shortcut if the shards are the same
    if (shard_id1 == shard_id2) {

      return container().invoke_on(shard_id1, [type1, key1, type2, key2, rel_type, this](Shard &local_shard) {
        uint64_t node_id2 = local_shard.NodeGetID(type2, key2);
        std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2, BOTH, rel_type);
        return RelationshipsGetPeered(links);
      });

    }
    // Nodes are on different Shards, so get the node id first, then check for it
    return container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                        return local_shard.NodeGetID(type2, key2);
                      }).then([this, shard_id1, type1, key1, rel_type](uint64_t node_id2) {
        return container().invoke_on(shard_id1, [type1, key1, node_id2, rel_type, this](Shard &local_shard) {
          std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2, BOTH, rel_type);
          return RelationshipsGetPeered(links);
        });
      });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(const std::string& type1, const std::string& key1, const std::string& type2, const std::string& key2, uint16_t type_id) {
    uint16_t shard_id1 = CalculateShardId(type1, key1);
    uint16_t shard_id2 = CalculateShardId(type2, key2);

    // Shortcut if the shards are the same
    if (shard_id1 == shard_id2) {

      return container().invoke_on(shard_id1, [type1, key1, type2, key2, type_id, this](Shard &local_shard) {
        uint64_t node_id2 = local_shard.NodeGetID(type2, key2);
        std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2, BOTH, type_id);
        return RelationshipsGetPeered(links);
      });

    }
    // Nodes are on different Shards, so get the node id first, then check for it
    return container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                        return local_shard.NodeGetID(type2, key2);
                      }).then([this, shard_id1, type1, key1, type_id](uint64_t node_id2) {
        return container().invoke_on(shard_id1, [type1, key1, node_id2, type_id, this](Shard &local_shard) {
          std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2, BOTH, type_id);
          return RelationshipsGetPeered(links);
        });
      });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(const std::string& type1, const std::string& key1, const std::string& type2, const std::string& key2, const std::vector<std::string> &rel_types) {
    uint16_t shard_id1 = CalculateShardId(type1, key1);
    uint16_t shard_id2 = CalculateShardId(type2, key2);

    // Shortcut if the shards are the same
    if (shard_id1 == shard_id2) {

      return container().invoke_on(shard_id1, [type1, key1, type2, key2, rel_types, this](Shard &local_shard) {
        uint64_t node_id2 = local_shard.NodeGetID(type2, key2);
        std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2, BOTH, rel_types);
        return RelationshipsGetPeered(links);
      });

    }
    // Nodes are on different Shards, so get the node id first, then check for it
    return container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                        return local_shard.NodeGetID(type2, key2);
                      }).then([this, shard_id1, type1, key1, rel_types](uint64_t node_id2) {
        return container().invoke_on(shard_id1, [type1, key1, node_id2, rel_types, this](Shard &local_shard) {
          std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2, BOTH, rel_types);
          return RelationshipsGetPeered(links);
        });
      });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(const std::string& type1, const std::string& key1, const std::string& type2, const std::string& key2, Direction direction) {
    uint16_t shard_id1 = CalculateShardId(type1, key1);
    uint16_t shard_id2 = CalculateShardId(type2, key2);

    // Shortcut if the shards are the same
    if (shard_id1 == shard_id2) {

      return container().invoke_on(shard_id1, [type1, key1, type2, key2, direction, this](Shard &local_shard) {
        uint64_t node_id2 = local_shard.NodeGetID(type2, key2);
        std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2, direction);
        return RelationshipsGetPeered(links);
      });

     }
    // Nodes are on different Shards, so get the node id first, then check for it
    return container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
             return local_shard.NodeGetID(type2, key2);
           }).then([this, shard_id1, type1, key1, direction](uint64_t node_id2) {
      return container().invoke_on(shard_id1, [type1, key1, node_id2, direction, this](Shard &local_shard) {
        std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2, direction);
        return RelationshipsGetPeered(links);
      });
    });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(const std::string& type1, const std::string& key1, const std::string& type2, const std::string& key2, Direction direction, const std::string& rel_type) {
    uint16_t shard_id1 = CalculateShardId(type1, key1);
    uint16_t shard_id2 = CalculateShardId(type2, key2);

    // Shortcut if the shards are the same
    if (shard_id1 == shard_id2) {

      return container().invoke_on(shard_id1, [type1, key1, type2, key2, direction, rel_type, this](Shard &local_shard) {
        uint64_t node_id2 = local_shard.NodeGetID(type2, key2);
        std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2, direction, rel_type);
        return RelationshipsGetPeered(links);
      });

    }
    // Nodes are on different Shards, so get the node id first, then check for it
    return container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                        return local_shard.NodeGetID(type2, key2);
                      }).then([this, shard_id1, type1, key1, direction, rel_type](uint64_t node_id2) {
        return container().invoke_on(shard_id1, [type1, key1, node_id2, direction, rel_type, this](Shard &local_shard) {
          std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2, direction, rel_type);
          return RelationshipsGetPeered(links);
        });
      });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(const std::string& type1, const std::string& key1, const std::string& type2, const std::string& key2, Direction direction, uint16_t type_id) {
    uint16_t shard_id1 = CalculateShardId(type1, key1);
    uint16_t shard_id2 = CalculateShardId(type2, key2);

    // Shortcut if the shards are the same
    if (shard_id1 == shard_id2) {

      return container().invoke_on(shard_id1, [type1, key1, type2, key2, direction, type_id, this](Shard &local_shard) {
        uint64_t node_id2 = local_shard.NodeGetID(type2, key2);
        std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2, direction, type_id);
        return RelationshipsGetPeered(links);
      });

    }
    // Nodes are on different Shards, so get the node id first, then check for it
    return container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                        return local_shard.NodeGetID(type2, key2);
                      }).then([this, shard_id1, type1, key1, direction, type_id](uint64_t node_id2) {
        return container().invoke_on(shard_id1, [type1, key1, node_id2, direction, type_id, this](Shard &local_shard) {
          std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2, direction, type_id);
          return RelationshipsGetPeered(links);
        });
      });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(const std::string& type1, const std::string& key1, const std::string& type2, const std::string& key2, Direction direction, const std::vector<std::string> &rel_types) {
    uint16_t shard_id1 = CalculateShardId(type1, key1);
    uint16_t shard_id2 = CalculateShardId(type2, key2);

    // Shortcut if the shards are the same
    if (shard_id1 == shard_id2) {

      return container().invoke_on(shard_id1, [type1, key1, type2, key2, direction, rel_types, this](Shard &local_shard) {
        uint64_t node_id2 = local_shard.NodeGetID(type2, key2);
        std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2, direction, rel_types);
        return RelationshipsGetPeered(links);
      });

    }
    // Nodes are on different Shards, so get the node id first, then check for it
    return container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                        return local_shard.NodeGetID(type2, key2);
                      }).then([this, shard_id1, type1, key1, direction, rel_types](uint64_t node_id2) {
        return container().invoke_on(shard_id1, [type1, key1, node_id2, direction, rel_types, this](Shard &local_shard) {
          std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(type1, key1, node_id2, direction, rel_types);
          return RelationshipsGetPeered(links);
        });
      });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(uint64_t id, uint64_t id2) {
    uint16_t shard_id1 = CalculateShardId(id);

    return container().invoke_on(shard_id1, [id, id2, this](Shard &local_shard) {
      std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(id, id2);
      return RelationshipsGetPeered(links);
    });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(uint64_t id, uint64_t id2, const std::string& rel_type) {
    uint16_t shard_id1 = CalculateShardId(id);

    return container().invoke_on(shard_id1, [id, id2, rel_type, this](Shard &local_shard) {
      std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(id, id2, BOTH, rel_type);
      return RelationshipsGetPeered(links);
    });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(uint64_t id, uint64_t id2, uint16_t type_id) {
    uint16_t shard_id1 = CalculateShardId(id);

    return container().invoke_on(shard_id1, [id, id2, type_id, this](Shard &local_shard) {
      std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(id, id2, BOTH, type_id);
      return RelationshipsGetPeered(links);
    });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(uint64_t id, uint64_t id2, const std::vector<std::string> &rel_types) {
    uint16_t shard_id1 = CalculateShardId(id);

    return container().invoke_on(shard_id1, [id, id2, rel_types, this](Shard &local_shard) {
      std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(id, id2, BOTH, rel_types);
      return RelationshipsGetPeered(links);
    });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(uint64_t id, uint64_t id2, Direction direction) {
    uint16_t shard_id1 = CalculateShardId(id);

    return container().invoke_on(shard_id1, [id, id2, direction, this](Shard &local_shard) {
      std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(id, id2, direction);
      return RelationshipsGetPeered(links);
    });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(uint64_t id, uint64_t id2, Direction direction, const std::string& rel_type) {
    uint16_t shard_id1 = CalculateShardId(id);

    return container().invoke_on(shard_id1, [id, id2, direction, rel_type, this](Shard &local_shard) {
      std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(id, id2, direction, rel_type);
      return RelationshipsGetPeered(links);
    });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(uint64_t id, uint64_t id2, Direction direction, uint16_t type_id) {
    uint16_t shard_id1 = CalculateShardId(id);

    return container().invoke_on(shard_id1, [id, id2, direction, type_id, this](Shard &local_shard) {
      std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(id, id2, direction, type_id);
      return RelationshipsGetPeered(links);
    });

  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetConnectedPeered(uint64_t id, uint64_t id2, Direction direction, const std::vector<std::string> &rel_types) {
    uint16_t shard_id1 = CalculateShardId(id);

    return container().invoke_on(shard_id1, [id, id2, direction, rel_types, this](Shard &local_shard) {
      std::vector<Link> links = local_shard.NodeGetRelationshipsIDs(id, id2, direction, rel_types);
      return RelationshipsGetPeered(links);
    });

  }

}
