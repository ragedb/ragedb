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

    std::map<uint16_t, std::vector<Link>> Shard::PartitionLinksByNodeShardId(std::vector<Link> &links) const {
      std::map<uint16_t, std::vector<Link>> sharded_links;
      for (int i = 0; i < cpus; i++) {
        sharded_links.insert({i, std::vector<Link>() });
      }

      for (Link link : links) {
        uint16_t node_shard_id = CalculateShardId(link.node_id);
        sharded_links.at(node_shard_id).push_back(link);
      }

      for (int i = 0; i < cpus; i++) {
        if (sharded_links.at(i).empty()) {
          sharded_links.erase(i);
        }
      }
      return sharded_links;
    }

    seastar::future<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIDsPeered(std::vector<Link> links) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByNodeShardId(links);

      std::vector<seastar::future<std::map<Link, std::vector<Link>>>> futures;
      for (auto const &[their_shard, grouped_links] : sharded_links) {
        auto future = container().invoke_on(their_shard, [grouped_links = grouped_links](Shard &local_shard) {
          return local_shard.LinksGetRelationshipsIDs(grouped_links);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<Link, std::vector<Link>>> &results) {
        std::map<Link, std::vector<Link>> combined;

        for (const std::map<Link, std::vector<Link>> &sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });

    }

    seastar::future<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIDsPeered(std::vector<Link> links, Direction direction) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByNodeShardId(links);

      std::vector<seastar::future<std::map<Link, std::vector<Link>>>> futures;
      for (auto const &[their_shard, grouped_links] : sharded_links) {
        auto future = container().invoke_on(their_shard, [grouped_links = grouped_links, direction](Shard &local_shard) {
          return local_shard.LinksGetRelationshipsIDs(grouped_links, direction);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<Link, std::vector<Link>>> &results) {
        std::map<Link, std::vector<Link>> combined;

        for (const std::map<Link, std::vector<Link>> &sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIDsPeered(std::vector<Link> links, Direction direction, const std::string &rel_type) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByNodeShardId(links);

      std::vector<seastar::future<std::map<Link, std::vector<Link>>>> futures;
      for (auto const &[their_shard, grouped_links] : sharded_links) {
        auto future = container().invoke_on(their_shard, [grouped_links = grouped_links, direction, rel_type](Shard &local_shard) {
          return local_shard.LinksGetRelationshipsIDs(grouped_links, direction, rel_type);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<Link, std::vector<Link>>> &results) {
        std::map<Link, std::vector<Link>> combined;

        for (const std::map<Link, std::vector<Link>> &sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIDsPeered(std::vector<Link> links, Direction direction, uint16_t type_id) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByNodeShardId(links);

      std::vector<seastar::future<std::map<Link, std::vector<Link>>>> futures;
      for (auto const &[their_shard, grouped_links] : sharded_links) {
        auto future = container().invoke_on(their_shard, [grouped_links = grouped_links, direction, type_id](Shard &local_shard) {
          return local_shard.LinksGetRelationshipsIDs(grouped_links, direction, type_id);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<Link, std::vector<Link>>> &results) {
        std::map<Link, std::vector<Link>> combined;

        for (const std::map<Link, std::vector<Link>> &sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIDsPeered(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByNodeShardId(links);

      std::vector<seastar::future<std::map<Link, std::vector<Link>>>> futures;
      for (auto const &[their_shard, grouped_links] : sharded_links) {
        auto future = container().invoke_on(their_shard, [grouped_links = grouped_links, direction, rel_types](Shard &local_shard) {
          return local_shard.LinksGetRelationshipsIDs(grouped_links, direction, rel_types);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<Link, std::vector<Link>>> &results) {
        std::map<Link, std::vector<Link>> combined;

        for (const std::map<Link, std::vector<Link>> &sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIDsPeered(std::vector<Link> links, const std::string &rel_type) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByNodeShardId(links);

      std::vector<seastar::future<std::map<Link, std::vector<Link>>>> futures;
      for (auto const &[their_shard, grouped_links] : sharded_links) {
        auto future = container().invoke_on(their_shard, [grouped_links = grouped_links](Shard &local_shard) {
          return local_shard.LinksGetRelationshipsIDs(grouped_links, Direction::BOTH);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<Link, std::vector<Link>>> &results) {
        std::map<Link, std::vector<Link>> combined;

        for (const std::map<Link, std::vector<Link>> &sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIDsPeered(std::vector<Link> links, uint16_t type_id) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByNodeShardId(links);

      std::vector<seastar::future<std::map<Link, std::vector<Link>>>> futures;
      for (auto const &[their_shard, grouped_links] : sharded_links) {
        auto future = container().invoke_on(their_shard, [grouped_links = grouped_links, type_id](Shard &local_shard) {
          return local_shard.LinksGetRelationshipsIDs(grouped_links, Direction::BOTH, type_id);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<Link, std::vector<Link>>> &results) {
        std::map<Link, std::vector<Link>> combined;

        for (const std::map<Link, std::vector<Link>> &sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIDsPeered(std::vector<Link> links, const std::vector<std::string> &rel_types) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByNodeShardId(links);

      std::vector<seastar::future<std::map<Link, std::vector<Link>>>> futures;
      for (auto const &[their_shard, grouped_links] : sharded_links) {
        auto future = container().invoke_on(their_shard, [grouped_links = grouped_links, rel_types](Shard &local_shard) {
          return local_shard.LinksGetRelationshipsIDs(grouped_links, Direction::BOTH, rel_types);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<Link, std::vector<Link>>> &results) {
        std::map<Link, std::vector<Link>> combined;

        for (const std::map<Link, std::vector<Link>> &sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

//    seastar::future<std::vector<std::vector<Relationship>>> Shard::LinksGetRelationshipsPeered(std::vector<Link> links)
//    {
//      return seastar::future<std::vector<std::vector<Relationship>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Relationship>>> Shard::LinksGetRelationshipsPeered(std::vector<Link> links, Direction direction)
//    {
//      return seastar::future<std::vector<std::vector<Relationship>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Relationship>>> Shard::LinksGetRelationshipsPeered(std::vector<Link> links, Direction direction, const std::string &rel_type)
//    {
//      return seastar::future<std::vector<std::vector<Relationship>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Relationship>>> Shard::LinksGetRelationshipsPeered(std::vector<Link> links, Direction direction, uint16_t type_id)
//    {
//      return seastar::future<std::vector<std::vector<Relationship>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Relationship>>> Shard::LinksGetRelationshipsPeered(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types)
//    {
//      return seastar::future<std::vector<std::vector<Relationship>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Relationship>>> Shard::LinksGetRelationshipsPeered(std::vector<Link> links, const std::string &rel_type)
//    {
//      return seastar::future<std::vector<std::vector<Relationship>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Relationship>>> Shard::LinksGetRelationshipsPeered(std::vector<Link> links, uint16_t type_id)
//    {
//      return seastar::future<std::vector<std::vector<Relationship>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Relationship>>> Shard::LinksGetRelationshipsPeered(std::vector<Link> links, const std::vector<std::string> &rel_types)
//    {
//      return seastar::future<std::vector<std::vector<Relationship>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Node>>> Shard::LinksGetNeighborsPeered(std::vector<Link> links)
//    {
//      return seastar::future<std::vector<std::vector<Node>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Node>>> Shard::LinksGetNeighborsPeered(std::vector<Link> links, Direction direction)
//    {
//      return seastar::future<std::vector<std::vector<Node>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Node>>> Shard::LinksGetNeighborsPeered(std::vector<Link> links, Direction direction, const std::string &rel_type)
//    {
//      return seastar::future<std::vector<std::vector<Node>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Node>>> Shard::LinksGetNeighborsPeered(std::vector<Link> links, Direction direction, uint16_t type_id)
//    {
//      return seastar::future<std::vector<std::vector<Node>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Node>>> Shard::LinksGetNeighborsPeered(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types)
//    {
//      return seastar::future<std::vector<std::vector<Node>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Node>>> Shard::LinksGetNeighborsPeered(std::vector<Link> links, const std::string &rel_type)
//    {
//      return seastar::future<std::vector<std::vector<Node>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Node>>> Shard::LinksGetNeighborsPeered(std::vector<Link> links, uint16_t type_id)
//    {
//      return seastar::future<std::vector<std::vector<Node>>>(seastar::future());
//    }
//    seastar::future<std::vector<std::vector<Node>>> Shard::LinksGetNeighborsPeered(std::vector<Link> links, const std::vector<std::string> &rel_types)
//    {
//      return seastar::future<std::vector<std::vector<Node>>>(seastar::future());
//    }
}