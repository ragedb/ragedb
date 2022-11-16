/*
 * Copyright Max De Marzi. All Rights Reserved.
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

#include <rapidcsv.h>
#include "../Shard.h"

namespace ragedb {

    /*
     * Schema must already exist
     * KEY(<type>) indicates Node Type and column to use for Key
     * ":" is a separator
     * <column name>:KEY indicates optional column name for property
     *
     * Supported files:
     * Nodes:
     *
     *   [no key column, using row_id]
     *   name
     *   Chocolate
     *   Milk

     *   [has a key field, but not added as a property]
     *   key, name
     *   3, Chocolate
     *   4, Milk

     *   [has a key field, and is added as a property]
     *   product_id:key, name
     *   3, Chocolate
     *   4, Milk

     *   [allow you to ignore columns]
     *   product_id:key, name, age:IGNORE
     *   3, Chocolate, 87
     *   4, Milk, 9877
     *
     *   Relationships:
     *   start_key:Aisle, date, end_key:Product



     */

    seastar::future<uint64_t> Shard::LoadCSVPeered(const std::string &type, const std::string &filename) {
        if (auto type_id = node_types.getTypeId(type); type_id > 0) {
            // We are importing Nodes
            std::map<uint16_t, std::vector<size_t>> sharded_nodes = PartitionNodesInCSV(type, filename);

            std::vector<seastar::future<uint64_t>> futures;
            for (auto const& [their_shard, grouped_nodes] : sharded_nodes ) {
                auto future = container().invoke_on(their_shard, [grouped_nodes = grouped_nodes, type_id, filename] (Shard &local_shard) {
                    return local_shard.LoadCSVNodes(type_id, filename, grouped_nodes);
                });
                futures.push_back(std::move(future));
            }

            auto p = make_shared(std::move(futures));

            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<uint64_t>& results) {
                return std::reduce(results.begin(), results.end()); // sum the counts of the results
            });

        } else if (type_id = relationship_types.getTypeId(type); type_id > 0) {
            // We are importing Relationships

        }
        // Return zero records loaded if failed
        return seastar::make_ready_future<uint64_t>(0);
    }
}