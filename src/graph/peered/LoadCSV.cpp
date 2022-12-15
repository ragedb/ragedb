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
#include <csv.hpp>
#include "../Shard.h"
#include "../../Templates.h"

namespace ragedb {

    /*
     * Schema must already exist
     * Node Type or Relationship Type is a parameter
     * ":" is a separator
     * <column name>:key indicates optional column name for property
     * start_key:NODE_TYPE, end_key:NODE_TYPE are for relationships
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
     *   Frozen Foods, 2022-12-05, Ice Cream



     */

    std::pair<std::string, std::vector<std::string>> Shard::GetToKeysFromRelationshipsInCSV(const std::string& filename, const char csv_separator) {
        rapidcsv::Document doc(filename, rapidcsv::LabelParams(), rapidcsv::SeparatorParams(csv_separator, true),
          rapidcsv::ConverterParams(),
          rapidcsv::LineReaderParams(true /* pSkipCommentLines */,
            '#' /* pCommentPrefix */,
            true /* pSkipEmptyLines */));

        std::string key_column;
        std::string type;
        for (const auto& name : doc.GetColumnNames()) {
            if (name.starts_with("end_key:")) {
                key_column = name;
                type = name.substr(name.find(":") + 1);
                break;
            }
        }

        // If we found our key column and type
        if (!key_column.empty() && !type.empty()) {
            return std::make_pair(type, doc.GetColumn<std::string>(key_column));
        }
        return std::make_pair("", std::vector<std::string>());
    }

    std::map<uint16_t, std::vector<size_t>> Shard::PartitionRelationshipsInCSV(const std::string& filename, const char csv_separator) {
        std::map<uint16_t, std::vector<size_t>> sharded_nodes;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes.try_emplace(i);
        }

        rapidcsv::Document doc(filename, rapidcsv::LabelParams(), rapidcsv::SeparatorParams(csv_separator, true),
          rapidcsv::ConverterParams(),
          rapidcsv::LineReaderParams(true /* pSkipCommentLines */,
            '#' /* pCommentPrefix */,
            true /* pSkipEmptyLines */));

        std::string key_column;
        std::string type;
        for (const auto&name : doc.GetColumnNames()) {
            if (name.starts_with("start_key:")) {
                key_column = name;
                type = name.substr(name.find(":") + 1);
                break;
            }
        }

        // If we found our key column and type
        if (!key_column.empty() && !type.empty()) {
            // Distribute the rows over the shards
            size_t row = 0;
            for (const auto& key : doc.GetColumn<std::string>(key_column)) {
                sharded_nodes[CalculateShardId(type, key)].emplace_back(row++);
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if (sharded_nodes.at(i).empty()) {
                sharded_nodes.erase(i);
            }
        }

        return sharded_nodes;
    }

    std::map<uint16_t, std::vector<size_t>> Shard::PartitionNodesInCSV(const std::string& type, const std::string& filename, const char csv_separator) {
        std::map<uint16_t, std::vector<size_t>> sharded_nodes;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes.try_emplace(i);
        }

        rapidcsv::Document doc(filename, rapidcsv::LabelParams(), rapidcsv::SeparatorParams(csv_separator, true),
          rapidcsv::ConverterParams(),
          rapidcsv::LineReaderParams(true /* pSkipCommentLines */,
            '#' /* pCommentPrefix */,
            true /* pSkipEmptyLines */));

        std::string key_column;
        for (const auto& name : doc.GetColumnNames()) {
            if (name == "key" || name.ends_with(":key")) {
                key_column = name;
                break;
            }
        }

        // If we found our key column
        if (!key_column.empty()) {
            // Distribute the rows over the shards
            size_t row = 0;
            for (const auto& key : doc.GetColumn<std::string>(key_column)) {
                sharded_nodes[CalculateShardId(type, key)].emplace_back(row++);
            }
        } else {
            auto max_rows = doc.GetRowCount();
            for (int row = 0; row < max_rows; row++) {
                sharded_nodes[CalculateShardId(type, std::to_string(row))].emplace_back(row);
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if (sharded_nodes.at(i).empty()) {
                sharded_nodes.erase(i);
            }
        }

        return sharded_nodes;
    }

    std::map<uint16_t, std::vector<size_t>> Shard::PartitionNodesInStreamCSV(const std::string& type, const std::string& filename, const char csv_separator) {
        std::map<uint16_t, std::vector<size_t>> sharded_nodes;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes.try_emplace(i);
        }

        csv::CSVFormat format;
        format.delimiter(csv_separator).quote('"').trim({ ' ', '\t' });
        std::ifstream infile(filename, std::ios::binary);
        csv::CSVReader reader(infile, format);

        std::string key_column;

        for (const auto& name : reader.get_col_names()) {
            if (name == "key" || name.ends_with(":key")) {
                key_column = name;
                break;
            }
        }

        // If we found our key column
        if (!key_column.empty()) {
            // Distribute the rows over the shards
            size_t row = 0;
            for (csv::CSVRow& csv_row: reader) {
                std::string key = csv_row[key_column].get<>();
                sharded_nodes[CalculateShardId(type, key)].emplace_back(row++);
            }
        } else {
            size_t row = 0;
            for (csv::CSVRow& csv_row: reader) {
                sharded_nodes[CalculateShardId(type, std::to_string(row))].emplace_back(row);
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if (sharded_nodes.at(i).empty()) {
                sharded_nodes.erase(i);
            }
        }

        return sharded_nodes;
    }

    seastar::future<uint64_t> Shard::StreamCSVPeered(const std::string &type, const std::string &filename, const char csv_separator) {
        if (auto type_id = node_types.getTypeId(type); type_id > 0) {
            // We are importing Nodes
            std::map<uint16_t, std::vector<size_t>> sharded_nodes = PartitionNodesInStreamCSV(type, filename, csv_separator);

            std::vector<seastar::future<uint64_t>> futures;
            for (const auto& [their_shard, grouped_nodes] : sharded_nodes ) {
                auto future = container().invoke_on(their_shard, [grouped_nodes = grouped_nodes, type_id, filename, csv_separator] (Shard &local_shard) {
                    return local_shard.StreamCSVNodes(type_id, filename, csv_separator, grouped_nodes);
                });
                futures.push_back(std::move(future));
            }

            auto p = make_shared(std::move(futures));

            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<uint64_t>& results) {
                return std::reduce(results.begin(), results.end()); // sum the counts of the results
            });
        }
        // Return zero records loaded if failed
        return seastar::make_ready_future<uint64_t>(0);
    }

    seastar::future<uint64_t> Shard::LoadCSVPeered(const std::string &type, const std::string &filename, const char csv_separator) {
        if (auto type_id = node_types.getTypeId(type); type_id > 0) {
            // We are importing Nodes
            std::map<uint16_t, std::vector<size_t>> sharded_nodes = PartitionNodesInCSV(type, filename, csv_separator);

            std::vector<seastar::future<uint64_t>> futures;
            for (const auto& [their_shard, grouped_nodes] : sharded_nodes ) {
                auto future = container().invoke_on(their_shard, [grouped_nodes = grouped_nodes, type_id, filename, csv_separator] (Shard &local_shard) {
                    return local_shard.LoadCSVNodes(type_id, filename, csv_separator, grouped_nodes);
                });
                futures.push_back(std::move(future));
            }

            auto p = make_shared(std::move(futures));

            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<uint64_t>& results) {
                return std::reduce(results.begin(), results.end()); // sum the counts of the results
            });
        } else if (type_id = relationship_types.getTypeId(type); type_id > 0) {
            // We are importing Relationships

            // We need to get the node ids of the TO nodes from their respective shard.
            // then create the relationships in the FROM nodes in their respective shard
            // then we have to add both to the incoming node id Link group.
            // We only need to parse the first line

            std::pair<std::string, std::vector<std::string>> to_type_and_keys = GetToKeysFromRelationshipsInCSV(filename, csv_separator);

            return NodesGetIdsPeered(to_type_and_keys.first, to_type_and_keys.second).then([type_id, filename, csv_separator, this] (std::map<std::string, uint64_t> to_keys_and_ids) {
                std::map<uint16_t, std::vector<size_t>> sharded_nodes = PartitionRelationshipsInCSV(filename, csv_separator);

                std::vector<seastar::future<std::map<uint16_t, std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>>>> futures;

                for (const auto& [their_shard, grouped_nodes] : sharded_nodes ) {
                    auto future = container().invoke_on(their_shard, [grouped_nodes = grouped_nodes, type_id, filename, csv_separator, to_keys_and_ids] (Shard &local_shard) {
                        return local_shard.LoadCSVRelationships(type_id, filename, csv_separator, grouped_nodes, to_keys_and_ids);
                    });
                    futures.push_back(std::move(future));
                }

                auto p = make_shared(std::move(futures));

                return seastar::when_all_succeed(p->begin(), p->end()).then([p, type_id, this] (const std::vector<std::map<uint16_t, std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>>>& results) {
                    std::vector<seastar::future<uint64_t>> futures;

                    // Combining results so on 56 cores we create 56 futures and not 3136 of them.
                    std::map<uint16_t, std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>> sharded_relationship_tuples;
                    for (uint16_t i = 0; i < cpus; i++) {
                        sharded_relationship_tuples.try_emplace(i);
                    }
                    for (const auto& result : results) {
                        for (const auto& [their_shard, relationship_tuples] : result ) {
                            sharded_relationship_tuples.at(their_shard).insert(
                              std::end(sharded_relationship_tuples.at(their_shard)),
                              std::begin(relationship_tuples), std::end(relationship_tuples));
                        }
                    }
                    for (uint16_t i = 0; i < cpus; i++) {
                        if (sharded_relationship_tuples.at(i).empty()) {
                            sharded_relationship_tuples.erase(i);
                        }
                    }

                    for (const auto& [their_shard, relationship_tuples] : sharded_relationship_tuples ) {
                        auto future = container().invoke_on(their_shard, [type_id, relationship_tuples = relationship_tuples](Shard &local_shard) {
                            uint64_t count = 0;
                            for (const auto& tuple : relationship_tuples) {
                                local_shard.RelationshipAddToIncoming(type_id, std::get<0>(tuple), std::get<1>(tuple), std::get<2>(tuple));
                                count++;
                            }
                            return count;
                        });
                        futures.push_back(std::move(future));
                    }

                    auto p2 = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2] (const std::vector<uint64_t>& results) {
                        return std::reduce(results.begin(), results.end()); // sum the counts of the results
                    });

                });
            });

        }
        // Return zero records loaded if failed
        return seastar::make_ready_future<uint64_t>(0);
    }
}