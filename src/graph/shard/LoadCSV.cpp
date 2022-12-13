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
#include "../types/Date.h"

namespace ragedb {

    std::string getPropertyName(std::string const& column_name) {
        return column_name.substr(0, column_name.find(":"));
    }

    std::string getTypeName(std::string const& column_name) {
        return column_name.substr(column_name.find(":") + 1);
    }


    seastar::future<std::map<uint16_t, std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>>> Shard::LoadCSVRelationships(uint16_t rel_type_id, const std::string &filename, const char csv_separator, const std::vector<size_t> rows, std::map<std::string, uint64_t> to_keys_and_ids) {
        std::map<uint16_t, std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>> sharded_relationships;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_relationships.try_emplace(i);
        }

        rapidcsv::Document doc(filename, rapidcsv::LabelParams(), rapidcsv::SeparatorParams(csv_separator, true),
          rapidcsv::ConverterParams(),
          rapidcsv::LineReaderParams(true /* pSkipCommentLines */,
            '#' /* pCommentPrefix */,
            true /* pSkipEmptyLines */));

        auto allowed_node_types = node_types.getTypes();
        auto allowed_property_names_and_types = relationship_types.getProperties(rel_type_id).getPropertyTypes();
        std::map<size_t, std::pair<std::string, uint8_t>> column_idx_property_name_and_type;
        std::string from_type;
        std::string to_type;
        size_t from_key_column_idx = -1;
        size_t to_key_column_idx = -1;

        for (auto &column_name : doc.GetColumnNames()) {
            if (column_name.ends_with("ignore")) {
                continue ;
            }

            if (column_name.starts_with("start_key:")) {
                from_key_column_idx = doc.GetColumnIdx(column_name);
                from_type = getTypeName(column_name);
                if (allowed_node_types.contains(from_type)) {
                    continue;
                }
            }

            if (column_name.starts_with("end_key:")) {
                to_key_column_idx = doc.GetColumnIdx(column_name);
                to_type = getTypeName(column_name);
                if (allowed_node_types.contains(to_type)) {
                    continue;
                }
            }

            if (allowed_property_names_and_types.contains(column_name)) {
                column_idx_property_name_and_type.emplace(doc.GetColumnIdx(column_name), std::make_pair(column_name, relationship_types.getProperties(rel_type_id).getPropertyTypeId(column_name)) );
            }
        }

        if (!from_type.empty() && !to_type.empty()) {
            uint16_t id1_type_id = NodeTypeGetTypeId(from_type);
            auto &properties = relationship_types.getProperties(rel_type_id);
            for (auto row_id : rows) {
                co_await seastar::coroutine::maybe_yield();
                std::string key1 = doc.GetCell<std::string>(from_key_column_idx, row_id);
                std::string key2 = doc.GetCell<std::string>(to_key_column_idx, row_id);
                uint64_t id1 = NodeGetID(from_type, key1);
                uint64_t id2 = to_keys_and_ids.at(key2);

                if (id1 > 0 && id2 > 0) {
                    uint64_t internal_id1 = externalToInternal(id1);
                    uint64_t external_id = 0;

                    uint64_t internal_id = relationship_types.getCount(rel_type_id);
                    if(relationship_types.hasDeleted(rel_type_id)) {
                        // If we have deleted relationships, fill in the space by reusing the new relationship
                        internal_id = relationship_types.getDeletedIdsMinimum(rel_type_id);
                        relationship_types.setStartingNodeId(rel_type_id, internal_id, id1);
                        relationship_types.setEndingNodeId(rel_type_id, internal_id, id2);
                    } else {
                        relationship_types.getStartingNodeIds(rel_type_id).emplace_back(id1);
                        relationship_types.getEndingNodeIds(rel_type_id).emplace_back(id2);
                    }

                    relationship_types.addId(rel_type_id, internal_id);

                    for (auto & [column_idx, name_type_pair] : column_idx_property_name_and_type) {
                        switch (name_type_pair.second) {
                        case Properties::boolean_type: {
                            properties.setBooleanProperty(name_type_pair.first, internal_id, doc.GetCell<bool>(column_idx, row_id));
                            break;
                        }
                        case Properties::integer_type: {
                            properties.setIntegerProperty(name_type_pair.first, internal_id, doc.GetCell<int64_t>(column_idx, row_id));
                            break;
                        }
                        case Properties::double_type: {
                            properties.setDoubleProperty(name_type_pair.first, internal_id, doc.GetCell<double>(column_idx, row_id));
                            break;
                        }
                        case Properties::string_type: {
                            properties.setStringProperty(name_type_pair.first, internal_id, doc.GetCell<std::string>(column_idx, row_id));
                            break;
                        }
                        case Properties::date_type: {
                            properties.setDateProperty(name_type_pair.first, internal_id, Date::convert(doc.GetCell<std::string>(column_idx, row_id))); // Date are stored as doubles
                            break;
                        }
                            // TODO: Find a way to import lists... maybe get as string, then split by semicolon, then convert from string to appropriate type
                            //                        case Properties::boolean_list_type: properties.setListOfBooleanProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<bool>>(column_idx, row_id));
                            //                        case Properties::integer_list_type: properties.setListOfIntegerProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<int64_t>>(column_idx, row_id));
                            //                        case Properties::double_list_type: properties.setListOfDoubleProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<double>>(column_idx, row_id));
                            //                        case Properties::string_list_type: properties.setListOfStringProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<std::string>>(column_idx, row_id));
                            //                        case Properties::date_list_type: properties.setListOfDateProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<double>>(column_idx, row_id)); // Lists of Dates are stored as Lists of Doubles
                        default: break;
                        }

                    }

                    external_id = internalToExternal(rel_type_id, internal_id);

                    // Add the relationship to the outgoing node
                    auto group = std::ranges::find_if(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1),
                      [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
                    // See if the relationship type is already there
                    if (group != std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1))) {
                        group->links.emplace_back(id2, external_id);
                        // TODO - Consider: insert_sorted(id2, external_id, group->links);
                    } else {
                        // otherwise create a new type with the links
                        node_types.getOutgoingRelationships(id1_type_id).at(internal_id1).emplace_back(rel_type_id, std::vector<Link>({Link(id2, external_id)}));
                    }

                    sharded_relationships[CalculateShardId(id2)].emplace_back(external_id, id1, id2);
                }
            }
        }


        for (uint16_t i = 0; i < cpus; i++) {
            if (sharded_relationships.at(i).empty()) {
                sharded_relationships.erase(i);
            }
        }

        co_return sharded_relationships;
    }

    seastar::future<uint64_t> Shard::LoadCSVNodes(uint16_t type_id, const std::string &filename, const char csv_separator, const std::vector<size_t> rows) {
        uint64_t count = 0;

        rapidcsv::Document doc(filename, rapidcsv::LabelParams(), rapidcsv::SeparatorParams(csv_separator, true),
          rapidcsv::ConverterParams(),
          rapidcsv::LineReaderParams(true /* pSkipCommentLines */,
            '#' /* pCommentPrefix */,
            true /* pSkipEmptyLines */));

        const auto allowed_property_names_and_types = node_types.getProperties(type_id).getPropertyTypes();
        std::map<size_t, std::pair<std::string, uint8_t>> column_idx_property_name_and_type;
        size_t key_column_idx = -1;
        bool has_key_column = false;

        for (auto &column_name : doc.GetColumnNames()) {
            if (column_name.ends_with("ignore")) {
                continue ;
            }

            if (column_name.ends_with(":key")) {
                key_column_idx = doc.GetColumnIdx(column_name);
                auto property_name = getPropertyName(column_name);
                if (allowed_property_names_and_types.contains(property_name)) {
                    column_idx_property_name_and_type.emplace(key_column_idx, std::make_pair(property_name, node_types.getProperties(type_id).getPropertyTypeId(property_name)) );
                    has_key_column = true;
                    continue;
                }
            }

            if (column_name == "key" ) {
                key_column_idx = doc.GetColumnIdx(column_name);
                has_key_column = true;
                continue;
            }

            if (allowed_property_names_and_types.contains(column_name)) {
                column_idx_property_name_and_type.emplace(doc.GetColumnIdx(column_name), std::make_pair(column_name, node_types.getProperties(type_id).getPropertyTypeId(column_name)) );
            }
        }

        auto &properties = node_types.getProperties(type_id);

        for (const auto row_id : rows) {
            co_await seastar::coroutine::maybe_yield();
            uint64_t internal_id = node_types.getCount(type_id);
            uint64_t external_id = 0;

            auto &keysToNodeId = node_types.getKeysToNodeId(type_id);
            std::string key;
            if (has_key_column) {
                key = doc.GetCell<std::string>(key_column_idx, row_id);
            } else {
                key = std::to_string(row_id);
            }

            if (auto key_search = keysToNodeId.find(key);
                key_search == std::end(keysToNodeId)) {
                // If we have deleted nodes, fill in the space by adding the new node here
                if (node_types.hasDeleted(type_id)) {
                    internal_id = node_types.getDeletedIdsMinimum(type_id);
                    external_id = internalToExternal(type_id, internal_id);

                    // Replace the deleted node and remove it from the list
                    node_types.getKeys(type_id).at(internal_id) = key;
                } else {
                    external_id = internalToExternal(type_id, internal_id);
                    // Add the node to the end and prepare a place for its relationships
                    node_types.getKeys(type_id).emplace_back(key);
                    node_types.getOutgoingRelationships(type_id).emplace_back();
                    node_types.getIncomingRelationships(type_id).emplace_back();
                }
                node_types.addId(type_id, internal_id);
                for (auto & [column_idx, name_type_pair] : column_idx_property_name_and_type) {
                    switch (name_type_pair.second) {
                    case Properties::boolean_type: {
                        properties.setBooleanProperty(name_type_pair.first, internal_id, doc.GetCell<bool>(column_idx, row_id));
                        break;
                    }
                    case Properties::integer_type: {
                        properties.setIntegerProperty(name_type_pair.first, internal_id, doc.GetCell<int64_t>(column_idx, row_id));
                        break;
                    }
                    case Properties::double_type: {
                        properties.setDoubleProperty(name_type_pair.first, internal_id, doc.GetCell<double>(column_idx, row_id));
                        break;
                    }
                    case Properties::string_type: {
                        properties.setStringProperty(name_type_pair.first, internal_id, doc.GetCell<std::string>(column_idx, row_id));
                        break;
                    }
                    case Properties::date_type: {
                        properties.setDateProperty(name_type_pair.first, internal_id, Date::convert(doc.GetCell<std::string>(column_idx, row_id))); // Date are stored as doubles
                        break;
                    }
                        // TODO: Find a way to import lists... maybe get as string, then split by semicolon, then convert from string to appropriate type
                        //                        case Properties::boolean_list_type: properties.setListOfBooleanProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<bool>>(column_idx, row_id));
                        //                        case Properties::integer_list_type: properties.setListOfIntegerProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<int64_t>>(column_idx, row_id));
                        //                        case Properties::double_list_type: properties.setListOfDoubleProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<double>>(column_idx, row_id));
                        //                        case Properties::string_list_type: properties.setListOfStringProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<std::string>>(column_idx, row_id));
                        //                        case Properties::date_list_type: properties.setListOfDateProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<double>>(column_idx, row_id)); // Lists of Dates are stored as Lists of Doubles
                    default: break;
                    }

                }
                keysToNodeId.insert({key, external_id });
                count++;
            }
        }
        co_return count;
    }

    uint64_t Shard::StreamCSVNodes(uint16_t type_id, const std::string &filename, const char csv_separator, const std::vector<size_t> rows) {
        uint64_t count = 0;

        csv::CSVFormat format;
        format.delimiter(csv_separator).quote('"').trim({ ' ', '\t' });
        std::ifstream infile(filename, std::ios::binary);
        csv::CSVReader reader(infile, format);

        const auto allowed_property_names_and_types = node_types.getProperties(type_id).getPropertyTypes();
        std::map<size_t, std::pair<std::string, uint8_t>> column_idx_property_name_and_type;
        size_t key_column_idx = -1;
        bool has_key_column = false;

        for (auto &column_name : reader.get_col_names()) {
            if (column_name.ends_with("ignore")) {
                continue ;
            }

            if (column_name.ends_with(":key")) {

                key_column_idx = reader.index_of(column_name);
                auto property_name = getPropertyName(column_name);
                if (allowed_property_names_and_types.contains(property_name)) {
                    column_idx_property_name_and_type.emplace(key_column_idx, std::make_pair(property_name, node_types.getProperties(type_id).getPropertyTypeId(property_name)) );
                    has_key_column = true;
                    continue;
                }
            }

            if (column_name == "key" ) {
                key_column_idx = reader.index_of(column_name);
                has_key_column = true;
                continue;
            }

            if (allowed_property_names_and_types.contains(column_name)) {
                column_idx_property_name_and_type.emplace(reader.index_of(column_name), std::make_pair(column_name, node_types.getProperties(type_id).getPropertyTypeId(column_name)) );
            }
        }

        auto &properties = node_types.getProperties(type_id);

        roaring::Roaring64Map valid_rows;
        valid_rows.addMany(rows.size(), rows.data());

        size_t row_id = 0;
        for (csv::CSVRow& csv_row: reader) {
            if (valid_rows.contains(row_id)) {
                uint64_t internal_id = node_types.getCount(type_id);
                uint64_t external_id = 0;

                auto &keysToNodeId = node_types.getKeysToNodeId(type_id);
                std::string key;
                if (has_key_column) {
                    key = csv_row[key_column_idx].get<std::string>();
                } else {
                    key = std::to_string(row_id);
                }

                if (auto key_search = keysToNodeId.find(key);
                    key_search == std::end(keysToNodeId)) {
                    // If we have deleted nodes, fill in the space by adding the new node here
                    if (node_types.hasDeleted(type_id)) {
                        internal_id = node_types.getDeletedIdsMinimum(type_id);
                        external_id = internalToExternal(type_id, internal_id);

                        // Replace the deleted node and remove it from the list
                        node_types.getKeys(type_id).at(internal_id) = key;
                    } else {
                        external_id = internalToExternal(type_id, internal_id);
                        // Add the node to the end and prepare a place for its relationships
                        node_types.getKeys(type_id).emplace_back(key);
                        node_types.getOutgoingRelationships(type_id).emplace_back();
                        node_types.getIncomingRelationships(type_id).emplace_back();
                    }
                    node_types.addId(type_id, internal_id);
                    for (auto & [column_idx, name_type_pair] : column_idx_property_name_and_type) {
                        switch (name_type_pair.second) {
                        case Properties::boolean_type: {
                            properties.setBooleanProperty(name_type_pair.first, internal_id, csv_row[column_idx].get<bool>());
                            break;
                        }
                        case Properties::integer_type: {
                            properties.setIntegerProperty(name_type_pair.first, internal_id, csv_row[column_idx].get<int64_t>());
                            break;
                        }
                        case Properties::double_type: {
                            properties.setDoubleProperty(name_type_pair.first, internal_id, csv_row[column_idx].get<double>());
                            break;
                        }
                        case Properties::string_type: {
                            properties.setStringProperty(name_type_pair.first, internal_id, csv_row[column_idx].get<std::string>());
                            break;
                        }
                        case Properties::date_type: {
                            properties.setDateProperty(name_type_pair.first, internal_id, Date::convert(csv_row[column_idx].get<std::string>())); // Date are stored as doubles
                            break;
                        }
                            // TODO: Find a way to import lists... maybe get as string, then split by semicolon, then convert from string to appropriate type
                            //                        case Properties::boolean_list_type: properties.setListOfBooleanProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<bool>>(column_idx, row_id));
                            //                        case Properties::integer_list_type: properties.setListOfIntegerProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<int64_t>>(column_idx, row_id));
                            //                        case Properties::double_list_type: properties.setListOfDoubleProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<double>>(column_idx, row_id));
                            //                        case Properties::string_list_type: properties.setListOfStringProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<std::string>>(column_idx, row_id));
                            //                        case Properties::date_list_type: properties.setListOfDateProperty(name_type_pair.first, internal_id, doc.GetCell<std::vector<double>>(column_idx, row_id)); // Lists of Dates are stored as Lists of Doubles
                        default: break;
                        }

                    }
                    keysToNodeId.insert({key, external_id });
                    count++;
                }
            }
            row_id++;
        }

        return count;
    }
}