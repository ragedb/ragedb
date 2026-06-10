/*
 * Copyright RageDB Contributors. All Rights Reserved.
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

#include "GqlCatalog.h"
#include <stdexcept>

namespace ragedb::gql {

seastar::future<std::string> GqlCatalog::execute_schema_op(ragedb::Graph& graph, const SchemaOperation& schema_op) {
    switch (schema_op.op) {
        case SchemaOperation::Op::CREATE_NODE_TYPE: {
            std::string name = schema_op.name;
            auto node_types = graph.shard.local().NodeTypesGetPeered();
            if (node_types.find(name) != node_types.end()) {
                return seastar::make_exception_future<std::string>(
                    std::runtime_error("NodeType '" + name + "' already exists")
                );
            }
            
            return graph.shard.local().NodeTypeInsertPeered(name)
            .then([&graph, name, properties = schema_op.properties]([[maybe_unused]] uint16_t type_id) {
                if (properties.empty()) {
                    return seastar::make_ready_future<std::string>(
                        "{\"status\": \"created\", \"node_type\": \"" + name + "\"}"
                    );
                }
                
                seastar::future<uint8_t> f = seastar::make_ready_future<uint8_t>(0);
                for (const auto& prop : properties) {
                    std::string prop_name = prop.first;
                    std::string prop_type = prop.second;
                    f = f.then([&graph, name, prop_name, prop_type](uint8_t) {
                        return graph.shard.local().NodePropertyTypeAddPeered(name, prop_name, prop_type);
                    });
                }
                
                return f.then([name](uint8_t) {
                    return seastar::make_ready_future<std::string>(
                        "{\"status\": \"created\", \"node_type\": \"" + name + "\"}"
                    );
                });
            });
        }
        case SchemaOperation::Op::DROP_NODE_TYPE: {
            std::string name = schema_op.name;
            auto node_types = graph.shard.local().NodeTypesGetPeered();
            if (node_types.find(name) == node_types.end()) {
                return seastar::make_exception_future<std::string>(
                    std::runtime_error("NodeType '" + name + "' does not exist")
                );
            }
            return graph.shard.local().DeleteNodeTypePeered(name)
            .then([name](bool success) {
                if (!success) {
                    return seastar::make_exception_future<std::string>(
                        std::runtime_error("Failed to delete NodeType '" + name + "'")
                    );
                }
                return seastar::make_ready_future<std::string>(
                    "{\"status\": \"deleted\", \"node_type\": \"" + name + "\"}"
                );
            });
        }
        case SchemaOperation::Op::CREATE_REL_TYPE: {
            std::string name = schema_op.name;
            auto rel_types = graph.shard.local().RelationshipTypesGetPeered();
            if (rel_types.find(name) != rel_types.end()) {
                return seastar::make_exception_future<std::string>(
                    std::runtime_error("RelationshipType '" + name + "' already exists")
                );
            }
            
            return graph.shard.local().RelationshipTypeInsertPeered(name)
            .then([&graph, name, properties = schema_op.properties]([[maybe_unused]] uint16_t type_id) {
                if (properties.empty()) {
                    return seastar::make_ready_future<std::string>(
                        "{\"status\": \"created\", \"relationship_type\": \"" + name + "\"}"
                    );
                }
                
                seastar::future<uint8_t> f = seastar::make_ready_future<uint8_t>(0);
                for (const auto& prop : properties) {
                    std::string prop_name = prop.first;
                    std::string prop_type = prop.second;
                    f = f.then([&graph, name, prop_name, prop_type](uint8_t) {
                        return graph.shard.local().RelationshipPropertyTypeAddPeered(name, prop_name, prop_type);
                    });
                }
                
                return f.then([name](uint8_t) {
                    return seastar::make_ready_future<std::string>(
                        "{\"status\": \"created\", \"relationship_type\": \"" + name + "\"}"
                    );
                });
            });
        }
        case SchemaOperation::Op::DROP_REL_TYPE: {
            std::string name = schema_op.name;
            auto rel_types = graph.shard.local().RelationshipTypesGetPeered();
            if (rel_types.find(name) == rel_types.end()) {
                return seastar::make_exception_future<std::string>(
                    std::runtime_error("RelationshipType '" + name + "' does not exist")
                );
            }
            return graph.shard.local().DeleteRelationshipTypePeered(name)
            .then([name](bool success) {
                if (!success) {
                    return seastar::make_exception_future<std::string>(
                        std::runtime_error("Failed to delete RelationshipType '" + name + "'")
                    );
                }
                return seastar::make_ready_future<std::string>(
                    "{\"status\": \"deleted\", \"relationship_type\": \"" + name + "\"}"
                );
            });
        }
        case SchemaOperation::Op::ALTER_NODE_TYPE: {
            std::string name = schema_op.name;
            auto node_types = graph.shard.local().NodeTypesGetPeered();
            if (node_types.find(name) == node_types.end()) {
                return seastar::make_exception_future<std::string>(
                    std::runtime_error("NodeType '" + name + "' does not exist")
                );
            }
            
            if (schema_op.alter_op == SchemaOperation::AlterOp::ADD) {
                std::string prop_name = schema_op.alter_property_name;
                std::string prop_type = schema_op.alter_property_type;
                std::string existing_type = graph.shard.local().NodePropertyTypeGet(name, prop_name);
                if (!existing_type.empty()) {
                    return seastar::make_exception_future<std::string>(
                        std::runtime_error("Property '" + prop_name + "' already exists on NodeType '" + name + "'")
                    );
                }
                return graph.shard.local().NodePropertyTypeAddPeered(name, prop_name, prop_type)
                .then([name, prop_name](uint8_t) {
                    return seastar::make_ready_future<std::string>(
                        "{\"status\": \"altered\", \"node_type\": \"" + name + "\", \"added\": \"" + prop_name + "\"}"
                    );
                });
            } else {
                std::string prop_name = schema_op.alter_property_name;
                std::string existing_type = graph.shard.local().NodePropertyTypeGet(name, prop_name);
                if (existing_type.empty()) {
                    return seastar::make_exception_future<std::string>(
                        std::runtime_error("Property '" + prop_name + "' does not exist on NodeType '" + name + "'")
                    );
                }
                return graph.shard.local().NodePropertyTypeDeletePeered(name, prop_name)
                .then([name, prop_name](bool success) {
                    if (!success) {
                        return seastar::make_exception_future<std::string>(
                            std::runtime_error("Failed to delete property '" + prop_name + "' on NodeType '" + name + "'")
                        );
                    }
                    return seastar::make_ready_future<std::string>(
                        "{\"status\": \"altered\", \"node_type\": \"" + name + "\", \"dropped\": \"" + prop_name + "\"}"
                    );
                });
            }
        }
        case SchemaOperation::Op::ALTER_REL_TYPE: {
            std::string name = schema_op.name;
            auto rel_types = graph.shard.local().RelationshipTypesGetPeered();
            if (rel_types.find(name) == rel_types.end()) {
                return seastar::make_exception_future<std::string>(
                    std::runtime_error("RelationshipType '" + name + "' does not exist")
                );
            }
            
            if (schema_op.alter_op == SchemaOperation::AlterOp::ADD) {
                std::string prop_name = schema_op.alter_property_name;
                std::string prop_type = schema_op.alter_property_type;
                std::string existing_type = graph.shard.local().RelationshipPropertyTypeGet(name, prop_name);
                if (!existing_type.empty()) {
                    return seastar::make_exception_future<std::string>(
                        std::runtime_error("Property '" + prop_name + "' already exists on RelationshipType '" + name + "'")
                    );
                }
                return graph.shard.local().RelationshipPropertyTypeAddPeered(name, prop_name, prop_type)
                .then([name, prop_name](uint8_t) {
                    return seastar::make_ready_future<std::string>(
                        "{\"status\": \"altered\", \"relationship_type\": \"" + name + "\", \"added\": \"" + prop_name + "\"}"
                    );
                });
            } else {
                std::string prop_name = schema_op.alter_property_name;
                std::string existing_type = graph.shard.local().RelationshipPropertyTypeGet(name, prop_name);
                if (existing_type.empty()) {
                    return seastar::make_exception_future<std::string>(
                        std::runtime_error("Property '" + prop_name + "' does not exist on RelationshipType '" + name + "'")
                    );
                }
                return graph.shard.local().RelationshipPropertyTypeDeletePeered(name, prop_name)
                .then([name, prop_name](bool success) {
                    if (!success) {
                        return seastar::make_exception_future<std::string>(
                            std::runtime_error("Failed to delete property '" + prop_name + "' on RelationshipType '" + name + "'")
                        );
                    }
                    return seastar::make_ready_future<std::string>(
                        "{\"status\": \"altered\", \"relationship_type\": \"" + name + "\", \"dropped\": \"" + prop_name + "\"}"
                    );
                });
            }
        }
        case SchemaOperation::Op::CREATE_INDEX: {
            std::string type_name = schema_op.name;
            std::string property_name = schema_op.alter_property_name;
            
            auto node_types = graph.shard.local().NodeTypesGetPeered();
            if (node_types.find(type_name) != node_types.end()) {
                std::string prop_type = graph.shard.local().NodePropertyTypeGet(type_name, property_name);
                if (prop_type.empty()) {
                    return seastar::make_exception_future<std::string>(
                        std::runtime_error("Property '" + property_name + "' does not exist on NodeType '" + type_name + "'")
                    );
                }
                return graph.shard.local().NodeIndexCreatePeered(type_name, property_name)
                .then([type_name, property_name](bool success) {
                    if (!success) {
                        return seastar::make_exception_future<std::string>(
                            std::runtime_error("Failed to create index on " + type_name + "." + property_name)
                        );
                    }
                    return seastar::make_ready_future<std::string>(
                        "{\"status\": \"created\", \"index\": \"" + type_name + "." + property_name + "\"}"
                    );
                });
            }
            
            auto rel_types = graph.shard.local().RelationshipTypesGetPeered();
            if (rel_types.find(type_name) != rel_types.end()) {
                std::string prop_type = graph.shard.local().RelationshipPropertyTypeGet(type_name, property_name);
                if (prop_type.empty()) {
                    return seastar::make_exception_future<std::string>(
                        std::runtime_error("Property '" + property_name + "' does not exist on RelationshipType '" + type_name + "'")
                    );
                }
                return graph.shard.local().RelationshipIndexCreatePeered(type_name, property_name)
                .then([type_name, property_name](bool success) {
                    if (!success) {
                        return seastar::make_exception_future<std::string>(
                            std::runtime_error("Failed to create index on " + type_name + "." + property_name)
                        );
                    }
                    return seastar::make_ready_future<std::string>(
                        "{\"status\": \"created\", \"index\": \"" + type_name + "." + property_name + "\"}"
                    );
                });
            }
            
            return seastar::make_exception_future<std::string>(
                std::runtime_error("NodeType or RelationshipType '" + type_name + "' does not exist")
            );
        }
        case SchemaOperation::Op::DROP_INDEX: {
            std::string type_name = schema_op.name;
            std::string property_name = schema_op.alter_property_name;
            
            auto node_types = graph.shard.local().NodeTypesGetPeered();
            if (node_types.find(type_name) != node_types.end()) {
                return graph.shard.local().NodeIndexDeletePeered(type_name, property_name)
                .then([type_name, property_name](bool success) {
                    if (!success) {
                        return seastar::make_exception_future<std::string>(
                            std::runtime_error("Failed to drop index on " + type_name + "." + property_name)
                        );
                    }
                    return seastar::make_ready_future<std::string>(
                        "{\"status\": \"dropped\", \"index\": \"" + type_name + "." + property_name + "\"}"
                    );
                });
            }
            
            auto rel_types = graph.shard.local().RelationshipTypesGetPeered();
            if (rel_types.find(type_name) != rel_types.end()) {
                return graph.shard.local().RelationshipIndexDeletePeered(type_name, property_name)
                .then([type_name, property_name](bool success) {
                    if (!success) {
                        return seastar::make_exception_future<std::string>(
                            std::runtime_error("Failed to drop index on " + type_name + "." + property_name)
                        );
                    }
                    return seastar::make_ready_future<std::string>(
                        "{\"status\": \"dropped\", \"index\": \"" + type_name + "." + property_name + "\"}"
                    );
                });
            }
            
            return seastar::make_exception_future<std::string>(
                std::runtime_error("NodeType or RelationshipType '" + type_name + "' does not exist")
            );
        }
        case SchemaOperation::Op::SHOW_INDEXES: {
            std::string filter_type_name = schema_op.name;
            std::string json = "[";
            bool first = true;
            
            auto node_indexes_map = graph.shard.local().NodeIndexesGet();
            for (const auto& [type_name, properties] : node_indexes_map) {
                if (filter_type_name.empty() || filter_type_name == type_name) {
                    for (const auto& property : properties) {
                        if (!first) json += ", ";
                        first = false;
                        json += "{\"type\": \"Node\", \"label\": \"" + type_name + "\", \"property\": \"" + property + "\"}";
                    }
                }
            }
            
            auto rel_indexes_map = graph.shard.local().RelationshipIndexesGet();
            for (const auto& [type_name, properties] : rel_indexes_map) {
                if (filter_type_name.empty() || filter_type_name == type_name) {
                    for (const auto& property : properties) {
                        if (!first) json += ", ";
                        first = false;
                        json += "{\"type\": \"Relationship\", \"label\": \"" + type_name + "\", \"property\": \"" + property + "\"}";
                    }
                }
            }
            json += "]";
            return seastar::make_ready_future<std::string>(json);
        }
    }
    return seastar::make_ready_future<std::string>("{}");
}

} // namespace ragedb::gql
