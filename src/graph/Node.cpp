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

#include "Node.h"

#include <utility>
#include "Shard.h"

namespace ragedb {

    Node::Node() = default;

    Node::Node(uint64_t id, std::string type, std::string key) : id(id), type(std::move(type)), key(std::move(key)) {}

    Node::Node(uint64_t id, std::string type, std::string key, std::map<std::string, std::any>  properties) : id(id), type(std::move(type)), key(std::move(key)), properties(std::move(properties)) {}

    uint64_t Node::getId() const {
        return id;
    }

    uint16_t Node::getTypeId() const {
        return Shard::externalToTypeId(id);
    }

    std::string Node::getKey() const {
        return key;
    }

    std::string Node::getType() const {
        return type;
    }

    std::map<std::string, std::any> Node::getProperties() const {
        return properties;
    }

}