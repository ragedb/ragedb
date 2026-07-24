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

// JSON serialization of GqlValue results. Split out of GqlValue.cpp so the value semantics (comparison,
// cast, evaluation, scalar functions) carry no seastar/json dependency -- this is the only unit that does.

#include "GqlValue.h"
#include <seastar/json/json_elements.hh>

namespace ragedb::gql {

/**
 * @brief Serializes a GqlValue (Nil, Node, Relationship, or Property variant) to a JSON string.
 *
 * Maps typed property types (booleans, integers, doubles, strings, array vectors) to
 * compliant JSON notations.
 *
 * @param val The GqlValue to serialize.
 * @return std::string JSON representation.
 */
std::string serialize_gql_value(const GqlValue& val) {
    if (val.type == GqlValue::NIL) {
        return "null";
    }
    if (val.type == GqlValue::PROPERTY) {
        switch (val.property.index()) {
            case 0: return "null";
            case 1: return std::get<bool>(val.property) ? "true" : "false";
            case 2: return std::to_string(std::get<int64_t>(val.property));
            case 3: return std::to_string(std::get<double>(val.property));
            case 4: return seastar::json::formatter::to_json(std::get<std::string>(val.property));
            case 5: {
                std::string s = "[";
                bool init = true;
                for (bool x : std::get<std::vector<bool>>(val.property)) {
                    if (!init) s += ",";
                    s += (x ? "true" : "false");
                    init = false;
                }
                s += "]";
                return s;
            }
            case 6: {
                std::string s = "[";
                bool init = true;
                for (int64_t x : std::get<std::vector<int64_t>>(val.property)) {
                    if (!init) s += ",";
                    s += std::to_string(x);
                    init = false;
                }
                s += "]";
                return s;
            }
            case 7: {
                std::string s = "[";
                bool init = true;
                for (double x : std::get<std::vector<double>>(val.property)) {
                    if (!init) s += ",";
                    s += std::to_string(x);
                    init = false;
                }
                s += "]";
                return s;
            }
            case 8: {
                std::string s = "[";
                bool init = true;
                for (const auto& x : std::get<std::vector<std::string>>(val.property)) {
                    if (!init) s += ",";
                    s += seastar::json::formatter::to_json(x);
                    init = false;
                }
                s += "]";
                return s;
            }
        }
    }
    if (val.type == GqlValue::NODE) {
        std::string s = "{\"id\": " + std::to_string(val.node->getId()) + ", \"type\": \"" + val.node->getType() + "\", \"key\": \"" + val.node->getKey() + "\", \"properties\": {";
        bool init = true;
        for (const auto& [k, v] : val.node->getProperties()) {
            if (!init) s += ", ";
            s += "\"" + k + "\": " + serialize_gql_value(GqlValue(v));
            init = false;
        }
        s += "}}";
        return s;
    }
    if (val.type == GqlValue::RELATIONSHIP) {
        std::string s = "{\"id\": " + std::to_string(val.relationship->getId()) + ", \"type\": \"" + val.relationship->getType() + "\", \"from\": " + std::to_string(val.relationship->getStartingNodeId()) + ", \"to\": " + std::to_string(val.relationship->getEndingNodeId()) + ", \"properties\": {";
        bool init = true;
        for (const auto& [k, v] : val.relationship->getProperties()) {
            if (!init) s += ", ";
            s += "\"" + k + "\": " + serialize_gql_value(GqlValue(v));
            init = false;
        }
        s += "}}";
        return s;
    }
    if (val.type == GqlValue::RELATIONSHIP_LIST) {
        std::string s = "[";
        bool init = true;
        for (const auto& rel : *val.relationship_list) {
            if (!init) s += ",";
            s += serialize_gql_value(GqlValue(rel));
            init = false;
        }
        s += "]";
        return s;
    }
    // Serialize a heterogeneous list (collect_list result) to a JSON array.
    if (val.type == GqlValue::LIST) {
        std::string s = "[";
        bool init = true;
        for (const auto& item : *val.list) {
            if (!init) s += ", ";
            s += serialize_gql_value(item);
            init = false;
        }
        s += "]";
        return s;
    }
    // Serialize a Path to a JSON object containing lists of serialized nodes and relationships.
    if (val.type == GqlValue::PATH) {
        std::string s = "{\"nodes\": [";
        bool init = true;
        for (const auto& node : val.path->GetNodes()) {
            if (!init) s += ", ";
            s += serialize_gql_value(GqlValue(node));
            init = false;
        }
        s += "], \"relationships\": [";
        init = true;
        for (const auto& rel : val.path->GetRelationships()) {
            if (!init) s += ", ";
            s += serialize_gql_value(GqlValue(rel));
            init = false;
        }
        s += "]}";
        return s;
    }
    return "null";
}

/**
 * @brief Serializes a property map into a JSON object string.
 *
 * @param props The property map to format.
 * @return std::string JSON object string.
 */
std::string serialize_properties_to_json(const std::map<std::string, property_type_t>& props) {
    std::string json = "{";
    bool first = true;
    for (const auto& [k, v] : props) {
        if (!first) json += ", ";
        json += "\"" + k + "\": " + serialize_gql_value(GqlValue(v));
        first = false;
    }
    json += "}";
    return json;
}

} // namespace ragedb::gql
