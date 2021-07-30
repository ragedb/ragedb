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

#ifndef RAGEDB_JSON_H
#define RAGEDB_JSON_H

#include <Graph.h>
#include <Node.h>
#include <seastar/core/print.hh>
#include <seastar/http/httpd.hh>
#include <seastar/json/json_elements.hh>

using namespace seastar;
using namespace httpd;
using namespace ragedb;

class json_values_builder {
public:
    json_values_builder() {
        result << OPEN_ARRAY;
    }

    void add_values(const std::vector<std::any>& values) {
        for(const auto& value : values) {
            add_value(value);
        }
    }

    void add_value(std::any const & value) {
        if(value.type() == typeid(std::string)) {
            add(seastar::json::formatter::to_json(std::any_cast<std::string>(value)));
            return;
        }

        if(value.type() == typeid(int64_t)) {
            add(seastar::json::formatter::to_json(std::any_cast<int64_t>(value)));
            return;
        }

        if(value.type() == typeid(double)) {
            add(seastar::json::formatter::to_json(std::any_cast<double>(value)));
            return;
        }

        // Booleans are stored in std::any as a bit reference so we can't use if(value.type() == typeid(bool)) {
        if(value.type() == typeid(std::_Bit_reference)) {
            if (std::any_cast<std::_Bit_reference>(value) ) {
                add(seastar::json::formatter::to_json(true));
            } else {
                add(seastar::json::formatter::to_json(false));
            }
            return;
        }

        if(value.type() == typeid(std::map<std::string, std::any>)) {
            add_properties(std::any_cast<std::map<std::string, std::any>>(value));
            return;
        }
    }

    void add_properties(std::map<std::string, std::any> const & props) {
        bool initial = true;
        for (auto[key, value] : props) {
            std::string property = static_cast<std::string>(key);

            if(value.type() == typeid(std::string)) {
                add_object(initial, property, seastar::json::formatter::to_json(std::any_cast<std::string>(value)));
                continue;
            }

            if(value.type() == typeid(int64_t)) {
                add_object(initial, property, seastar::json::formatter::to_json(std::any_cast<int64_t>(value)));
                continue;
            }

            if(value.type() == typeid(double)) {
                add_object(initial, property, seastar::json::formatter::to_json(std::any_cast<double>(value)));
                continue;
            }

            // Booleans are stored in std::any as a bit reference so we can't use if(value.type() == typeid(bool)) {
            if(value.type() == typeid(std::_Bit_reference)) {
                if (std::any_cast<std::_Bit_reference>(value) ) {
                    add_object(initial, property, seastar::json::formatter::to_json(true));
                } else {
                    add_object(initial, property, seastar::json::formatter::to_json(false));
                }
                continue;
            }

            if(value.type() == typeid(std::map<std::string, std::any>)) {
                add_properties(std::any_cast<std::map<std::string, std::any>>(value));
                continue;
            }
            initial = false;
        }
    }

    void add_object(bool initial, const std::string& name, const std::string& str) {
        if (!initial) {
            result << ", ";
        }
        result << '"' << name << "\": " << str;
    }

    void add(const std::string& str) {
        if (first) {
            first = false;
        } else {
            result << ", ";
        }
        result << str;
    }

    std::string as_json() {
        result << CLOSE_ARRAY;
        return result.str();
    }

private:
    static const char OPEN_ARRAY = '[';
    static const char CLOSE_ARRAY = ']';
    std::stringstream result;
    bool first{true};
};

class json_properties_builder {
public:
    json_properties_builder() {
        result << OPEN << SPACE;
    }

    void add_properties(std::map<std::string, std::any> const & props) {
        for (auto[key, value] : props) {
            std::string property = static_cast<std::string>(key);

            if(value.type() == typeid(std::string)) {
                add(property, seastar::json::formatter::to_json(std::any_cast<std::string>(value)));
                continue;
            }

            if(value.type() == typeid(int64_t)) {
                add(property, seastar::json::formatter::to_json(std::any_cast<int64_t>(value)));
                continue;
            }

            if(value.type() == typeid(double)) {
                add(property, seastar::json::formatter::to_json(std::any_cast<double>(value)));
                continue;
            }

            // Booleans are stored in std::any as a bit reference so we can't use if(value.type() == typeid(bool)) {
            if(value.type() == typeid(std::_Bit_reference)) {
                if (std::any_cast<std::_Bit_reference>(value) ) {
                    add(property, seastar::json::formatter::to_json(true));
                } else {
                    add(property, seastar::json::formatter::to_json(false));
                }
                continue;
            }

            if(value.type() == typeid(std::vector<std::string>)) {
                add_key(property);
                result << OPEN_ARRAY;

                bool nested_initial = true;
                for (const auto& item : std::any_cast<std::vector<std::string>>(value)) {
                    if (!nested_initial) {
                        result << ", ";
                    }
                    result << '"' << item << "\"";
                    nested_initial = false;
                }

                result << CLOSE_ARRAY;
                continue;
            }

            if(value.type() == typeid(std::vector<int64_t>)) {
                add_key(property);
                result << OPEN_ARRAY;

                bool nested_initial = true;
                for (const auto& item : std::any_cast<std::vector<int64_t>>(value)) {
                    if (!nested_initial) {
                        result << ", ";
                    }
                    result << item;
                    nested_initial = false;
                }

                result << CLOSE_ARRAY;
                continue;
            }

            if(value.type() == typeid(std::vector<double>)) {
                add_key(property);
                result << OPEN_ARRAY;

                bool nested_initial = true;
                for (const auto& item : std::any_cast<std::vector<double>>(value)) {
                    if (!nested_initial) {
                        result << ", ";
                    }
                    result << item;
                    nested_initial = false;
                }

                result << CLOSE_ARRAY;
                continue;
            }

            if(value.type() == typeid(std::vector<bool>)) {
                add_key(property);
                result << OPEN_ARRAY;

                bool nested_initial = true;
                for (const auto& item : std::any_cast<std::vector<bool>>(value)) {
                    if (!nested_initial) {
                        result << ", ";
                    }
                    result << item;
                    nested_initial = false;
                }

                result << CLOSE_ARRAY;
                continue;
            }

            if(value.type() == typeid(std::map<std::string, std::any>)) {
                add_properties(std::any_cast<std::map<std::string, std::any>>(value));
                continue;
            }
        }
    }

    void add(seastar::json::json_base_element* element) {
        if (element == nullptr || !element->_set) {
            return;
        }
        try {
            add(element->_name, element->to_string());
        } catch (...) {
            std::throw_with_nested(std::runtime_error(format("Json generation failed for field: " , element->_name)));
        }
    }

    void add(const std::string& name, const std::string& str) {
        if (first) {
            first = false;
        } else {
            result << ", ";
        }
        result << '"' << name << "\": " << str;
    }

    void add_key(const std::string& name) {
        if (first) {
            first = false;
        } else {
            result << ", ";
        }
        result << '"' << name << "\": ";
    }

    std::string as_json() {
        result << SPACE << CLOSE;
        return result.str();
    }

private:
    static const char OPEN = '{';
    static const char CLOSE = '}';
    static const char SPACE = ' ';
    static const char OPEN_ARRAY = '[';
    static const char CLOSE_ARRAY = ']';
    std::stringstream result;
    bool first{true};

};

struct schema_json : public json::jsonable {
private:
    std::map<std::string, std::string> schema;

public:
    schema_json(const std::map<std::string, std::string> &_schema) : schema(_schema) {}
    schema_json() = default;
    ~schema_json() {
        schema.clear();
    }

    std::string to_json() const {
        json_properties_builder jsonPropertiesBuilder;
        for (auto [key, val] : schema) {
            jsonPropertiesBuilder.add(key, val);
        }

        return jsonPropertiesBuilder.as_json();
    }
};

struct properties_json : public json::jsonable {
private:
    std::map<std::string, std::any> properties;

public:
    properties_json(const std::map<std::string, std::any> &_properties) : properties(_properties) {}
    properties_json() = default;
    ~properties_json() {
        properties.clear();
    }

    std::string to_json() const {
        json_properties_builder jsonPropertiesBuilder;
        jsonPropertiesBuilder.add_properties(properties);
        return jsonPropertiesBuilder.as_json();
    }

};

struct relationship_json : public json::json_base {
    json::json_element<std::uint64_t> id;
    json::json_element<std::string> type;
    json::json_element<std::uint64_t> from;
    json::json_element<std::uint64_t> to;
    json::json_element<properties_json> properties;

    void register_params() {
        add(&id, "id");
        add(&type, "type");
        add(&from, "from");
        add(&to, "to");
        add(&properties, "properties");
    }

    relationship_json() {
        register_params();
    }

    relationship_json(relationship_json const & e) : json::json_base() {
        register_params();
        id = e.id;
        type = e.type;
        from = e.from;
        to = e.to;
        properties = e.properties;
    }

    template<class T>
    relationship_json& operator=(const T& e) {
        id = e.id;
        type = e.type;
        from = e.starting_node_ids;
        to = e.ending_node_ids;
        properties = e.properties;
        return *this;
    }

    relationship_json& operator=(const relationship_json& e) {
        id = e.id;
        type = e.type;
        from = e.from;
        to = e.to;
        properties = e.properties;
        return *this;
    }

public:

    // We have the relationship, but don't know the relationship type, so go get it
    relationship_json(Relationship& r) {
        register_params();
        this->id = r.getId();
        this->type = r.getType();
        this->from = r.getStartingNodeId();
        this->to = r.getEndingNodeId();
        this->properties = r.getProperties();
    }

};

struct node_json : public json::json_base {
    json::json_element<std::uint64_t> id;
    json::json_element<std::string> type;
    json::json_element<std::string> key;
    json::json_element<properties_json> properties;

    void register_params() {
        add(&id, "id");
        add(&type, "type");
        add(&key, "key");
        add(&properties, "properties");
    }

    node_json() {
        register_params();
    }

    node_json(node_json const & e) : json::json_base() {
        register_params();
        id = e.id;
        type = e.type;
        key = e.key;
        properties = e.properties;
    }

    template<class T>
    node_json& operator=(const T& e){
        id = e.getId();
        type = e.getType();
        key = e.getKey();
        properties = e.getProperties();
        return *this;
    }

    node_json& operator=(const node_json& e){
        id = e.id;
        type = e.type;
        key = e.key;
        properties = e.properties;
        return *this;
    }

public:

    node_json(Node& n) {
        register_params();
        this->id = n.getId();
        this->type = n.getType();
        this->key = n.getKey();
        this->properties = n.getProperties();
    }

};

#endif //RAGEDB_JSON_H
