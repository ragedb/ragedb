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

#ifndef RAGEDB_PROPERTIES_H
#define RAGEDB_PROPERTIES_H

#include <any>
#include <cstdint>
#include <map>
#include <tsl/sparse_map.h>
#include <seastar/core/rwlock.hh>
#include <roaring/roaring64map.hh>

namespace ragedb {
    class Properties {

    private:
        tsl::sparse_map<std::string, uint8_t> types;
        tsl::sparse_map<std::string, uint8_t> type_map;
        std::vector<std::string> allowed_types;
        tsl::sparse_map<std::string, roaring::Roaring64Map> deleted;
        tsl::sparse_map<std::string, std::vector<bool>> booleans;
        tsl::sparse_map<std::string, std::vector<int64_t>> integers;
        tsl::sparse_map<std::string, std::vector<double>> doubles;
        tsl::sparse_map<std::string, std::vector<std::string>> strings;
        tsl::sparse_map<std::string, std::vector<std::vector<bool>>> booleans_list;
        tsl::sparse_map<std::string, std::vector<std::vector<int64_t>>> integers_list;
        tsl::sparse_map<std::string, std::vector<std::vector<double>>> doubles_list;
        tsl::sparse_map<std::string, std::vector<std::vector<std::string>>> strings_list;
        // TODO: Supported Nested Objects

        const std::any tombstone_any = std::any();

        const bool tombstone_boolean = false;
        const int64_t tombstone_int = std::numeric_limits<int64_t>::min();
        const double tombstone_double = std::numeric_limits<double>::min();
        const std::string tombstone_string = std::string("");

        const std::vector<bool> tombstone_list_of_booleans = std::vector<bool>();
        const std::vector<int64_t> tombstone_list_of_ints = std::vector<int64_t>();
        const std::vector<double> tombstone_list_of_doubles = std::vector<double>();
        const std::vector<std::string> tombstone_list_of_strings = std::vector<std::string>();

        void addPropertyTypeVectors(const std::string &key, uint8_t type_id);
        void removePropertyTypeVectors(const std::string &key, uint8_t type_id);

    public:
        Properties();
        void clear();
        seastar::rwlock property_type_lock;

        std::map<std::string, std::string> getPropertyTypes();
        uint8_t getPropertyTypeId(const std::string& key);
        uint8_t setPropertyTypeId(const std::string& key, uint8_t property_type_id);
        uint8_t setPropertyType(const std::string& key, const std::string& type);
        bool removePropertyType(const std::string& key);

        bool setBooleanProperty(const std::string&, uint64_t, bool);
        bool setIntegerProperty(const std::string&, uint64_t, int64_t);
        bool setDoubleProperty(const std::string&, uint64_t, double);
        bool setStringProperty(const std::string&, uint64_t, const std::string&);

        bool setListOfBooleanProperty(const std::string&, uint64_t, const std::vector<bool>&);
        bool setListOfIntegerProperty(const std::string&, uint64_t, const std::vector<int64_t>&);
        bool setListOfDoubleProperty(const std::string&, uint64_t, const std::vector<double>&);
        bool setListOfStringProperty(const std::string&, uint64_t, const std::vector<std::string>&);

        std::map<std::string, std::any> getProperties(uint64_t);
        std::any getProperty(const std::string&, uint64_t);
        bool isDeleted(const std::string&, uint64_t);

        bool getBooleanProperty(const std::string&, uint64_t);
        int64_t getIntegerProperty(const std::string&, uint64_t);
        double getDoubleProperty(const std::string&, uint64_t);
        std::string getStringProperty(const std::string&, uint64_t);

        std::vector<bool> getListOfBooleanProperty(const std::string&, uint64_t);
        std::vector<int64_t> getListOfIntegerProperty(const std::string&, uint64_t);
        std::vector<double> getListOfDoubleProperty(const std::string&, uint64_t);
        std::vector<std::string> getListOfStringProperty(const std::string&, uint64_t);

        bool deleteProperty(const std::string&, uint64_t);
        bool deleteProperties(uint64_t);

        bool static isBooleanProperty(std::any value);
        bool static isIntegerProperty(std::any value);
        bool static isDoubleProperty(std::any value);
        bool static isStringProperty(std::any value);
        bool static isBooleanListProperty(std::any value);
        bool static isIntegerListProperty(std::any value);
        bool static isDoubleListProperty(std::any value);
        bool static isStringListProperty(std::any value);

        constexpr static uint16_t getBooleanPropertyType() {
            return 1;
        }
        constexpr static uint16_t getIntegerPropertyType() {
            return 2;
        }
        constexpr static uint16_t getDoublePropertyType() {
            return 3;
        }
        constexpr static uint16_t getStringPropertyType() {
            return 4;
        }
        constexpr static uint16_t getBooleanListPropertyType() {
            return 5;
        }
        constexpr static uint16_t getIntegerListPropertyType() {
            return 6;
        }
        constexpr static uint16_t getDoubleListPropertyType() {
            return 7;
        }
        constexpr static uint16_t getStringListPropertyType() {
            return 8;
        }

    };
}


#endif //RAGEDB_PROPERTIES_H
