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

#include <cstdint>
#include <map>
#include <tsl/sparse_map.h>
#include <seastar/core/rwlock.hh>
#include <roaring/roaring64map.hh>
#include "PropertyType.h"

namespace ragedb {
    class Properties {

    private:
        tsl::sparse_map<std::string, uint8_t> types;
        tsl::sparse_map<std::string, uint8_t> type_map = {
          {"boolean",      boolean_type},
          {"integer",      integer_type},
          {"double",       double_type},
          {"string",       string_type},
          {"boolean_list", boolean_list_type},
          {"integer_list", integer_list_type},
          {"double_list",  double_list_type},
          {"string_list",  string_list_type},
          {"date",         date_type},
          {"date_list",    date_list_type}
        };
        std::vector<std::string> allowed_types = { "", "boolean", "integer", "double", "string", "boolean_list", "integer_list", "double_list", "string_list", "date", "date_list" };
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
        Properties (Properties && _properties) noexcept = default;
        void clear();
        seastar::rwlock property_type_lock;

        std::map<std::string, std::string> getPropertyTypes();
        std::string getPropertyType(const std::string& key) const;
        uint8_t getPropertyTypeId(const std::string& key) const;
        uint8_t setPropertyTypeId(const std::string& key, uint8_t property_type_id);
        uint8_t setPropertyType(const std::string& key, const std::string& type);
        bool removePropertyType(const std::string& key);

        std::string getPropertyKey(uint8_t) const;
        bool validType(const std::string &key);
        bool setBooleanProperty(const std::string&, uint64_t, bool);
        bool setIntegerProperty(const std::string&, uint64_t, int64_t);
        bool setDoubleProperty(const std::string&, uint64_t, double);
        bool setDateProperty(const std::string &key, uint64_t index, double value);
        bool setStringProperty(const std::string&, uint64_t, const std::string&);

        bool setListOfBooleanProperty(const std::string&, uint64_t, const std::vector<bool>&);
        bool setListOfIntegerProperty(const std::string&, uint64_t, const std::vector<int64_t>&);
        bool setListOfDoubleProperty(const std::string&, uint64_t, const std::vector<double>&);
        bool setListOfDateProperty(const std::string&, uint64_t, const std::vector<double>&);
        bool setListOfStringProperty(const std::string&, uint64_t, const std::vector<std::string>&);

        std::map<std::string, property_type_t> getProperties(uint64_t) const;
        std::map<uint64_t, property_type_t> getProperty(const std::vector<uint64_t> &external_ids, const std::vector<uint64_t> &internal_ids, const std::string& key) const;
        std::map<uint64_t, std::map<std::string, property_type_t>> getProperties(const std::vector<uint64_t> &external_ids, const std::vector<uint64_t> &internal_ids) const;
        property_type_t getProperty(const std::string&, uint64_t);
        bool isDeleted(const std::string&, uint64_t);
        uint64_t getDeletedCount(const std::string&);
        roaring::Roaring64Map& getDeletedMap(const std::string&);

        std::vector<bool>& getBooleans(const std::string&);
        std::vector<int64_t>& getIntegers(const std::string&);
        std::vector<double>& getDoubles(const std::string&);
        std::vector<std::string>& getStrings(const std::string&);
        std::vector<std::vector<bool>>& getBooleansList(const std::string&);
        std::vector<std::vector<int64_t>>& getIntegersList(const std::string&);
        std::vector<std::vector<double>>& getDoublesList(const std::string&);
        std::vector<std::vector<std::string>>& getStringsList(const std::string&);

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

        bool static isBooleanProperty(const property_type_t& value);
        bool static isIntegerProperty(const property_type_t& value);
        bool static isDoubleProperty(const property_type_t& value);
        bool static isStringProperty(const property_type_t& value);
        bool static isBooleanListProperty(const property_type_t& value);
        bool static isIntegerListProperty(const property_type_t& value);
        bool static isDoubleListProperty(const property_type_t& value);
        bool static isStringListProperty(const property_type_t& value);

        inline const static uint8_t boolean_type = 1;
        inline const static uint8_t integer_type = 2;
        inline const static uint8_t double_type = 3;
        inline const static uint8_t string_type = 4;
        inline const static uint8_t boolean_list_type = 5;
        inline const static uint8_t integer_list_type = 6;
        inline const static uint8_t double_list_type = 7;
        inline const static uint8_t string_list_type = 8;

        inline const static uint8_t date_type = 9;
        inline const static uint8_t date_list_type = 10;

    };
}


#endif //RAGEDB_PROPERTIES_H
