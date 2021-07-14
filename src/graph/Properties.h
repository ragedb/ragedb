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

namespace ragedb {
    class Properties {
    private:
        tsl::sparse_map<std::string, uint8_t> types;
        tsl::sparse_map<std::string, uint8_t> type_map;
        tsl::sparse_map<std::string, std::vector<bool>> booleans;
        tsl::sparse_map<std::string, std::vector<int64_t>> integers;
        tsl::sparse_map<std::string, std::vector<double>> doubles;
        tsl::sparse_map<std::string, std::vector<std::string>> strings;
        tsl::sparse_map<std::string, std::vector<std::vector<bool>>> list_of_booleans;
        tsl::sparse_map<std::string, std::vector<std::vector<int64_t>>> list_of_integers;
        tsl::sparse_map<std::string, std::vector<std::vector<double>>> list_of_doubles;
        tsl::sparse_map<std::string, std::vector<std::vector<std::string>>> list_of_strings;
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

    public:
        Properties();
        seastar::rwlock property_type_lock;

        uint8_t setPropertyTypeId(const std::string& key, uint8_t property_type_id);
        uint8_t setPropertyType(const std::string& key, const std::string& type);

        tsl::sparse_map<std::string, std::vector<bool>> & getAllBooleanProperties();
        bool getBooleanProperty(const std::string&, uint64_t);
        int64_t getIntegerProperty(const std::string&, uint64_t);
        double getDoubleProperty(const std::string&, uint64_t);
        std::string getStringProperty(const std::string&, uint64_t);

        std::vector<bool> getListOfBooleanProperty(const std::string&, uint64_t);
        std::vector<int64_t> getListOfIntegerProperty(const std::string&, uint64_t);
        std::vector<double> getListOfDoubleProperty(const std::string&, uint64_t);
        std::vector<std::string> getListOfStringProperty(const std::string&, uint64_t);

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
        bool setProperty(const std::string&, uint64_t, bool);
        bool setProperty(const std::string&, uint64_t, int64_t);
        bool setProperty(const std::string&, uint64_t, double);
        bool setProperty(const std::string&, uint64_t, std::string);

    };
}


#endif //RAGEDB_PROPERTIES_H
