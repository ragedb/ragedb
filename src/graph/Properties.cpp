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

#include "Properties.h"

namespace ragedb {

    Properties::Properties() = default;

    const static uint8_t boolean_type = 1;
    const static uint8_t integer_type = 2;
    const static uint8_t double_type = 3;
    const static uint8_t string_type = 4;
    const static uint8_t list_of_booleans_type = 5;
    const static uint8_t list_of_integers_type = 6;
    const static uint8_t list_of_doubles_type = 7;
    const static uint8_t list_of_strings_type = 8;

    tsl::sparse_map<std::string, uint8_t> type_map = {
        {"boolean", boolean_type},
        {"integer", integer_type},
        {"double", double_type},
        {"string", string_type},
        {"boolean[]", list_of_booleans_type},
        {"integer[]", list_of_integers_type},
        {"double[]", list_of_doubles_type},
        {"string[]", list_of_strings_type}
    };

    uint8_t Properties::setPropertyTypeId(const std::string& key, uint8_t property_type_id) {
        if(types.find(key) != types.end()) {
            if (types[key] == property_type_id) {
                return property_type_id;
            }
            // Type already exists and it is not what we asked for
            return 0;
        }

        types.emplace(key, property_type_id);

        addPropertyTypeVectors(key, property_type_id);

        return property_type_id;
    }

    void Properties::addPropertyTypeVectors(const std::string& key, uint8_t type_id) {
        switch(type_id) {
            case boolean_type: {
                booleans.emplace(key, std::vector<bool>());
                break;
            }
            case integer_type: {
                integers.emplace(key, std::vector<int64_t>());
                break;
            }
            case double_type: {
                doubles.emplace(key, std::vector<double>());
                break;
            }
            case string_type: {
                strings.emplace(key, std::vector<std::string>());
                break;
            }
            case list_of_booleans_type: {
                list_of_booleans.emplace(key, std::vector<std::vector<bool>>());
                break;
            }
            case list_of_integers_type: {
                list_of_integers.emplace(key, std::vector<std::vector<int64_t>>());
                break;
            }
            case list_of_doubles_type: {
                list_of_doubles.emplace(key, std::vector<std::vector<double>>());
                break;
            }
            case list_of_strings_type: {
                list_of_strings.emplace(key, std::vector<std::vector<std::string>>());
                break;
            }
            default: {
                return;
            }
        }
    }

    uint8_t Properties::setPropertyType(const std::string& key, const std::string& type) {
        // What type do we want?
        uint8_t type_id = 0;
        auto type_search = type_map.find(type);
        if (type_search != type_map.end()) {
            type_id = type_map[type];
        }

        if (type_id == 0) {
            // We have a bad type
            return 0;
        }

        // See if we already have it set
        if(types.find(key) != types.end()) {
            if (types[key] == type_id) {
                return type_id;
            }
            // Type already exists and it is not what we asked for
            return 0;
        }

        types.emplace(key, type_id);

        addPropertyTypeVectors(key, type_id);

        return type_id;
    }

    tsl::sparse_map<std::string, std::vector<bool>> & Properties::getAllBooleanProperties() {
        return booleans;
    }

    bool Properties::getBooleanProperty(const std::string& key, uint64_t index) {
        auto search = booleans.find(key);

        if (search != booleans.end()) {
            if (index < booleans[key].size() ) {
                return booleans[key][index];
            }
        }
        return tombstone_boolean;
    }

    int64_t Properties::getIntegerProperty(const std::string& key, uint64_t index) {
        auto search = integers.find(key);

        if (search != integers.end()) {
            if (index < integers[key].size() ) {
                return integers[key][index];
            }
        }
        return tombstone_int;
    }

    double Properties::getDoubleProperty(const std::string& key, uint64_t index) {
        auto search = doubles.find(key);

        if (search != doubles.end()) {
            if (index < doubles[key].size() ) {
                return doubles[key][index];
            }
        }
        return tombstone_double;
    }

    std::string Properties::getStringProperty(const std::string& key, uint64_t index) {
        auto search = strings.find(key);

        if (search != strings.end()) {
            if (index < strings[key].size() ) {
                return strings[key][index];
            }
        }
        return tombstone_string;
    }

    std::vector<bool> Properties::getListOfBooleanProperty(const std::string& key, uint64_t index) {
        auto search = list_of_booleans.find(key);

        if (search != list_of_booleans.end()) {
            if (index < list_of_booleans[key].size() ) {
                return list_of_booleans[key][index];
            }
        }
        return tombstone_list_of_booleans;
    }

    std::vector<int64_t> Properties::getListOfIntegerProperty(const std::string& key, uint64_t index) {
        auto search = list_of_integers.find(key);

        if (search != list_of_integers.end()) {
            if (index < list_of_integers[key].size() ) {
                return list_of_integers[key][index];
            }
        }
        return tombstone_list_of_ints;
    }

    std::vector<double> Properties::getListOfDoubleProperty(const std::string& key, uint64_t index) {
        auto search = list_of_doubles.find(key);

        if (search != list_of_doubles.end()) {
            if (index < list_of_doubles[key].size() ) {
                return list_of_doubles[key][index];
            }
        }
        return tombstone_list_of_doubles;
    }

    std::vector<std::string> Properties::getListOfStringProperty(const std::string& key, uint64_t index) {
        auto search = list_of_strings.find(key);

        if (search != list_of_strings.end()) {
            if (index < list_of_strings[key].size() ) {
                return list_of_strings[key][index];
            }
        }
        return tombstone_list_of_strings;
    }

    bool Properties::setBooleanProperty(const std::string& key, uint64_t index, bool value) {
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != boolean_type) {
            return false;
        }

        if (booleans[key].size() < index) {
             booleans[key].resize(1 + index);
        }
        booleans[key][index] = value;

        return true;
    }

    bool Properties::setIntegerProperty(const std::string& key, uint64_t index, int64_t value) {
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != integer_type) {
            return false;
        }

        if (integers[key].size() < index) {
            integers[key].resize(1 + index);
        }
        integers[key][index] = value;

        return true;
    }

    bool Properties::setDoubleProperty(const std::string& key, uint64_t index, double value) {
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != double_type) {
            return false;
        }

        if (doubles[key].size() < index) {
            doubles[key].resize(1 + index);
        }
        doubles[key][index] = value;

        return true;
    }
    bool Properties::setStringProperty(const std::string& key, uint64_t index, const std::string& value) {
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != string_type) {
            return false;
        }

        if (strings[key].size() < index) {
            strings[key].resize(1 + index);
        }
        strings[key][index] = value;

        return true;
    }

    bool Properties::setListOfBooleanProperty(const std::string& key, uint64_t index, const std::vector<bool>& value) {
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != list_of_booleans_type) {
            return false;
        }

        if (list_of_booleans[key].size() < index) {
            list_of_booleans[key].resize(1 + index);
        }
        list_of_booleans[key][index] = value;

        return true;
    }

    bool Properties::setListOfIntegerProperty(const std::string& key, uint64_t index, const std::vector<int64_t>& value) {
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != list_of_integers_type) {
            return false;
        }

        if (list_of_integers[key].size() < index) {
            list_of_integers[key].resize(1 + index);
        }
        list_of_integers[key][index] = value;

        return true;
    }

    bool Properties::setListOfDoubleProperty(const std::string& key, uint64_t index, const std::vector<double>& value) {
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != list_of_doubles_type) {
            return false;
        }

        if (list_of_doubles[key].size() < index) {
            list_of_doubles[key].resize(1 + index);
        }
        list_of_doubles[key][index] = value;

        return true;
    }

    bool Properties::setListOfStringProperty(const std::string& key, uint64_t index, const std::vector<std::string>& value) {
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != list_of_strings_type) {
            return false;
        }

        if (list_of_strings[key].size() < index) {
            list_of_strings[key].resize(1 + index);
        }
        list_of_strings[key][index] = value;

        return true;
    }

}