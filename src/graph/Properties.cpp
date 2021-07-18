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

    const static uint8_t boolean_type = 1;
    const static uint8_t integer_type = 2;
    const static uint8_t double_type = 3;
    const static uint8_t string_type = 4;
    const static uint8_t boolean_list_type = 5;
    const static uint8_t integer_list_type = 6;
    const static uint8_t double_list_type = 7;
    const static uint8_t string_list_type = 8;

    Properties::Properties()  {
        type_map = {
                {"boolean",      boolean_type},
                {"integer",      integer_type},
                {"double",       double_type},
                {"string",       string_type},
                {"boolean_list", boolean_list_type},
                {"integer_list", integer_list_type},
                {"double_list",  double_list_type},
                {"string_list", string_list_type}
        };

        allowed_types = {"", "boolean", "integer", "double", "string", "boolean_list", "integer_list", "double_list", "string_list"};

        types.insert({"", 0});
    }

    void Properties::clear() {
        types.clear();
        types.insert({"", 0});
        booleans.clear();
        integers.clear();
        doubles.clear();
        strings.clear();
        booleans_list.clear();
        integers_list.clear();
        doubles_list.clear();
        strings_list.clear();
    }

    std::map<std::string, std::string> Properties::getPropertyTypes() {
        std::map<std::string, std::string> map;
        for (auto [type, type_id]: types) {
            map.insert({type, allowed_types[type_id]});
        }
        map.erase("");
        return map;
    }

    uint8_t Properties::setPropertyTypeId(const std::string &key, uint8_t property_type_id) {
        if (types.find(key) != types.end()) {
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

    void Properties::addPropertyTypeVectors(const std::string &key, uint8_t type_id) {
        switch (type_id) {
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
            case boolean_list_type: {
                booleans_list.emplace(key, std::vector<std::vector<bool>>());
                break;
            }
            case integer_list_type: {
                integers_list.emplace(key, std::vector<std::vector<int64_t>>());
                break;
            }
            case double_list_type: {
                doubles_list.emplace(key, std::vector<std::vector<double>>());
                break;
            }
            case string_list_type: {
                strings_list.emplace(key, std::vector<std::vector<std::string>>());
                break;
            }
            default: {
                return;
            }
        }
    }

    void Properties::removePropertyTypeVectors(const std::string &key, uint8_t type_id) {
        switch (type_id) {
            case boolean_type: {
                booleans.erase(key);
                break;
            }
            case integer_type: {
                integers.erase(key);
                break;
            }
            case double_type: {
                doubles.erase(key);
                break;
            }
            case string_type: {
                strings.erase(key);
                break;
            }
            case boolean_list_type: {
                booleans_list.erase(key);
                break;
            }
            case integer_list_type: {
                integers_list.erase(key);
                break;
            }
            case double_list_type: {
                doubles_list.erase(key);
                break;
            }
            case string_list_type: {
                strings_list.erase(key);
                break;
            }
            default: {
                return;
            }
        }
    }

    uint8_t Properties::setPropertyType(const std::string &key, const std::string &type) {
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
        if (types.find(key) != types.end()) {
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

    bool Properties::removePropertyType(const std::string& key) {
        if (types.find(key) != types.end()) {
            removePropertyTypeVectors(key, types[key]);
            types.erase(key);
        }
        return false;
    }

    tsl::sparse_map<std::string, std::vector<bool>> &Properties::getAllBooleanProperties() {
        return booleans;
    }

    bool Properties::getBooleanProperty(const std::string &key, uint64_t index) {
        auto search = booleans.find(key);

        if (search != booleans.end()) {
            if (index < booleans[key].size()) {
                return booleans[key][index];
            }
        }
        return tombstone_boolean;
    }

    int64_t Properties::getIntegerProperty(const std::string &key, uint64_t index) {
        auto search = integers.find(key);

        if (search != integers.end()) {
            if (index < integers[key].size()) {
                return integers[key][index];
            }
        }
        return tombstone_int;
    }

    double Properties::getDoubleProperty(const std::string &key, uint64_t index) {
        auto search = doubles.find(key);

        if (search != doubles.end()) {
            if (index < doubles[key].size()) {
                return doubles[key][index];
            }
        }
        return tombstone_double;
    }

    std::string Properties::getStringProperty(const std::string &key, uint64_t index) {
        auto search = strings.find(key);

        if (search != strings.end()) {
            if (index < strings[key].size()) {
                return strings[key][index];
            }
        }
        return tombstone_string;
    }

    std::vector<bool> Properties::getListOfBooleanProperty(const std::string &key, uint64_t index) {
        auto search = booleans_list.find(key);

        if (search != booleans_list.end()) {
            if (index < booleans_list[key].size()) {
                return booleans_list[key][index];
            }
        }
        return tombstone_booleans_list;
    }

    std::vector<int64_t> Properties::getListOfIntegerProperty(const std::string &key, uint64_t index) {
        auto search = integers_list.find(key);

        if (search != integers_list.end()) {
            if (index < integers_list[key].size()) {
                return integers_list[key][index];
            }
        }
        return tombstone_integers_list;
    }

    std::vector<double> Properties::getListOfDoubleProperty(const std::string &key, uint64_t index) {
        auto search = doubles_list.find(key);

        if (search != doubles_list.end()) {
            if (index < doubles_list[key].size()) {
                return doubles_list[key][index];
            }
        }
        return tombstone_doubles_list;
    }

    std::vector<std::string> Properties::getListOfStringProperty(const std::string &key, uint64_t index) {
        auto search = strings_list.find(key);

        if (search != strings_list.end()) {
            if (index < strings_list[key].size()) {
                return strings_list[key][index];
            }
        }
        return tombstone_strings_list;
    }

    bool Properties::setBooleanProperty(const std::string &key, uint64_t index, bool value) {
        if(types.empty()) {
            return false;
        }
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != boolean_type) {
            return false;
        }

        if (booleans[key].size() <= index) {
            booleans[key].resize(1 + index);
        }
        booleans[key][index] = value;

        return true;
    }

    bool Properties::setIntegerProperty(const std::string &key, uint64_t index, int64_t value) {
        if(types.empty()) {
            return false;
        }
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != integer_type) {
            return false;
        }

        if (integers[key].size() <= index) {
            integers[key].resize(1 + index);
        }
        integers[key][index] = value;

        return true;
    }

    bool Properties::setDoubleProperty(const std::string &key, uint64_t index, double value) {
        if(types.empty()) {
            return false;
        }
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != double_type) {
            return false;
        }

        if (doubles[key].size() <= index) {
            doubles[key].resize(1 + index);
        }
        doubles[key][index] = value;

        return true;
    }

    bool Properties::setStringProperty(const std::string &key, uint64_t index, const std::string &value) {
        if(types.empty()) {
            return false;
        }
        if (types.find(key) == types.end()) {
            return false;
        }
        if (types[key] != string_type) {
            return false;
        }

        if (strings[key].size() <= index) {
            strings[key].resize(1 + index);
        }
        strings[key][index] = value;

        return true;
    }

    bool Properties::setListOfBooleanProperty(const std::string &key, uint64_t index, const std::vector<bool> &value) {
        if(types.empty()) {
            return false;
        }
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != boolean_list_type) {
            return false;
        }

        if (booleans_list[key].size() <= index) {
            booleans_list[key].resize(1 + index);
        }
        booleans_list[key][index] = value;

        return true;
    }

    bool
    Properties::setListOfIntegerProperty(const std::string &key, uint64_t index, const std::vector<int64_t> &value) {
        if(types.empty()) {
            return false;
        }
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != integer_list_type) {
            return false;
        }

        if (integers_list[key].size() <= index) {
            integers_list[key].resize(1 + index);
        }
        integers_list[key][index] = value;

        return true;
    }

    bool Properties::setListOfDoubleProperty(const std::string &key, uint64_t index, const std::vector<double> &value) {
        if(types.empty()) {
            return false;
        }
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != double_list_type) {
            return false;
        }

        if (doubles_list[key].size() <= index) {
            doubles_list[key].resize(1 + index);
        }
        doubles_list[key][index] = value;

        return true;
    }

    bool
    Properties::setListOfStringProperty(const std::string &key, uint64_t index, const std::vector<std::string> &value) {
        if(types.empty()) {
            return false;
        }
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != string_list_type) {
            return false;
        }

        if (strings_list[key].size() <= index) {
            strings_list[key].resize(1 + index);
        }
        strings_list[key][index] = value;

        return true;
    }

    std::map<std::string, std::any> Properties::getProperties(uint64_t index) {
        std::map<std::string, std::any> properties;
        for (auto const&[key, type_id] : types) {
            switch (type_id) {
                case boolean_type: {
                    if(booleans[key].size() > index) {
                        properties.emplace(key, booleans[key][index]);
                    }
                    break;
                }
                case integer_type: {
                    if(integers[key].size() > index) {
                        properties.emplace(key, integers[key][index]);
                    }
                    break;
                }
                case double_type: {
                    if(doubles[key].size() > index) {
                        properties.emplace(key, doubles[key][index]);
                    }
                    break;
                }
                case string_type: {
                    if(strings[key].size() > index) {
                        properties.emplace(key, strings[key][index]);
                    }
                    break;
                }
                case boolean_list_type: {
                    if(booleans_list[key].size() > index) {
                        properties.emplace(key, booleans_list[key][index]);
                    }
                    break;
                }
                case integer_list_type: {
                    if(integers_list[key].size() > index) {
                        properties.emplace(key, integers_list[key][index]);
                    }
                    break;
                }
                case double_list_type: {
                    if(doubles_list[key].size() > index) {
                        properties.emplace(key, doubles_list[key][index]);
                    }
                    break;
                }
                case string_list_type: {
                    if(strings_list[key].size() > index) {
                        properties.emplace(key, strings_list[key][index]);
                    }
                    break;
                }
                default: {

                }
            }

        }
        return properties;
    }

    std::any Properties::getProperty(const std::string& key, uint64_t index) {
        if (types.find(key) != types.end()) {
            switch (types[key]) {
                case boolean_type: {
                    return booleans[key][index];
                }
                case integer_type: {
                    return integers[key][index];
                }
                case double_type: {
                    return doubles[key][index];
                }
                case string_type: {
                    return strings[key][index];
                }
                case boolean_list_type: {
                    return booleans_list[key][index];
                }
                case integer_list_type: {
                    return integers_list[key][index];
                }
                case double_list_type: {
                    return doubles_list[key][index];
                }
                case string_list_type: {
                    return strings_list[key][index];
                }
                default: {

                }
            }
        }
        return tombstone_any;
    }

    bool Properties::setProperty(const std::string &key, uint64_t index, bool value) {
        return setBooleanProperty(key, index, value);
    }

    bool Properties::setProperty(const std::string &key, uint64_t index, int64_t value) {
        return setIntegerProperty(key, index, value);
    }

    bool Properties::setProperty(const std::string &key, uint64_t index, double value) {
        return setDoubleProperty(key, index, value);
    }

    bool Properties::setProperty(const std::string &key, uint64_t index, std::string value) {
        return setStringProperty(key, index, value);
    }

    bool Properties::deleteProperties(uint64_t index) {
        for (auto[key, value] : types) {
            switch (types[key]) {
                case boolean_type: {
                    booleans[key][index] = tombstone_boolean;
                    break;
                }
                case integer_type: {
                    integers[key][index] = tombstone_int;
                    break;
                }
                case double_type: {
                    doubles[key][index] = tombstone_double;
                    break;
                }
                case string_type: {
                    strings[key][index] = tombstone_string;
                    break;
                }
                case boolean_list_type: {
                    booleans_list[key][index] = tombstone_booleans_list;
                    break;
                }
                case integer_list_type: {
                    integers_list[key][index] =  tombstone_integers_list;
                    break;
                }
                case double_list_type: {
                    doubles_list[key][index] = tombstone_doubles_list;
                    break;
                }
                case string_list_type: {
                    strings_list[key][index] = tombstone_strings_list;
                    break;
                }
                default: {
                    return false;
                }
            }
        }
        return true;
    }

    bool Properties::deleteProperty(const std::string& key, uint64_t index) {
        if (types.find(key) != types.end()) {
            switch (types[key]) {
                case boolean_type: {
                    booleans[key][index] = tombstone_boolean;
                    break;
                }
                case integer_type: {
                    integers[key][index] = tombstone_int;
                    break;
                }
                case double_type: {
                    doubles[key][index] = tombstone_double;
                    break;
                }
                case string_type: {
                    strings[key][index] = tombstone_string;
                    break;
                }
                case boolean_list_type: {
                    booleans_list[key][index] = tombstone_booleans_list;
                    break;
                }
                case integer_list_type: {
                    integers_list[key][index] =  tombstone_integers_list;
                    break;
                }
                case double_list_type: {
                    doubles_list[key][index] = tombstone_doubles_list;
                    break;
                }
                case string_list_type: {
                    strings_list[key][index] = tombstone_strings_list;
                    break;
                }
                default: {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

}