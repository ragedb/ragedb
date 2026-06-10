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

#ifndef RAGEDB_PROPERTYINDEX_H
#define RAGEDB_PROPERTYINDEX_H

#include <map>
#include <vector>
#include <string>
#include <variant>
#include <algorithm>
#include <cstring>
#include "PropertyType.h"
#include "Operation.h"
#include "art.hpp"

namespace ragedb {

    class PropertyIndex {
    public:
        PropertyIndex() = default;
        
        ~PropertyIndex() {
            clear();
        }

        PropertyIndex(const PropertyIndex&) = delete;
        PropertyIndex& operator=(const PropertyIndex&) = delete;
        PropertyIndex(PropertyIndex&& other) noexcept = default;
        PropertyIndex& operator=(PropertyIndex&& other) noexcept = default;

        void clear() {
            true_index.clear();
            false_index.clear();
            int_index.clear();
            double_index.clear();
            
            // Delete heap-allocated vectors in string index
            string_index.scan([](const auto& visitor) {
                auto* vec = visitor.get_value();
                delete vec;
                return false; // continue scan
            });
            string_index.clear();
        }

        void insert(const property_type_t& value, uint64_t external_id) {
            if (value.index() == 1) { // bool
                if (std::get<bool>(value)) {
                    true_index.add(external_id);
                } else {
                    false_index.add(external_id);
                }
            } else if (value.index() == 2) { // int64_t
                int_index.insert({std::get<int64_t>(value), external_id});
            } else if (value.index() == 3) { // double
                double_index.insert({std::get<double>(value), external_id});
            } else if (value.index() == 4) { // std::string
                const auto& str = std::get<std::string>(value);
                unodb::key_encoder encoder;
                encoder.encode_text(str);
                auto key = encoder.get_key_view();
                
                auto res = string_index.get(key);
                if (res.has_value()) {
                    auto* vec = res.value();
                    // Avoid duplicate external_ids
                    if (std::find(vec->begin(), vec->end(), external_id) == vec->end()) {
                        vec->push_back(external_id);
                    }
                } else {
                    auto* vec = new std::vector<uint64_t>{external_id};
                    string_index.insert(key, vec);
                }
            }
        }

        void remove(const property_type_t& value, uint64_t external_id) {
            if (value.index() == 1) { // bool
                if (std::get<bool>(value)) {
                    true_index.remove(external_id);
                } else {
                    false_index.remove(external_id);
                }
            } else if (value.index() == 2) { // int64_t
                auto range = int_index.equal_range(std::get<int64_t>(value));
                for (auto it = range.first; it != range.second; ++it) {
                    if (it->second == external_id) {
                        int_index.erase(it);
                        break;
                    }
                }
            } else if (value.index() == 3) { // double
                auto range = double_index.equal_range(std::get<double>(value));
                for (auto it = range.first; it != range.second; ++it) {
                    if (it->second == external_id) {
                        double_index.erase(it);
                        break;
                    }
                }
            } else if (value.index() == 4) { // std::string
                const auto& str = std::get<std::string>(value);
                unodb::key_encoder encoder;
                encoder.encode_text(str);
                auto key = encoder.get_key_view();
                
                auto res = string_index.get(key);
                if (res.has_value()) {
                    auto* vec = res.value();
                    auto it = std::find(vec->begin(), vec->end(), external_id);
                    if (it != vec->end()) {
                        vec->erase(it);
                    }
                    if (vec->empty()) {
                        string_index.remove(key);
                        delete vec;
                    }
                }
            }
        }

        std::vector<uint64_t> lookup_exact(const property_type_t& value) {
            std::vector<uint64_t> results;
            if (value.index() == 1) { // bool
                bool b = std::get<bool>(value);
                const auto& bitmap = b ? true_index : false_index;
                results.resize(bitmap.cardinality());
                bitmap.toUint64Array(results.data());
            } else if (value.index() == 2) { // int64_t
                auto range = int_index.equal_range(std::get<int64_t>(value));
                for (auto it = range.first; it != range.second; ++it) {
                    results.push_back(it->second);
                }
            } else if (value.index() == 3) { // double
                auto range = double_index.equal_range(std::get<double>(value));
                for (auto it = range.first; it != range.second; ++it) {
                    results.push_back(it->second);
                }
            } else if (value.index() == 4) { // std::string
                const auto& str = std::get<std::string>(value);
                unodb::key_encoder encoder;
                encoder.encode_text(str);
                auto key = encoder.get_key_view();
                
                auto res = string_index.get(key);
                if (res.has_value()) {
                    results = *res.value();
                }
            }
            return results;
        }

        std::vector<uint64_t> lookup_range(Operation operation, const property_type_t& value) {
            std::vector<uint64_t> results;
            if (value.index() == 1) { // bool
                bool search_val = std::get<bool>(value);
                if (operation == Operation::EQ) {
                    return lookup_exact(value);
                } else if (operation == Operation::NEQ) {
                    const auto& bitmap = search_val ? false_index : true_index;
                    results.resize(bitmap.cardinality());
                    bitmap.toUint64Array(results.data());
                } else if (operation == Operation::LT) {
                    if (search_val) {
                        results.resize(false_index.cardinality());
                        false_index.toUint64Array(results.data());
                    }
                } else if (operation == Operation::LTE) {
                    if (search_val) {
                        roaring::Roaring64Map union_map = true_index;
                        union_map |= false_index;
                        results.resize(union_map.cardinality());
                        union_map.toUint64Array(results.data());
                    } else {
                        results.resize(false_index.cardinality());
                        false_index.toUint64Array(results.data());
                    }
                } else if (operation == Operation::GT) {
                    if (!search_val) {
                        results.resize(true_index.cardinality());
                        true_index.toUint64Array(results.data());
                    }
                } else if (operation == Operation::GTE) {
                    if (!search_val) {
                        roaring::Roaring64Map union_map = true_index;
                        union_map |= false_index;
                        results.resize(union_map.cardinality());
                        union_map.toUint64Array(results.data());
                    } else {
                        results.resize(true_index.cardinality());
                        true_index.toUint64Array(results.data());
                    }
                }
            } else if (value.index() == 2) { // int64_t
                int64_t search_val = std::get<int64_t>(value);
                if (operation == Operation::EQ) {
                    return lookup_exact(value);
                } else if (operation == Operation::NEQ) {
                    for (const auto& [val, id] : int_index) {
                        if (val != search_val) results.push_back(id);
                    }
                } else if (operation == Operation::LT) {
                    auto it_end = int_index.lower_bound(search_val);
                    for (auto it = int_index.begin(); it != it_end; ++it) {
                        results.push_back(it->second);
                    }
                } else if (operation == Operation::LTE) {
                    auto it_end = int_index.upper_bound(search_val);
                    for (auto it = int_index.begin(); it != it_end; ++it) {
                        results.push_back(it->second);
                    }
                } else if (operation == Operation::GT) {
                    auto it_start = int_index.upper_bound(search_val);
                    for (auto it = it_start; it != int_index.end(); ++it) {
                        results.push_back(it->second);
                    }
                } else if (operation == Operation::GTE) {
                    auto it_start = int_index.lower_bound(search_val);
                    for (auto it = it_start; it != int_index.end(); ++it) {
                        results.push_back(it->second);
                    }
                }
            } else if (value.index() == 3) { // double
                double search_val = std::get<double>(value);
                if (operation == Operation::EQ) {
                    return lookup_exact(value);
                } else if (operation == Operation::NEQ) {
                    for (const auto& [val, id] : double_index) {
                        if (val != search_val) results.push_back(id);
                    }
                } else if (operation == Operation::LT) {
                    auto it_end = double_index.lower_bound(search_val);
                    for (auto it = double_index.begin(); it != it_end; ++it) {
                        results.push_back(it->second);
                    }
                } else if (operation == Operation::LTE) {
                    auto it_end = double_index.upper_bound(search_val);
                    for (auto it = double_index.begin(); it != it_end; ++it) {
                        results.push_back(it->second);
                    }
                } else if (operation == Operation::GT) {
                    auto it_start = double_index.upper_bound(search_val);
                    for (auto it = it_start; it != double_index.end(); ++it) {
                        results.push_back(it->second);
                    }
                } else if (operation == Operation::GTE) {
                    auto it_start = double_index.lower_bound(search_val);
                    for (auto it = it_start; it != double_index.end(); ++it) {
                        results.push_back(it->second);
                    }
                }
            } else if (value.index() == 4) { // std::string
                const auto& search_val = std::get<std::string>(value);
                unodb::key_encoder encoder;
                encoder.encode_text(search_val);
                auto search_key = encoder.get_key_view();

                if (operation == Operation::EQ) {
                    return lookup_exact(value);
                } else if (operation == Operation::NEQ) {
                    string_index.scan([&results, &search_key](const auto& visitor) {
                        auto visitor_key = visitor.get_key().view();
                        if (visitor_key.size() != search_key.size() || 
                            std::memcmp(visitor_key.data(), search_key.data(), visitor_key.size()) != 0) {
                            auto* vec = visitor.get_value();
                            results.insert(results.end(), vec->begin(), vec->end());
                        }
                        return false;
                    });
                } else if (operation == Operation::LT) {
                    string_index.scan([&results, &search_key](const auto& visitor) {
                        auto visitor_key = visitor.get_key().view();
                        // Since scan goes in lexicographical order, stop when we reach search_key
                        if (std::lexicographical_compare(search_key.begin(), search_key.end(), visitor_key.begin(), visitor_key.end())) {
                            return true; // Stop scan
                        }
                        // Check if equal
                        if (visitor_key.size() == search_key.size() && 
                            std::memcmp(visitor_key.data(), search_key.data(), visitor_key.size()) == 0) {
                            return true; // Stop scan
                        }
                        auto* vec = visitor.get_value();
                        results.insert(results.end(), vec->begin(), vec->end());
                        return false;
                    });
                } else if (operation == Operation::LTE) {
                    string_index.scan([&results, &search_key](const auto& visitor) {
                        auto visitor_key = visitor.get_key().view();
                        if (std::lexicographical_compare(search_key.begin(), search_key.end(), visitor_key.begin(), visitor_key.end())) {
                            return true; // Stop scan
                        }
                        auto* vec = visitor.get_value();
                        results.insert(results.end(), vec->begin(), vec->end());
                        return false;
                    });
                } else if (operation == Operation::GT) {
                    string_index.scan_from(search_key, [&results, &search_key](const auto& visitor) {
                        auto visitor_key = visitor.get_key().view();
                        if (visitor_key.size() == search_key.size() && 
                            std::memcmp(visitor_key.data(), search_key.data(), visitor_key.size()) == 0) {
                            return false; // Skip equal key
                        }
                        auto* vec = visitor.get_value();
                        results.insert(results.end(), vec->begin(), vec->end());
                        return false;
                    });
                } else if (operation == Operation::GTE) {
                    string_index.scan_from(search_key, [&results](const auto& visitor) {
                        auto* vec = visitor.get_value();
                        results.insert(results.end(), vec->begin(), vec->end());
                        return false;
                    });
                }
            }
            return results;
        }

    private:
        roaring::Roaring64Map true_index;
        roaring::Roaring64Map false_index;
        std::multimap<int64_t, uint64_t> int_index;
        std::multimap<double, uint64_t> double_index;
        unodb::db<unodb::key_view, std::vector<uint64_t>*> string_index;
    };

}

#endif // RAGEDB_PROPERTYINDEX_H
