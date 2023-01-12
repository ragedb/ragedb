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

#ifndef RAGEDB_UTILITIES_H
#define RAGEDB_UTILITIES_H

#include <algorithm>
#include <Graph.h>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string.hpp>

class Utilities {

private:
    static std::vector<simdjson::dom::parser> parsers;

public:
    Utilities();

    static inline const seastar::sstring PROPERTY = seastar::sstring ("property");
    static inline const seastar::sstring DATA_TYPE = seastar::sstring ("data_type");
    static inline const seastar::sstring TYPE = seastar::sstring ("type");
    static inline const seastar::sstring KEY = seastar::sstring ("key");
    static inline const seastar::sstring ID = seastar::sstring ("id");
    static inline const seastar::sstring ID2 = seastar::sstring ("id2");
    static inline const seastar::sstring TYPE2 = seastar::sstring ("type2");
    static inline const seastar::sstring KEY2 = seastar::sstring ("key2");
    static inline const seastar::sstring REL_TYPE = seastar::sstring ("rel_type");
    static inline const seastar::sstring OPTIONS = seastar::sstring ("options");
    static inline const std::unordered_set<std::string> allowed_types = {"boolean", "integer", "double", "string", "boolean_list", "integer_list", "double_list", "string_list"};

    static bool validate_parameter(const seastar::sstring& parameter, std::unique_ptr<seastar::http::request> &req, std::unique_ptr<seastar::http::reply> &rep, std::string message);
    static uint64_t validate_id(const std::unique_ptr<seastar::http::request> &req, std::unique_ptr<seastar::http::reply> &rep);
    static uint64_t validate_id2(const std::unique_ptr<seastar::http::request> &req, std::unique_ptr<seastar::http::reply> &rep);
    static uint64_t validate_limit(std::unique_ptr<seastar::http::request> &req, std::unique_ptr<seastar::http::reply> &rep);
    static uint64_t validate_skip(std::unique_ptr<seastar::http::request> &req, std::unique_ptr<seastar::http::reply> &rep);
    static ragedb::Operation validate_operation(std::unique_ptr<seastar::http::request> &req, std::unique_ptr<seastar::http::reply> &rep);
    static bool validate_json(const std::unique_ptr<seastar::http::request> &req, std::unique_ptr<seastar::http::reply> &rep);
    static ragedb::property_type_t validate_json_property(const std::unique_ptr<seastar::http::request> &req, std::unique_ptr<seastar::http::reply> &rep);
    static bool validate_combination(const ragedb::Operation& operation, const ragedb::property_type_t &property);
    static bool validate_allowed_data_type(std::unique_ptr<seastar::http::request> &req, std::unique_ptr<seastar::http::reply> &rep);
    static void convert_property_to_json(std::unique_ptr<seastar::http::reply> &rep, const ragedb::property_type_t &property);
    static double convert_parameter_to_double(const seastar::sstring& parameter, const std::unique_ptr<seastar::http::request> &req, std::unique_ptr<seastar::http::reply> &rep);

};


#endif //RAGEDB_UTILITIES_H
