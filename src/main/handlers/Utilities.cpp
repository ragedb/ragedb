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

#include "Utilities.h"

bool Utilities::validate_parameter(const sstring &parameter, std::unique_ptr<request> &req, std::unique_ptr<reply> &rep, std::string message) {
    bool valid_type = req->param.exists(parameter);
    if (!valid_type) {
        rep->write_body("json", json::stream_object(std::move(message)));
        rep->set_status(reply::status_type::bad_request);
    }
    return valid_type;
}

uint64_t Utilities::validate_id(const std::unique_ptr<request> &req, std::unique_ptr<reply> &rep) {
    // Validate id is unsigned long long
    uint64_t id = 0;

    try {
        id = std::stoull(req->param["id"]);
    } catch (std::exception& e) {
        rep->write_body("json", json::stream_object("Invalid id"));
        rep->set_status(reply::status_type::bad_request);
        return 0;
    }

    return id;
}

uint64_t Utilities::validate_id2(const std::unique_ptr<request> &req, std::unique_ptr<reply> &rep) {
    // Validate id is unsigned long long
    uint64_t id = 0;

    try {
        id = std::stoull(req->param["id2"]);
    } catch (std::exception& e) {
        rep->write_body("json", json::stream_object("Invalid id2"));
        rep->set_status(reply::status_type::bad_request);
        return 0;
    }

    return id;
}

uint64_t Utilities::validate_limit(std::unique_ptr<request> &req, std::unique_ptr<reply> &rep) {
    // Validate limit is unsigned long long

    sstring limit_param = req->get_query_param("limit");
    if (limit_param.empty()) {
        return 100;
    }
    try {
        uint64_t limit = std::stoull(limit_param);
        return limit;
    } catch (std::exception& e) {
        rep->write_body("json", json::stream_object("Invalid limit parameter"));
        rep->set_status(reply::status_type::bad_request);
        return 0;
    }
}

uint64_t Utilities::validate_offset(std::unique_ptr<request> &req, std::unique_ptr<reply> &rep) {
    // Validate offset is unsigned long long

    sstring offset_param = req->get_query_param("offset");
    if (offset_param.empty()) {
        return 0;
    }

    try {
        uint64_t offset = std::stoull(offset_param);
        return offset;
    } catch (std::exception& e) {
        rep->write_body("json", json::stream_object("Invalid offset parameter"));
        rep->set_status(reply::status_type::bad_request);
        return 0;
    }
}

void Utilities::convert_property_to_json(std::unique_ptr<reply> &rep, const std::any &property) {
    if(property.type() == typeid(std::string)) {
        rep->write_body("json", json::stream_object(std::any_cast<std::string>(property)));
    }

    if(property.type() == typeid(int64_t)) {
        rep->write_body("json", json::stream_object(std::any_cast<int64_t>(property)));
    }

    if(property.type() == typeid(double)) {
        rep->write_body("json", json::stream_object(std::any_cast<double>(property)));
    }

    if(property.type() == typeid(bool)) {
        rep->write_body("json", json::stream_object(std::any_cast<bool>(property)));
    }
}

static simdjson::dom::parser parser;

bool Utilities::validate_json(const std::unique_ptr<request> &req, std::unique_ptr<reply> &rep) {
    simdjson::dom::object object;
    simdjson::error_code error = parser.parse(req->content).get(object);
    if (error) {
        rep->write_body("json", json::stream_object("Invalid JSON"));
        rep->set_status(reply::status_type::bad_request);
    }
    return !error;
}