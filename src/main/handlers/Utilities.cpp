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



bool Utilities::validate_parameter(const seastar::sstring &parameter, std::unique_ptr<seastar::request> &req, std::unique_ptr<seastar::httpd::reply> &rep, std::string message) {
    bool valid_type = req->param.exists(parameter);
    if (!valid_type) {
        rep->write_body("json", seastar::json::stream_object(std::move(message)));
        rep->set_status(seastar::httpd::reply::status_type::bad_request);
    }
    return valid_type;
}

uint64_t Utilities::validate_id(const std::unique_ptr<seastar::request> &req, std::unique_ptr<seastar::httpd::reply> &rep) {
    // Validate id is unsigned long long
    uint64_t id;

    try {
        id = std::stoull(req->param["id"]);
    } catch (std::exception& e) {
        rep->write_body("json", seastar::json::stream_object("Invalid id"));
        rep->set_status(seastar::httpd::reply::status_type::bad_request);
        return 0;
    }

    return id;
}

uint64_t Utilities::validate_id2(const std::unique_ptr<seastar::request> &req, std::unique_ptr<seastar::httpd::reply> &rep) {
    // Validate id is unsigned long long
    uint64_t id;

    try {
        id = std::stoull(req->param["id2"]);
    } catch (std::exception& e) {
        rep->write_body("json", seastar::json::stream_object("Invalid id2"));
        rep->set_status(seastar::httpd::reply::status_type::bad_request);
        return 0;
    }

    return id;
}

uint64_t Utilities::validate_limit(std::unique_ptr<seastar::request> &req, std::unique_ptr<seastar::httpd::reply> &rep) {
    // Validate limit is unsigned long long

    seastar::sstring limit_param = req->get_query_param("limit");
    if (limit_param.empty()) {
        return 100;
    }
    try {
        uint64_t limit = std::stoull(limit_param);
        return limit;
    } catch (std::exception& e) {
        rep->write_body("json", seastar::json::stream_object("Invalid limit parameter"));
        rep->set_status(seastar::httpd::reply::status_type::bad_request);
        return 0;
    }
}

uint64_t Utilities::validate_skip(std::unique_ptr<seastar::request> &req, std::unique_ptr<seastar::httpd::reply> &rep) {
    // Validate skip is unsigned long long

    seastar::sstring skip_param = req->get_query_param("skip");
    if (skip_param.empty()) {
        return 0;
    }

    try {
        uint64_t skip = std::stoull(skip_param);
        return skip;
    } catch (std::exception& e) {
        rep->write_body("json", seastar::json::stream_object("Invalid skip parameter"));
        rep->set_status(seastar::httpd::reply::status_type::bad_request);
        return 0;
    }
}

ragedb::Operation Utilities::validate_operation(std::unique_ptr<seastar::request> &req, std::unique_ptr<seastar::httpd::reply> &rep) {
    // Validate operation is allowed

    seastar::sstring parameter = req->param["operation"];

    static std::unordered_map<std::string,ragedb::Operation> const table = {
            {"EQ", ragedb::Operation::EQ},
            {"NEQ", ragedb::Operation::NEQ},
            {"GT", ragedb::Operation::GT},
            {"GTE", ragedb::Operation::GTE},
            {"LT", ragedb::Operation::LT},
            {"LTE", ragedb::Operation::LTE},
            {"IS_NULL", ragedb::Operation::IS_NULL},
            {"STARTS_WITH", ragedb::Operation::STARTS_WITH},
            {"CONTAINS", ragedb::Operation::CONTAINS},
            {"ENDS_WITH", ragedb::Operation::ENDS_WITH},
            {"NOT_IS_NULL", ragedb::Operation::NOT_IS_NULL},
            {"NOT_STARTS_WITH", ragedb::Operation::NOT_STARTS_WITH},
            {"NOT_CONTAINS", ragedb::Operation::NOT_CONTAINS},
            {"NOT_ENDS_WITH", ragedb::Operation::NOT_ENDS_WITH},
            {"UNKNOWN", ragedb::Operation::UNKNOWN}
    };
    auto it = table.find(parameter);
    if (it != table.end()) {
        return it->second;
    } else {
        rep->write_body("json", seastar::json::stream_object("Invalid operation parameter"));
        rep->set_status(seastar::httpd::reply::status_type::bad_request);
        return ragedb::Operation::UNKNOWN;
    }
}

bool Utilities::validate_combination(const ragedb::Operation& operation, const ragedb::property_type_t &property) {
    if (operation == ragedb::Operation::UNKNOWN) {
        return false;
    }
    if (operation == ragedb::Operation::IS_NULL || operation == ragedb::Operation::NOT_IS_NULL) {
        return true;
    }
    return property.index() > 0;
}

std::vector<std::string> vector_bool_to_string(std::vector<bool> source) {
  std::vector<std::string> return_vector;
  for(bool value : source) {
    return_vector.push_back(value ? "true" : "false");
  }
  return return_vector;
}

void Utilities::convert_property_to_json(std::unique_ptr<seastar::httpd::reply> &rep, const ragedb::property_type_t &property) {

  switch (property.index()) {
    case 0:
      rep->write_body("json", seastar::json::stream_object("null"));
      break;
    case 1:
      rep->write_body("json", seastar::json::stream_object(get<bool>(property)));
      break;
    case 2:
      rep->write_body("json", seastar::json::stream_object(get<int64_t>(property)));
      break;
    case 3:
      rep->write_body("json", seastar::json::stream_object(get<double>(property)));
      break;
    case 4:
      rep->write_body("json", seastar::json::stream_object(get<std::string>(property)));
      break;
    case 5:
      rep->write_body("json", seastar::json::stream_object(vector_bool_to_string(get<std::vector<bool>>(property))));
      break;
    case 6:
      rep->write_body("json", seastar::json::stream_object(get<std::vector<int64_t>>(property)));
      break;
    case 7:
      rep->write_body("json", seastar::json::stream_object(get<std::vector<double>>(property)));
      break;
    case 8:
      rep->write_body("json", seastar::json::stream_object(get<std::vector<std::string>>(property)));
      break;
  }
}

double Utilities::convert_parameter_to_double(const seastar::sstring &parameter, const std::unique_ptr<seastar::request> &req, std::unique_ptr<seastar::httpd::reply> &rep) {
    if (!req->param.exists(parameter)) {
        rep->write_body("json", seastar::json::stream_object("Invalid parameter: " + parameter));
        rep->set_status(seastar::httpd::reply::status_type::bad_request);
    }

    double d;

    try {
        d = std::stod(req->param[parameter]);
    } catch (const std::invalid_argument&) {
        rep->write_body("json", seastar::json::stream_object("Invalid parameter value: " + parameter + " " + req->param[parameter]));
        rep->set_status(seastar::httpd::reply::status_type::bad_request);
        throw;
    } catch (const std::out_of_range&) {
        rep->write_body("json", seastar::json::stream_object("Parameter out of range for a double: " + parameter + " " + req->param[parameter]));
        rep->set_status(seastar::httpd::reply::status_type::bad_request);
    }
    return d;
}

std::vector<simdjson::dom::parser> Utilities::parsers;

bool Utilities::validate_json(const std::unique_ptr<seastar::request> &req, std::unique_ptr<seastar::httpd::reply> &rep) {
    simdjson::dom::object object;
    simdjson::error_code error = parsers[seastar::this_shard_id()].parse(req->content).get(object);
    if (error) {
        rep->write_body("json", seastar::json::stream_object("Invalid JSON"));
        rep->set_status(seastar::httpd::reply::status_type::bad_request);
    }
    return !error;
}

ragedb::property_type_t Utilities::validate_json_property(const std::unique_ptr<seastar::request> &req, std::unique_ptr<seastar::httpd::reply> &rep) {
        simdjson::dom::element value;

        simdjson::error_code error = parsers[seastar::this_shard_id()].parse(req->content).get(value);
        if (error == 0U) {

            switch (value.type()) {
                case simdjson::dom::element_type::INT64: {
                    return int64_t(value);
                }
                case simdjson::dom::element_type::UINT64: {
                    // Unsigned Integer Values are not allowed, convert to signed
                    return static_cast<std::make_signed_t<uint64_t>>(value);
                }
                case simdjson::dom::element_type::DOUBLE: {
                    return double(value);
                }
                case simdjson::dom::element_type::STRING: {
                    return std::string(value);
                }
                case simdjson::dom::element_type::BOOL: {
                    return bool(value);
                }
                case simdjson::dom::element_type::NULL_VALUE: {
                    // Null Values are not allowed, just ignore them
                    rep->write_body("json", seastar::json::stream_object("Invalid JSON"));
                    rep->set_status(seastar::httpd::reply::status_type::bad_request);
                    return std::monostate();
                }
                case simdjson::dom::element_type::OBJECT: {
                    // TODO: Add support for nested properties
                    rep->write_body("json", seastar::json::stream_object("Invalid JSON"));
                    rep->set_status(seastar::httpd::reply::status_type::bad_request);
                    return std::monostate();
                }
                case simdjson::dom::element_type::ARRAY: {
                    auto array = simdjson::dom::array(value);
                    if (array.size() > 0) {
                        simdjson::dom::element first = array.at(0);
                        std::vector<int64_t> int_vector;
                        std::vector<double> double_vector;
                        std::vector<std::string> string_vector;
                        std::vector<bool> bool_vector;
                        switch (first.type()) {
                            case simdjson::dom::element_type::ARRAY: {
                                // TODO: Add support for nested properties
                                rep->write_body("json", seastar::json::stream_object("Invalid JSON"));
                                rep->set_status(seastar::httpd::reply::status_type::bad_request);
                                return std::monostate();
                            }
                            case simdjson::dom::element_type::OBJECT: {
                                // TODO: Add support for nested properties
                                rep->write_body("json", seastar::json::stream_object("Invalid JSON"));
                                rep->set_status(seastar::httpd::reply::status_type::bad_request);
                                return std::monostate();
                            }
                            case simdjson::dom::element_type::INT64: {
                                for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                    int_vector.emplace_back(int64_t(child));
                                }
                                return int_vector;
                            }
                            case simdjson::dom::element_type::UINT64: {
                                for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                    int_vector.emplace_back(static_cast<std::make_signed_t<uint64_t>>(child));
                                }
                                return int_vector;
                            }
                            case simdjson::dom::element_type::DOUBLE: {
                                for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                    double_vector.emplace_back(double(child));
                                }
                                return double_vector;
                            }
                            case simdjson::dom::element_type::STRING: {
                                for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                    string_vector.emplace_back(child);
                                }
                                return string_vector;
                            }
                            case simdjson::dom::element_type::BOOL: {
                                for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                    bool_vector.emplace_back(bool(child));
                                }
                                return bool_vector;
                            }
                            case simdjson::dom::element_type::NULL_VALUE: {
                                // Null Values are not allowed, just ignore them
                                rep->write_body("json", seastar::json::stream_object("Invalid JSON"));
                                rep->set_status(seastar::httpd::reply::status_type::bad_request);
                                return std::monostate();
                            }
                        }
                    }
                }
            }
        }
    return std::monostate();
}

bool Utilities::validate_allowed_data_type(std::unique_ptr<seastar::request> &req, std::unique_ptr<seastar::httpd::reply> &rep) {
  if ( req->param.exists(Utilities::DATA_TYPE) ) {
    bool allowed_type = allowed_types.find(req->param.at(Utilities::DATA_TYPE)) != allowed_types.end();
    if (!allowed_type) {
      rep->write_body("json", seastar::json::stream_object("Allowed data types: \"boolean\", \"integer\", \"double\", \"string\", \"boolean_list\", \"integer_list\", \"double_list\", \"string_list\""));
      rep->set_status(seastar::httpd::reply::status_type::bad_request);
    }
    return allowed_type;
  }
  return false;
}

Utilities::Utilities() {
    // We need to create one parser per core because otherwise we get parsing errors
    int cores = static_cast<int>(seastar::smp::count);
    for (int i = 0; i < cores; ++i) {
        parsers.emplace_back();
    }
}
