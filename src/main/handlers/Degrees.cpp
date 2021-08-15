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
#include "../json/JSON.h"
#include "Degrees.h"

void Degrees::set_routes(routes &routes) {
    auto getDegree = new match_rule(&getDegreeHandler);
    getDegree->add_str("/db/" + graph.GetName() + "/node");
    getDegree->add_param("type");
    getDegree->add_param("key");
    getDegree->add_str("/degree");
    getDegree->add_param("options", true);
    routes.add(getDegree, operation_type::GET);

    auto getDegreeById = new match_rule(&getDegreeByIdHandler);
    getDegreeById->add_str("/db/" + graph.GetName() + "/node");
    getDegreeById->add_param("id");
    getDegreeById->add_str("/degree");
    getDegreeById->add_param("options", true);
    routes.add(getDegreeById, operation_type::GET);
}

future<std::unique_ptr<reply>> Degrees::GetDegreeHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

    if (valid_type && valid_key) {
        // Gather Options
        std::string options_string;
        Direction direction = BOTH;
        options_string = req->param.at(Utilities::OPTIONS).c_str();

        if(options_string.empty()) {
            // Get Node Degree
            return parent.graph.shard.local().NodeGetDegreePeered(req->param[Utilities::TYPE], req->param[Utilities::KEY])
                    .then([rep = std::move(rep)] (uint64_t degree) mutable {
                        json_properties_builder json;
                        json.add("degree", degree);
                        rep->write_body("json", sstring(json.as_json()));
                        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                    });
        }

        std::vector<std::string> options;
        boost::split(options, options_string, [](char c){return c == '/';});

        // Erase empty first element from leading slash
        options.erase(options.begin());

        // Parse Direction
        boost::algorithm::to_lower(options[0]);
        if (options[0] == "in") {
            direction = IN;
        } else if (options[0] == "out") {
            direction = OUT;
        }

        switch(options.size()) {
            case 1:
                // Get Node Degree with Direction
                return parent.graph.shard.local().NodeGetDegreePeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], direction)
                        .then([rep = std::move(rep)] (uint64_t degree) mutable {
                            json_properties_builder json;
                            json.add("degree", degree);
                            rep->write_body("json", sstring(json.as_json()));
                            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                        });
            case 2: {
                // Get Node Degree with Direction and Type(s)
                std::vector<std::string> rel_types;
                boost::split(rel_types, options[1], boost::is_any_of("&,%26"), boost::token_compress_on);
                // Single Relationship Type
                if (rel_types.size() == 1) {
                    return parent.graph.shard.local().NodeGetDegreePeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], direction, rel_types[0])
                            .then([rep = std::move(rep)] (uint64_t degree) mutable {
                                json_properties_builder json;
                                json.add("degree", degree);
                                rep->write_body("json", sstring(json.as_json()));
                                return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                            });
                }

                // Multiple Relationship Types
                return parent.graph.shard.local().NodeGetDegreePeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], direction, rel_types)
                        .then([rep = std::move(rep)] (uint64_t degree) mutable {
                            json_properties_builder json;
                            json.add("degree", degree);
                            rep->write_body("json", sstring(json.as_json()));
                            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                        });
            }

            default:  {
                rep->write_body("json", json::stream_object("Invalid request"));
                rep->set_status(reply::status_type::bad_request);
                return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            }
        }
    }
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> Degrees::GetDegreeByIdHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    // Gather Options
    std::string options_string;
    Direction direction = BOTH;
    options_string = req->param.at(Utilities::OPTIONS).c_str();

    if(options_string.empty()) {
        // Get Node Degree
        return parent.graph.shard.local().NodeGetDegreePeered(id)
                .then([rep = std::move(rep)] (uint64_t degree) mutable {
                    json_properties_builder json;
                    json.add("degree", degree);
                    rep->write_body("json", sstring(json.as_json()));
                    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                });
    }

    std::vector<std::string> options;
    boost::split(options, options_string, [](char c){return c == '/';});

    // Erase empty first element from leading slash
    options.erase(options.begin());

    // Parse Direction
    boost::algorithm::to_lower(options[0]);
    if (options[0] == "in") {
        direction = IN;
    } else if (options[0] == "out") {
        direction = OUT;
    }

    switch(options.size()) {
        case 1:
            // Get Node Degree with Direction
            return parent.graph.shard.local().NodeGetDegreePeered(id, direction)
                    .then([rep = std::move(rep)] (uint64_t degree) mutable {
                        json_properties_builder json;
                        json.add("degree", degree);
                        rep->write_body("json", sstring(json.as_json()));
                        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                    });
        case 2: {
            // Get Node Degree with Direction and Type(s)
            std::vector<std::string> rel_types;
            boost::split(rel_types, options[1], boost::is_any_of("&,%26"), boost::token_compress_on);
            // Single Relationship Type
            if (rel_types.size() == 1) {
                return parent.graph.shard.local().NodeGetDegreePeered(id, direction, rel_types[0])
                        .then([rep = std::move(rep)] (uint64_t degree) mutable {
                            json_properties_builder json;
                            json.add("degree", degree);
                            rep->write_body("json", sstring(json.as_json()));
                            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                        });
            }

            // Multiple Relationship Types
            return parent.graph.shard.local().NodeGetDegreePeered(id, direction, rel_types).then([rep = std::move(rep)] (uint64_t degree) mutable {
                json_properties_builder json;
                json.add("degree", degree);
                rep->write_body("json", sstring(json.as_json()));
                return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            });
        }

        default:  {
            rep->write_body("json", json::stream_object("Invalid request"));
            rep->set_status(reply::status_type::bad_request);
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }
    }
}