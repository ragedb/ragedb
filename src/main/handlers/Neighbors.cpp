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
#include "Neighbors.h"

void Neighbors::set_routes(seastar::httpd::routes &routes) {
    auto getNeighbors = new seastar::httpd::match_rule(&getNeighborsHandler);
    getNeighbors->add_str("/db/" + graph.GetName() + "/node");
    getNeighbors->add_param("type");
    getNeighbors->add_param("key");
    getNeighbors->add_str("/neighbors");
    getNeighbors->add_param("options", true);
    routes.add(getNeighbors, seastar::httpd::operation_type::GET);

    auto getNeighborsById = new seastar::httpd::match_rule(&getNeighborsByIdHandler);
    getNeighborsById->add_str("/db/" + graph.GetName() + "/node");
    getNeighborsById->add_param("id");
    getNeighborsById->add_str("/neighbors");
    getNeighborsById->add_param("options", true);
    routes.add(getNeighborsById, seastar::httpd::operation_type::GET);
}


future<std::unique_ptr<seastar::http::reply>> Neighbors::GetNeighborsHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

    if (valid_type && valid_key) {
        // Gather Options
        std::string options_string;
        Direction direction = Direction::BOTH;
        options_string = req->param.at(Utilities::OPTIONS).c_str();

        if(options_string.empty()) {
            // Get Node Neighbors
            return parent.graph.shard.local().NodeGetNeighborsPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY])
                    .then([rep = std::move(rep)] (const std::vector<Node>& nodes) mutable {
                        std::vector<node_json> json_array;
                        json_array.reserve(nodes.size());
                        for(Node n : nodes) {
                            json_array.emplace_back(n);
                        }
                        rep->write_body("json", seastar::json::stream_object(json_array));
                        return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                    });
        }

        std::vector<std::string> options;
        boost::split(options, options_string, [](char c){return c == '/';});

        // Erase empty first element from leading slash
        options.erase(options.begin());

        // Parse Direction
        boost::algorithm::to_lower(options[0]);
        if (options[0] == "in") {
            direction = Direction::IN;
        } else if (options[0] == "out") {
            direction = Direction::OUT;
        }

        switch(options.size()) {
            case 1:
                // Get Node Neighbors with Direction
                return parent.graph.shard.local().NodeGetNeighborsPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], direction)
                        .then([rep = std::move(rep)] (const std::vector<Node>& nodes) mutable {
                            std::vector<node_json> json_array;
                            json_array.reserve(nodes.size());
                            for(Node n : nodes) {
                                json_array.emplace_back(n);
                            }
                            rep->write_body("json", seastar::json::stream_object(json_array));
                            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                        });

            case 2: {
                // Get Node Neighbors with Direction and Type(s)
                std::vector<std::string> rel_types;
                boost::split(rel_types, options[1], boost::is_any_of("&,%26"), boost::token_compress_on);
                // Single Relationship Type
                if (rel_types.size() == 1) {
                    return parent.graph.shard.local().NodeGetNeighborsPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], direction, rel_types[0])
                            .then([rep = std::move(rep)] (const std::vector<Node>& nodes) mutable {
                                std::vector<node_json> json_array;
                                json_array.reserve(nodes.size());
                                for(Node n : nodes) {
                                    json_array.emplace_back(n);
                                }
                                rep->write_body("json", seastar::json::stream_object(json_array));
                                return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                            });
                }

                // Multiple Relationship Types
                return parent.graph.shard.local().NodeGetNeighborsPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], direction, rel_types)
                        .then([rep = std::move(rep)] (const std::vector<Node>& nodes) mutable {
                            std::vector<node_json> json_array;
                            json_array.reserve(nodes.size());
                            for(Node n : nodes) {
                                json_array.emplace_back(n);
                            }
                            rep->write_body("json", seastar::json::stream_object(json_array));
                            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                        });
            }

            default:  {
                rep->write_body("json", seastar::json::stream_object("Invalid request"));
                rep->set_status(seastar::http::reply::status_type::bad_request);
                return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
            }
        }
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> Neighbors::GetNeighborsByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    // Gather Options
    std::string options_string;
    Direction direction = Direction::BOTH;
    options_string = req->param.at(Utilities::OPTIONS).c_str();

    if(options_string.empty()) {
        // Get Node Neighbors
        return parent.graph.shard.local().NodeGetNeighborsPeered(id)
                .then([rep = std::move(rep)] (const std::vector<Node>& nodes) mutable {
                    std::vector<node_json> json_array;
                    json_array.reserve(nodes.size());
                    for(Node n : nodes) {
                        json_array.emplace_back(n);
                    }
                    rep->write_body("json", seastar::json::stream_object(json_array));
                    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                });
    }

    std::vector<std::string> options;
    boost::split(options, options_string, [](char c){return c == '/';});

    // Erase empty first element from leading slash
    options.erase(options.begin());

    // Parse Direction
    boost::algorithm::to_lower(options[0]);
    if (options[0] == "in") {
        direction = Direction::IN;
    } else if (options[0] == "out") {
        direction = Direction::OUT;
    }

    switch(options.size()) {
        case 1:
            // Get Node Neighbors with Direction
            return parent.graph.shard.local().NodeGetNeighborsPeered(id, direction)
                    .then([rep = std::move(rep)] (const std::vector<Node>& nodes) mutable {
                        std::vector<node_json> json_array;
                        json_array.reserve(nodes.size());
                        for(Node n : nodes) {
                            json_array.emplace_back(n);
                        }
                        rep->write_body("json", seastar::json::stream_object(json_array));
                        return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                    });
        case 2: {
            // Get Node Neighbors with Direction and Type(s)
            std::vector<std::string> rel_types;
            boost::split(rel_types, options[1], boost::is_any_of("&,%26"), boost::token_compress_on);
            // Single Relationship Type
            if (rel_types.size() == 1) {
                return parent.graph.shard.local().NodeGetNeighborsPeered(id, direction, rel_types[0])
                        .then([rep = std::move(rep)] (const std::vector<Node>& nodes) mutable {
                            std::vector<node_json> json_array;
                            json_array.reserve(nodes.size());
                            for(Node n : nodes) {
                                json_array.emplace_back(n);
                            }
                            rep->write_body("json", seastar::json::stream_object(json_array));
                            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                        });
            }

            // Multiple Relationship Types
            return parent.graph.shard.local().NodeGetNeighborsPeered(id, direction, rel_types)
                    .then([rep = std::move(rep)] (const std::vector<Node>& nodes) mutable {
                        std::vector<node_json> json_array;
                        json_array.reserve(nodes.size());
                        for(Node n : nodes) {
                            json_array.emplace_back(n);
                        }
                        rep->write_body("json", seastar::json::stream_object(json_array));
                        return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                    });
        }

        default:  {
            rep->write_body("json", seastar::json::stream_object("Invalid request"));
            rep->set_status(seastar::http::reply::status_type::bad_request);
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        }
    }
}
