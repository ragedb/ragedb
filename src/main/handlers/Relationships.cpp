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


#include "Relationships.h"
#include "Utilities.h"
#include "../json/JSON.h"

void Relationships::set_routes(seastar::routes &routes) {

    auto getRelationships = new seastar::match_rule(&getRelationshipsHandler);
    getRelationships->add_str("/db/" + graph.GetName() + "/relationships");
    routes.add(getRelationships, seastar::operation_type::GET);

    auto getRelationshipsOfType = new seastar::match_rule(&getRelationshipsOfTypeHandler);
    getRelationshipsOfType->add_str("/db/" + graph.GetName() + "/relationships");
    getRelationshipsOfType->add_param("type");
    routes.add(getRelationshipsOfType, seastar::operation_type::GET);

    auto getRelationship = new seastar::match_rule(&getRelationshipHandler);
    getRelationship->add_str("/db/" + graph.GetName() + "/relationship");
    getRelationship->add_param("id");
    routes.add(getRelationship, seastar::operation_type::GET);

    auto postRelationshipById = new seastar::match_rule(&postRelationshipByIdHandler);
    postRelationshipById->add_str("/db/" + graph.GetName() + "/node");
    postRelationshipById->add_param("id");
    postRelationshipById->add_str("/relationship");
    postRelationshipById->add_param("id2");
    postRelationshipById->add_param("rel_type");
    routes.add(postRelationshipById, seastar::operation_type::POST);

    auto postRelationship = new seastar::match_rule(&postRelationshipHandler);
    postRelationship->add_str("/db/" + graph.GetName() + "/node");
    postRelationship->add_param("type");
    postRelationship->add_param("key");
    postRelationship->add_str("/relationship");
    postRelationship->add_param("type2");
    postRelationship->add_param("key2");
    postRelationship->add_param("rel_type");
    routes.add(postRelationship, seastar::operation_type::POST);

    auto deleteRelationship = new seastar::match_rule(&deleteRelationshipHandler);
    deleteRelationship->add_str("/db/" + graph.GetName() + "/relationship");
    deleteRelationship->add_param("id");
    routes.add(deleteRelationship, seastar::operation_type::DELETE);

    auto getRelationshipsById = new seastar::match_rule(&getNodeRelationshipsByIdHandler);
    getRelationshipsById->add_str("/db/" + graph.GetName() + "/node");
    getRelationshipsById->add_param("id");
    getRelationshipsById->add_str("/relationships");
    getRelationshipsById->add_param("options", true);
    routes.add(getRelationshipsById, seastar::operation_type::GET);

    auto getNodeRelationships = new seastar::match_rule(&getNodeRelationshipsHandler);
    getNodeRelationships->add_str("/db/" + graph.GetName() + "/node");
    getNodeRelationships->add_param("type");
    getNodeRelationships->add_param("key");
    getNodeRelationships->add_str("/relationships");
    getNodeRelationships->add_param("options", true);
    routes.add(getNodeRelationships, seastar::operation_type::GET);

    auto findRelationshipsOfType = new seastar::match_rule(&findRelationshipsOfTypeHandler);
    findRelationshipsOfType->add_str("/db/" + graph.GetName() + "/relationships");
    findRelationshipsOfType->add_param("type");
    findRelationshipsOfType->add_param("property");
    findRelationshipsOfType->add_param("operation");
    routes.add(findRelationshipsOfType, seastar::operation_type::POST);
}

future<std::unique_ptr<seastar::httpd::reply>> Relationships::GetRelationshipsHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    uint64_t limit = Utilities::validate_limit(req, rep);
    uint64_t skip = Utilities::validate_skip(req, rep);

    return parent.graph.shard.local().AllRelationshipsPeered(skip, limit)
            .then([rep = std::move(rep)] (const std::vector<Relationship>& relationships) mutable {
                std::vector<relationship_json> json_array;
                json_array.reserve(relationships.size());
                for(Relationship r : relationships) {
                    json_array.emplace_back(r);
                }
                rep->write_body("json", seastar::json::stream_object(json_array));
                return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
            });
}

future<std::unique_ptr<seastar::httpd::reply>> Relationships::GetRelationshipsOfTypeHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if(valid_type) {
        uint64_t limit = Utilities::validate_limit(req, rep);
        uint64_t skip = Utilities::validate_skip(req, rep);

        return parent.graph.shard.local().AllRelationshipsPeered(req->param[Utilities::TYPE], skip, limit)
                .then([rep = std::move(rep)](const std::vector<Relationship>& relationships) mutable {
                    std::vector<relationship_json> json_array;
                    json_array.reserve(relationships.size());
                    if (!relationships.empty()) {
                        for(Relationship r : relationships) {
                            json_array.emplace_back(r);
                        }
                        rep->write_body("json", seastar::json::stream_object(json_array));
                        return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                    }
                    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                });
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::httpd::reply>> Relationships::GetRelationshipHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        return parent.graph.shard.local().RelationshipGetPeered(id)
                .then([rep = std::move(rep)] (Relationship relationship) mutable {
                    if (relationship.getId() == 0) {
                        rep->set_status(seastar::httpd::reply::status_type::not_found);
                        return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                    }
                    rep->write_body("json", seastar::json::stream_object((relationship_json(relationship))));
                    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                });
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::httpd::reply>> Relationships::PostRelationshipHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");
    bool valid_type2 = Utilities::validate_parameter(Utilities::TYPE2, req, rep, "Invalid type2");
    bool valid_key2 = Utilities::validate_parameter(Utilities::KEY2, req, rep, "Invalid key2");
    bool valid_rel_type = Utilities::validate_parameter(Utilities::REL_TYPE, req, rep, "Invalid relationship type");

    if(valid_type && valid_key && valid_type2 && valid_key2 && valid_rel_type) {
        // If there are no properties
        if (req->content.empty()) {
            parent.graph.Log(req->_method, req->get_url());
            return parent.graph.shard.local().RelationshipAddEmptyPeered(req->param[Utilities::REL_TYPE], req->param[Utilities::TYPE],req->param[Utilities::KEY], req->param[Utilities::TYPE2], req->param[Utilities::KEY2])
                    .then([rep = std::move(rep), rel_type=req->param[Utilities::REL_TYPE], this] (uint64_t id) mutable {
                        if (id > 0) {
                            return parent.graph.shard.local().RelationshipGetPeered(id).then([rep = std::move(rep), rel_type] (Relationship relationship) mutable {
                                rep->write_body("json", seastar::json::stream_object((relationship_json(relationship))));
                                rep->set_status(seastar::httpd::reply::status_type::created);
                                return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                            });
                        } else {
                            rep->write_body("json", seastar::json::stream_object("Invalid Request"));
                            rep->set_status(seastar::httpd::reply::status_type::bad_request);
                        }
                        return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                    });
        } else {
            if (Utilities::validate_json(req, rep)) {
                parent.graph.Log(req->_method, req->get_url(), req->content);
                return parent.graph.shard.local().RelationshipAddPeered(req->param[Utilities::REL_TYPE],
                                                                        req->param[Utilities::TYPE],
                                                                        req->param[Utilities::KEY],
                                                                        req->param[Utilities::TYPE2],
                                                                        req->param[Utilities::KEY2],
                                                                        req->content.c_str())
                        .then([rep = std::move(rep), rel_type = req->param[Utilities::REL_TYPE], this](
                                uint64_t id) mutable {
                            if (id > 0) {
                                return parent.graph.shard.local().RelationshipGetPeered(id).then(
                                        [rep = std::move(rep)](Relationship relationship) mutable {
                                            rep->write_body("json",
                                                            json::stream_object((relationship_json(relationship))));
                                            rep->set_status(seastar::httpd::reply::status_type::created);
                                            return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                                        });
                            } else {
                                rep->write_body("json", seastar::json::stream_object("Invalid Request"));
                                rep->set_status(seastar::httpd::reply::status_type::bad_request);
                            }
                            return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                        });
            }
        }
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::httpd::reply>> Relationships::PostRelationshipByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);
    uint64_t id2 = Utilities::validate_id2(req, rep);
    bool valid_rel_type = Utilities::validate_parameter(Utilities::REL_TYPE, req, rep, "Invalid relationship type");

    if(id > 0 && id2 > 0 && valid_rel_type) {
        // If there are no properties
        if (req->content.empty()) {
            parent.graph.Log(req->_method, req->get_url(), req->content);
            return parent.graph.shard.local().RelationshipAddEmptyPeered(req->param[Utilities::REL_TYPE], id, id2)
                    .then([rep = std::move(rep), rel_type=req->param[Utilities::REL_TYPE], this] (uint64_t relationship_id) mutable {
                        if (relationship_id > 0) {
                            return parent.graph.shard.local().RelationshipGetPeered(relationship_id).then([rep = std::move(rep)] (Relationship relationship) mutable {
                                rep->write_body("json", seastar::json::stream_object((relationship_json(relationship))));
                                rep->set_status(seastar::httpd::reply::status_type::created);
                                return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                            });
                        } else {
                            rep->write_body("json", seastar::json::stream_object("Invalid Request"));
                            rep->set_status(seastar::httpd::reply::status_type::bad_request);
                        }
                        return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                    });
        } else {
            if (Utilities::validate_json(req, rep)) {
                parent.graph.Log(req->_method, req->get_url(), req->content);
                return parent.graph.shard.local().RelationshipAddPeered(req->param[Utilities::REL_TYPE], id, id2,
                                                                        req->content.c_str())
                        .then([rep = std::move(rep), rel_type = req->param[Utilities::REL_TYPE], this](
                                uint64_t relationship_id) mutable {
                            if (relationship_id > 0) {
                                return parent.graph.shard.local().RelationshipGetPeered(relationship_id).then(
                                        [rep = std::move(rep)](Relationship relationship) mutable {
                                            rep->write_body("json",
                                                            json::stream_object((relationship_json(relationship))));
                                            rep->set_status(seastar::httpd::reply::status_type::created);
                                            return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                                        });

                            } else {
                                rep->write_body("json", seastar::json::stream_object("Invalid Request"));
                                rep->set_status(seastar::httpd::reply::status_type::bad_request);
                            }
                            return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                        });
            }
        }
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::httpd::reply>> Relationships::DeleteRelationshipHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().RelationshipRemovePeered(id).then([rep = std::move(rep)] (bool success) mutable {
            if(success) {
                rep->set_status(seastar::httpd::reply::status_type::no_content);
            } else {
                rep->set_status(seastar::httpd::reply::status_type::not_modified);
            }
            return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
        });
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::httpd::reply>> Relationships::GetNodeRelationshipsHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

    if(valid_type && valid_key) {

        // Gather Options
        std::string options_string;
        Direction direction = Direction::BOTH;
        options_string = req->param.at(Utilities::OPTIONS).c_str();

        if(options_string.empty()) {
            // Get Node Relationships
            return parent.graph.shard.local().NodeGetRelationshipsPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY])
                    .then([rep = std::move(rep)] (const std::vector<Relationship>& relationships) mutable {
                        std::vector<relationship_json> json_array;
                        json_array.reserve(relationships.size());
                        for(Relationship r : relationships) {
                            json_array.emplace_back(r);
                        }
                        rep->write_body("json", seastar::json::stream_object(json_array));
                        return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
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
                // Get Node Degree with Direction
                return parent.graph.shard.local().NodeGetRelationshipsPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], direction)
                        .then([rep = std::move(rep)] (const std::vector<Relationship>& relationships) mutable {
                            std::vector<relationship_json> json_array;
                            json_array.reserve(relationships.size());
                            for(Relationship r : relationships) {
                                json_array.emplace_back(r);
                            }
                            rep->write_body("json", seastar::json::stream_object(json_array));
                            return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                        });
            case 2: {
                // Get Node Relationships with Direction and Type(s)
                std::vector<std::string> rel_types;
                // Deal with both escaped and unescaped "&"
                boost::split(rel_types, options[1], boost::is_any_of("&,%26"), boost::token_compress_on);
                // Single Relationship Type
                if (rel_types.size() == 1) {
                    return parent.graph.shard.local().NodeGetRelationshipsPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], direction, rel_types[0])
                            .then([rep = std::move(rep), rel_type = rel_types[0]] (const std::vector<Relationship>& relationships) mutable {
                                std::vector<relationship_json> json_array;
                                json_array.reserve(relationships.size());
                                for(Relationship r : relationships) {
                                    json_array.emplace_back(r);
                                }
                                rep->write_body("json", seastar::json::stream_object(json_array));
                                return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                            });
                }

                // Multiple Relationship Types
                return parent.graph.shard.local().NodeGetRelationshipsPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], direction, rel_types)
                        .then([rep = std::move(rep)] (const std::vector<Relationship>& relationships) mutable {
                            std::vector<relationship_json> json_array;
                            json_array.reserve(relationships.size());
                            for(Relationship r : relationships) {
                                json_array.emplace_back(r);
                            }
                            rep->write_body("json", seastar::json::stream_object(json_array));
                            return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                        });
            }

            default:  {
                rep->write_body("json", seastar::json::stream_object("Invalid request"));
                rep->set_status(seastar::httpd::reply::status_type::bad_request);
                return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
            }
        }
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::httpd::reply>> Relationships::GetNodeRelationshipsByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        // Gather Options
        std::string options_string;
        Direction direction = Direction::BOTH;
        options_string = req->param.at(Utilities::OPTIONS).c_str();

        if(options_string.empty()) {
            // Get Node Relationships
            return parent.graph.shard.local().NodeGetRelationshipsPeered(id)
                    .then([rep = std::move(rep)] (const std::vector<Relationship>& relationships) mutable {
                        std::vector<relationship_json> json_array;
                        json_array.reserve(relationships.size());
                        for(Relationship r : relationships) {
                            json_array.emplace_back(r);
                        }
                        rep->write_body("json", seastar::json::stream_object(json_array));
                        return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
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
                // Get Node Relationships with Direction
                return parent.graph.shard.local().NodeGetRelationshipsPeered(id, direction)
                        .then([rep = std::move(rep)] (const std::vector<Relationship>& relationships) mutable {
                            std::vector<relationship_json> json_array;
                            json_array.reserve(relationships.size());
                            for(Relationship r : relationships) {
                                json_array.emplace_back(r);
                            }
                            rep->write_body("json", seastar::json::stream_object(json_array));
                            return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                        });
            case 2: {
                // Get Node Degree with Direction and Type(s)
                std::vector<std::string> rel_types;
                boost::split(rel_types, options[1], boost::is_any_of("&,%26"), boost::token_compress_on);
                // Single Relationship Type
                if (rel_types.size() == 1) {
                    return parent.graph.shard.local().NodeGetRelationshipsPeered(id, direction, rel_types[0])
                            .then([rep = std::move(rep), rel_type = rel_types[0]] (const std::vector<Relationship>& relationships) mutable {
                                std::vector<relationship_json> json_array;
                                json_array.reserve(relationships.size());
                                for(Relationship r : relationships) {
                                    json_array.emplace_back(r);
                                }
                                rep->write_body("json", seastar::json::stream_object(json_array));
                                return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                            });
                }

                // Multiple Relationship Types
                return parent.graph.shard.local().NodeGetRelationshipsPeered(id, direction, rel_types)
                        .then([rep = std::move(rep)] (const std::vector<Relationship>& relationships) mutable {
                            std::vector<relationship_json> json_array;
                            json_array.reserve(relationships.size());
                            for(Relationship r : relationships) {
                                json_array.emplace_back(r);
                            }
                            rep->write_body("json", seastar::json::stream_object(json_array));
                            return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                        });
            }

            default:  {
                rep->write_body("json", seastar::json::stream_object("Invalid request"));
                rep->set_status(seastar::httpd::reply::status_type::bad_request);
                return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
            }
        }
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::httpd::reply>> Relationships::FindRelationshipsOfTypeHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");
    ragedb::Operation operation = Utilities::validate_operation(req, rep);
    property_type_t value = Utilities::validate_json_property(req, rep);
    bool valid_combination = Utilities::validate_combination(operation, value);

    if(valid_type && valid_property && valid_combination) {
        uint64_t skip = Utilities::validate_skip(req, rep);
        uint64_t limit = Utilities::validate_limit(req, rep);

        return parent.graph.shard.local().FindRelationshipsPeered(req->param[Utilities::TYPE], req->param[Utilities::PROPERTY], operation, value, skip, limit)
                .then([rep = std::move(rep)](const std::vector<Relationship>& relationships) mutable {
                    std::vector<relationship_json> json_array;
                    json_array.reserve(relationships.size());
                    if (!relationships.empty()) {
                        for(Relationship relationship : relationships) {
                            json_array.emplace_back(relationship);
                        }
                        rep->write_body("json", seastar::json::stream_object(json_array));
                        return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                    }
                    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                });
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}