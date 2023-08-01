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

#include "Nodes.h"
#include "Utilities.h"
#include "../json/JSON.h"

void Nodes::set_routes(seastar::httpd::routes &routes) {

    auto getNodes = new seastar::httpd::match_rule(&getNodesHandler);
    getNodes->add_str("/db/" + graph.GetName() + "/nodes");
    routes.add(getNodes, seastar::httpd::operation_type::GET);

    auto getNodesOfType = new seastar::httpd::match_rule(&getNodesOfTypeHandler);
    getNodesOfType->add_str("/db/" + graph.GetName() + "/nodes");
    getNodesOfType->add_param("type");
    routes.add(getNodesOfType, seastar::httpd::operation_type::GET);

    auto getNode = new seastar::httpd::match_rule(&getNodeHandler);
    getNode->add_str("/db/" + graph.GetName() + "/node");
    getNode->add_param("type");
    getNode->add_param("key");
    routes.add(getNode, seastar::httpd::operation_type::GET);

    auto getNodeById = new seastar::httpd::match_rule(&getNodeByIdHandler);
    getNodeById->add_str("/db/" + graph.GetName() + "/node");
    getNodeById->add_param("id");
    routes.add(getNodeById, seastar::httpd::operation_type::GET);

    auto postNode = new seastar::httpd::match_rule(&postNodeHandler);
    postNode->add_str("/db/" + graph.GetName() + "/node");
    postNode->add_param("type");
    postNode->add_param("key");
    routes.add(postNode, seastar::httpd::operation_type::POST);

    auto deleteNode = new seastar::httpd::match_rule(&deleteNodeHandler);
    deleteNode->add_str("/db/" + graph.GetName() + "/node");
    deleteNode->add_param("type");
    deleteNode->add_param("key");
    routes.add(deleteNode, seastar::httpd::operation_type::DELETE);

    auto deleteNodeById = new seastar::httpd::match_rule(&deleteNodeByIdHandler);
    deleteNodeById->add_str("/db/" + graph.GetName() + "/node");
    deleteNodeById->add_param("id");
    routes.add(deleteNodeById, seastar::httpd::operation_type::DELETE);

    auto findNodesOfType = new seastar::httpd::match_rule(&findNodesOfTypeHandler);
    findNodesOfType->add_str("/db/" + graph.GetName() + "/nodes");
    findNodesOfType->add_param("type");
    findNodesOfType->add_param("property");
    findNodesOfType->add_param("operation");
    routes.add(findNodesOfType, seastar::httpd::operation_type::POST);
}

future<std::unique_ptr<seastar::http::reply>> Nodes::GetNodesHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    uint64_t skip = Utilities::validate_skip(req, rep);
    uint64_t limit = Utilities::validate_limit(req, rep);

    return parent.graph.shard.local().AllNodesPeered(skip, limit)
            .then([rep = std::move(rep)](const std::vector<Node>& nodes) mutable {
                std::vector<node_json> json_array;
                json_array.reserve(nodes.size());
                for(Node node : nodes) {
                    json_array.emplace_back(node);
                }
                rep->write_body("json", seastar::json::stream_object(json_array));
                return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
            });
}

future<std::unique_ptr<seastar::http::reply>> Nodes::GetNodesOfTypeHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if(valid_type) {
        uint64_t skip = Utilities::validate_skip(req, rep);
        uint64_t limit = Utilities::validate_limit(req, rep);

        return parent.graph.shard.local().AllNodesPeered(req->param[Utilities::TYPE], skip, limit)
                .then([rep = std::move(rep)](const std::vector<Node>& nodes) mutable {
                    std::vector<node_json> json_array;
                    json_array.reserve(nodes.size());
                    if (!nodes.empty()) {
                        for(Node node : nodes) {
                            json_array.emplace_back(node);
                        }
                        rep->write_body("json", seastar::json::stream_object(json_array));
                        return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                    }
                    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                });
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> Nodes::GetNodeByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        uint16_t node_shard_id = Shard::CalculateShardId(id);
        return parent.graph.shard.invoke_on(node_shard_id, [id] (ragedb::Shard &local_shard) {
            // Find Node by Id
            return local_shard.NodeGet(id);
        }).then([rep = std::move(rep)] (Node node) mutable {
            if (node.getId() == 0) {
                rep->set_status(seastar::http::reply::status_type::not_found);
                return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
            }
            rep->write_body("json", seastar::json::stream_object(node_json(node)));
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> Nodes::GetNodeHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

    if(valid_type && valid_key) {
        return parent.graph.shard.local().NodeGetPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY])
                .then([rep = std::move(rep)](Node node) mutable {
                    if (node.getId() == 0) {
                        rep->set_status(seastar::http::reply::status_type::not_found);
                        return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                    }
                    rep->write_body("json", seastar::json::stream_object(node_json(node)));
                    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                });
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> Nodes::PostNodeHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

    if(valid_type && valid_key) {
        // If there are no properties
        if (req->content.empty()) {
            parent.graph.Log(req->_method, req->get_url());
            return parent.graph.shard.local().NodeAddEmptyPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY])
                    .then([rep = std::move(rep), type = req->param[Utilities::TYPE], key = req->param[Utilities::KEY]](uint64_t id) mutable {
                        if (id > 0) {
                            Node node(id, type, key);
                            rep->write_body("json", seastar::json::stream_object(node_json(node)));
                            rep->set_status(seastar::http::reply::status_type::created);
                        } else {
                            rep->write_body("json", seastar::json::stream_object("Invalid Request"));
                            rep->set_status(seastar::http::reply::status_type::bad_request);
                        }
                        return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                    });
        }
        if (Utilities::validate_json(req, rep)) {
            parent.graph.Log(req->_method, req->get_url(), req->content);
            return parent.graph.shard.local().NodeAddPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY],
                                                            req->content.c_str())
                    .then([rep = std::move(
                            rep), type = req->param[Utilities::TYPE], key = req->param[Utilities::KEY], this](
                            uint64_t id) mutable {
                        if (id > 0) {
                            return parent.graph.shard.local().NodeGetPeered(id).then(
                                    [rep = std::move(rep)](Node node) mutable {
                                        rep->write_body("json", seastar::json::stream_object((node_json(node))));
                                        rep->set_status(seastar::http::reply::status_type::created);
                                        return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                                    });
                        }

                        rep->write_body("json", seastar::json::stream_object("Invalid Request"));
                        rep->set_status(seastar::http::reply::status_type::bad_request);
                        return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                    });
        }
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> Nodes::DeleteNodeHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

    if(valid_type && valid_key) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().NodeRemovePeered(req->param[Utilities::TYPE], req->param[Utilities::KEY]).then([rep = std::move(rep)](bool success) mutable {
            if (success) {
                rep->set_status(seastar::http::reply::status_type::no_content);
            } else {
                rep->set_status(seastar::http::reply::status_type::not_modified);
            }
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> Nodes::DeleteNodeByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id >0) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().NodeRemovePeered(id).then([rep = std::move(rep)] (bool success) mutable {
            if(success) {
                rep->set_status(seastar::http::reply::status_type::no_content);
            } else {
                rep->set_status(seastar::http::reply::status_type::not_modified);
            }
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> Nodes::FindNodesOfTypeHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");
    ragedb::Operation operation = Utilities::validate_operation(req, rep);
    property_type_t value = Utilities::validate_json_property(req, rep);
    bool valid_combination = Utilities::validate_combination(operation, value);

    if(valid_type && valid_property && valid_combination) {
        uint64_t skip = Utilities::validate_skip(req, rep);
        uint64_t limit = Utilities::validate_limit(req, rep);

        return parent.graph.shard.local().FindNodesPeered(req->param[Utilities::TYPE], req->param[Utilities::PROPERTY], operation, value, skip, limit)
        .then([rep = std::move(rep)](const std::vector<Node>& nodes) mutable {
            std::vector<node_json> json_array;
            json_array.reserve(nodes.size());
            if (!nodes.empty()) {
                for(Node node : nodes) {
                    json_array.emplace_back(node);
                }
                rep->write_body("json", seastar::json::stream_object(json_array));
                return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
            }
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}