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

void Nodes::set_routes(routes &routes) {

    auto getNodes = new match_rule(&getNodesHandler);
    getNodes->add_str("/db/" + graph.GetName() + "/nodes");
    routes.add(getNodes, operation_type::GET);

    auto getNodesOfType = new match_rule(&getNodesOfTypeHandler);
    getNodesOfType->add_str("/db/" + graph.GetName() + "/nodes");
    getNodesOfType->add_param("type");
    routes.add(getNodesOfType, operation_type::GET);

    auto getNode = new match_rule(&getNodeHandler);
    getNode->add_str("/db/" + graph.GetName() + "/node");
    getNode->add_param("type");
    getNode->add_param("key");
    routes.add(getNode, operation_type::GET);

    auto getNodeById = new match_rule(&getNodeByIdHandler);
    getNodeById->add_str("/db/" + graph.GetName() + "/node");
    getNodeById->add_param("id");
    routes.add(getNodeById, operation_type::GET);

    auto postNode = new match_rule(&postNodeHandler);
    postNode->add_str("/db/" + graph.GetName() + "/node");
    postNode->add_param("type");
    postNode->add_param("key");
    routes.add(postNode, operation_type::POST);

    auto deleteNode = new match_rule(&deleteNodeHandler);
    deleteNode->add_str("/db/" + graph.GetName() + "/node");
    deleteNode->add_param("type");
    deleteNode->add_param("key");
    routes.add(deleteNode, operation_type::DELETE);

    auto deleteNodeById = new match_rule(&deleteNodeByIdHandler);
    deleteNodeById->add_str("/db/" + graph.GetName() + "/node");
    deleteNodeById->add_param("id");
    routes.add(deleteNodeById, operation_type::DELETE);
}

future<std::unique_ptr<reply>> Nodes::GetNodesHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    uint64_t limit = Utilities::validate_limit(req, rep);
    uint64_t offset = Utilities::validate_offset(req, rep);

    return parent.graph.shard.local().AllNodesPeered(offset, limit)
            .then([rep = std::move(rep)](const std::vector<Node>& nodes) mutable {
                std::vector<node_json> json_array;
                json_array.reserve(nodes.size());
                std::copy(nodes.begin(), nodes.end(), json_array.begin());
                rep->write_body("json", json::stream_object(json_array));
                return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            });
}

future<std::unique_ptr<reply>> Nodes::GetNodesOfTypeHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if(valid_type) {
        uint64_t limit = Utilities::validate_limit(req, rep);
        uint64_t offset = Utilities::validate_offset(req, rep);

        return parent.graph.shard.local().AllNodesPeered(req->param[Utilities::TYPE], offset, limit)
                .then([rep = std::move(rep)](std::vector<Node> nodes) mutable {
                    std::vector<node_json> json_array;
                    json_array.reserve(nodes.size());
                    if (!nodes.empty()) {
                        std::copy(nodes.begin(), nodes.end(), json_array.begin());
                        rep->write_body("json", json::stream_object(json_array));
                        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                    }
                    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                });
    }
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> Nodes::GetNodeByIdHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        uint16_t node_shard_id = Shard::CalculateShardId(id);
        return parent.graph.shard.invoke_on(node_shard_id, [id] (Shard &local_shard) {
            // Find Node by Id
            return local_shard.NodeGet(id);
        }).then([rep = std::move(rep)] (Node node) mutable {
            if (node.getId() == 0) {
                rep->set_status(reply::status_type::not_found);
                return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            }
            rep->write_body("json", json::stream_object((node_json(node))));
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        });
    }

    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> Nodes::GetNodeHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

    if(valid_type && valid_key) {
        return parent.graph.shard.local().NodeGetPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY])
                .then([rep = std::move(rep)](Node node) mutable {
                    if (node.getId() == 0) {
                        rep->set_status(reply::status_type::not_found);
                        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                    }
                    rep->write_body("json", json::stream_object((node_json(node))));
                    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                });
    }
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> Nodes::PostNodeHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

    if(valid_type && valid_key) {
        // If there are no properties
        if (req->content.empty()) {
            return parent.graph.shard.local().NodeAddEmptyPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY])
                    .then([rep = std::move(rep), type = req->param[Utilities::TYPE], key = req->param[Utilities::KEY]](uint64_t id) mutable {
                        if (id > 0) {
                            Node node(id, type, key);
                            rep->write_body("json", json::stream_object((node_json(node))));
                            rep->set_status(reply::status_type::created);
                        } else {
                            rep->write_body("json", json::stream_object("Invalid Request"));
                            rep->set_status(reply::status_type::bad_request);
                        }
                        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                    });
        }
        if (Utilities::validate_json(req, rep)) {
            return parent.graph.shard.local().NodeAddPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY],
                                                            req->content.c_str())
                    .then([rep = std::move(
                            rep), type = req->param[Utilities::TYPE], key = req->param[Utilities::KEY], this](
                            uint64_t id) mutable {
                        if (id > 0) {
                            return parent.graph.shard.local().NodeGetPeered(id).then(
                                    [rep = std::move(rep)](Node node) mutable {
                                        rep->write_body("json", json::stream_object((node_json(node))));
                                        rep->set_status(reply::status_type::created);
                                        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                                    });
                        }

                        rep->write_body("json", json::stream_object("Invalid Request"));
                        rep->set_status(reply::status_type::bad_request);
                        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                    });
        }
    }

    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> Nodes::DeleteNodeHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

    if(valid_type && valid_key) {
        return parent.graph.shard.local().NodeRemovePeered(req->param[Utilities::TYPE], req->param[Utilities::KEY]).then([rep = std::move(rep)](bool success) mutable {
            if (success) {
                rep->set_status(reply::status_type::no_content);
            } else {
                rep->set_status(reply::status_type::not_modified);
            }
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        });
    }
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> Nodes::DeleteNodeByIdHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id >0) {
        return parent.graph.shard.local().NodeRemovePeered(id).then([rep = std::move(rep)] (bool success) mutable {
            if(success) {
                rep->set_status(reply::status_type::no_content);
            } else {
                rep->set_status(reply::status_type::not_modified);
            }
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        });
    }

    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}