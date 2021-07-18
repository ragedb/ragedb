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

#include "Schema.h"

#include <utility>
#include "Utilities.h"
#include "../json/JSON.h"
#include <seastar/json/formatter.hh>

void Schema::set_routes(routes &routes) {

    auto getNodeTypes = new match_rule(&getNodeTypesHandler);
    getNodeTypes->add_str("/db/" + graph.GetName() + "/schema/nodes");
    routes.add(getNodeTypes, operation_type::GET);

    auto getRelationshipTypes = new match_rule(&getRelationshipTypesHandler);
    getRelationshipTypes->add_str("/db/" + graph.GetName() + "/schema/relationships");
    routes.add(getRelationshipTypes, operation_type::GET);

    auto getNodeType = new match_rule(&getNodeTypeHandler);
    getNodeType->add_str("/db/" + graph.GetName() + "/schema/nodes/{type}");
    routes.add(getNodeType, operation_type::GET);

    auto postNodeType = new match_rule(&postNodeTypeHandler);
    postNodeType->add_str("/db/" + graph.GetName() + "/schema/nodes/{type}");
    routes.add(postNodeType, operation_type::POST);

    auto deleteNodeType = new match_rule(&deleteNodeTypeHandler);
    deleteNodeType->add_str("/db/" + graph.GetName() + "/schema/nodes/{type}");
    routes.add(deleteNodeType, operation_type::DELETE);

    auto getRelationshipType = new match_rule(&getRelationshipTypeHandler);
    getRelationshipType->add_str("/db/" + graph.GetName() + "/schema/relationships/{type}");
    routes.add(getRelationshipType, operation_type::GET);

    auto postRelationshipType = new match_rule(&postRelationshipTypeHandler);
    postRelationshipType->add_str("/db/" + graph.GetName() + "/schema/relationships/{type}");
    routes.add(postRelationshipType, operation_type::POST);

    auto deleteRelationshipType = new match_rule(&deleteRelationshipTypeHandler);
    deleteRelationshipType->add_str("/db/" + graph.GetName() + "/schema/relationships/{type}");
    routes.add(deleteRelationshipType, operation_type::DELETE);

    auto getNodeTypeProperty = new match_rule(&getNodeTypePropertyHandler);
    getNodeTypeProperty->add_str("/db/" + graph.GetName() + "/schema/nodes/{type}/properties/{property}");
    routes.add(getNodeTypeProperty, operation_type::GET);

    auto postNodeTypeProperty = new match_rule(&postNodeTypePropertyHandler);
    postNodeTypeProperty->add_str("/db/" + graph.GetName() + "/schema/nodes/{type}/{property}/{data_type}");
    routes.add(postNodeTypeProperty, operation_type::POST);

    auto deleteNodeTypeProperty = new match_rule(&deleteNodeTypePropertyHandler);
    deleteNodeTypeProperty->add_str("/db/" + graph.GetName() + "/schema/nodes/{type}/properties/{property}");
    routes.add(deleteNodeTypeProperty, operation_type::DELETE);

    auto getRelationshipTypeProperty = new match_rule(&getRelationshipTypePropertyHandler);
    getRelationshipTypeProperty->add_str("/db/" + graph.GetName() + "/schema/relationships/{type}/properties/{property}");
    routes.add(getRelationshipTypeProperty, operation_type::GET);

    auto postRelationshipTypeProperty = new match_rule(&postRelationshipTypePropertyHandler);
    postRelationshipTypeProperty->add_str("/db/" + graph.GetName() + "/schema/relationships/{type}/{property}/{data_type}");
    routes.add(postRelationshipTypeProperty, operation_type::POST);

    auto deleteRelationshipTypeProperty = new match_rule(&deleteRelationshipTypePropertyHandler);
    deleteRelationshipTypeProperty->add_str("/db/" + graph.GetName() + "/schema/relationships/{type}/properties/{property}");
    routes.add(deleteRelationshipTypeProperty, operation_type::DELETE);

}

future<std::unique_ptr<reply>>
Schema::GetNodeTypesHandler::handle([[maybe_unused]] const sstring &path, [[maybe_unused]] std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    std::set<std::string> types = parent.graph.shard.local().NodeTypesGet();
    rep->write_body("json", json::stream_object(std::vector<std::string>( types.begin(), types.end() )));
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>>
Schema::GetRelationshipTypesHandler::handle([[maybe_unused]] const sstring &path, [[maybe_unused]] std::unique_ptr<request> req,
                                            std::unique_ptr<reply> rep) {
    std::set<std::string> types = parent.graph.shard.local().RelationshipTypesGet();
    rep->write_body("json", json::stream_object(std::vector<std::string>( types.begin(), types.end() )));
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>>
Schema::GetNodeTypeHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if(valid_type) {
        std::map<std::string, std::string> type = parent.graph.shard.local().NodeTypeGet(req->param[Utilities::TYPE]);
        rep->write_body("json", json::stream_object(schema_json(type)));
    }

    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>>
Schema::PostNodeTypeHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if(valid_type) {

    }
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>>
Schema::DeleteNodeTypeHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if(valid_type) {
        return parent.graph.shard.local().DeleteNodeTypePeered(req->param[Utilities::TYPE]).then([rep = std::move(rep)](bool success) mutable {
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

future<std::unique_ptr<reply>>
Schema::GetRelationshipTypeHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req,
                                           std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if(valid_type) {
        std::map<std::string, std::string> type = parent.graph.shard.local().RelationshipTypeGet(req->param[Utilities::TYPE]);
        rep->write_body("json", json::stream_object(schema_json(type)));
    }
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>>
Schema::PostRelationshipTypeHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req,
                                            std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if(valid_type) {

    }
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>>
Schema::DeleteRelationshipTypeHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req,
                                              std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if(valid_type) {
        return parent.graph.shard.local().DeleteRelationshipTypePeered(req->param[Utilities::TYPE]).then([rep = std::move(rep)](bool success) mutable {
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

future<std::unique_ptr<reply>>
Schema::GetNodeTypePropertyHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req,
                                           std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if(valid_type && valid_property) {
        std::string data_type = parent.graph.shard.local().NodePropertyTypeGet(req->param[Utilities::TYPE], req->param[Utilities::PROPERTY]);
        rep->write_body("json", json::stream_object(data_type));
    }
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>>
Schema::PostNodeTypePropertyHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req,
                                            std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");
    bool valid_data_type = Utilities::validate_parameter(Utilities::DATA_TYPE, req, rep, "Invalid data type");

    if(valid_type && valid_property && valid_data_type) {

    }
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>>
Schema::DeleteNodeTypePropertyHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req,
                                              std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if(valid_type && valid_property) {
        return parent.graph.shard.local().NodePropertyTypeDeletePeered(req->param[Utilities::TYPE],
                                                                       req->param[Utilities::PROPERTY]).then([rep = std::move(rep)](bool success) mutable {
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

future<std::unique_ptr<reply>>
Schema::GetRelationshipTypePropertyHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req,
                                                   std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if(valid_type && valid_property) {
        std::string data_type = parent.graph.shard.local().RelationshipPropertyTypeGet(req->param[Utilities::TYPE], req->param[Utilities::PROPERTY]);
        rep->write_body("json", json::stream_object(data_type));
    }
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>>
Schema::PostRelationshipTypePropertyHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req,
                                                    std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");
    bool valid_data_type = Utilities::validate_parameter(Utilities::DATA_TYPE, req, rep, "Invalid data type");

    if(valid_type && valid_property && valid_data_type) {

    }
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>>
Schema::DeleteRelationshipTypePropertyHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req,
                                                      std::unique_ptr<reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if(valid_type && valid_property) {
        return parent.graph.shard.local().RelationshipPropertyTypeDeletePeered(req->param[Utilities::TYPE], req->param[Utilities::PROPERTY]).then([rep = std::move(rep)](bool success) mutable {
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
