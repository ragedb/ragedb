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

void Schema::set_routes(seastar::httpd::routes &routes) {
    auto clearGraph = new seastar::httpd::match_rule(&clearGraphHandler);
    clearGraph->add_str("/db/" + graph.GetName() + "/schema");
    routes.add(clearGraph, seastar::httpd::operation_type::DELETE);

    auto getNodeTypes = new seastar::httpd::match_rule(&getNodeTypesHandler);
    getNodeTypes->add_str("/db/" + graph.GetName() + "/schema/nodes");
    routes.add(getNodeTypes, seastar::httpd::operation_type::GET);

    auto getRelationshipTypes = new seastar::httpd::match_rule(&getRelationshipTypesHandler);
    getRelationshipTypes->add_str("/db/" + graph.GetName() + "/schema/relationships");
    routes.add(getRelationshipTypes, seastar::httpd::operation_type::GET);

    auto getNodeType = new seastar::httpd::match_rule(&getNodeTypeHandler);
    getNodeType->add_str("/db/" + graph.GetName() + "/schema/nodes");
    getNodeType->add_param("type");
    routes.add(getNodeType, seastar::httpd::operation_type::GET);

    auto postNodeType = new seastar::httpd::match_rule(&postNodeTypeHandler);
    postNodeType->add_str("/db/" + graph.GetName() + "/schema/nodes");
    postNodeType->add_param("type");
    routes.add(postNodeType, seastar::httpd::operation_type::POST);

    auto deleteNodeType = new seastar::httpd::match_rule(&deleteNodeTypeHandler);
    deleteNodeType->add_str("/db/" + graph.GetName() + "/schema/nodes");
    deleteNodeType->add_param("type");
    routes.add(deleteNodeType, seastar::httpd::operation_type::DELETE);

    auto getRelationshipType = new seastar::httpd::match_rule(&getRelationshipTypeHandler);
    getRelationshipType->add_str("/db/" + graph.GetName() + "/schema/relationships");
    getRelationshipType->add_param("type");
    routes.add(getRelationshipType, seastar::httpd::operation_type::GET);

    auto postRelationshipType = new seastar::httpd::match_rule(&postRelationshipTypeHandler);
    postRelationshipType->add_str("/db/" + graph.GetName() + "/schema/relationships");
    postRelationshipType->add_param("type");
    routes.add(postRelationshipType, seastar::httpd::operation_type::POST);

    auto deleteRelationshipType = new seastar::httpd::match_rule(&deleteRelationshipTypeHandler);
    deleteRelationshipType->add_str("/db/" + graph.GetName() + "/schema/relationships");
    deleteRelationshipType->add_param("type");
    routes.add(deleteRelationshipType, seastar::httpd::operation_type::DELETE);

    auto getNodeTypeProperty = new seastar::httpd::match_rule(&getNodeTypePropertyHandler);
    getNodeTypeProperty->add_str("/db/" + graph.GetName() + "/schema/nodes");
    getNodeTypeProperty->add_param("type");
    getNodeTypeProperty->add_str("/properties");
    getNodeTypeProperty->add_param("property");
    routes.add(getNodeTypeProperty, seastar::httpd::operation_type::GET);

    auto postNodeTypeProperty = new seastar::httpd::match_rule(&postNodeTypePropertyHandler);
    postNodeTypeProperty->add_str("/db/" + graph.GetName() + "/schema/nodes");
    postNodeTypeProperty->add_param("type");
    postNodeTypeProperty->add_str("/properties");
    postNodeTypeProperty->add_param("property");
    postNodeTypeProperty->add_param("data_type");
    routes.add(postNodeTypeProperty, seastar::httpd::operation_type::POST);

    auto deleteNodeTypeProperty = new seastar::httpd::match_rule(&deleteNodeTypePropertyHandler);
    deleteNodeTypeProperty->add_str("/db/" + graph.GetName() + "/schema/nodes");
    deleteNodeTypeProperty->add_param("type");
    deleteNodeTypeProperty->add_str("/properties");
    deleteNodeTypeProperty->add_param("property");
    routes.add(deleteNodeTypeProperty, seastar::httpd::operation_type::DELETE);

    auto getRelationshipTypeProperty = new seastar::httpd::match_rule(&getRelationshipTypePropertyHandler);
    getRelationshipTypeProperty->add_str("/db/" + graph.GetName() + "/schema/relationships");
    getRelationshipTypeProperty->add_param("type");
    getRelationshipTypeProperty->add_str("/properties");
    getRelationshipTypeProperty->add_param("property");
    routes.add(getRelationshipTypeProperty, seastar::httpd::operation_type::GET);

    auto postRelationshipTypeProperty = new seastar::httpd::match_rule(&postRelationshipTypePropertyHandler);
    postRelationshipTypeProperty->add_str("/db/" + graph.GetName() + "/schema/relationships");
    postRelationshipTypeProperty->add_param("type");
    postRelationshipTypeProperty->add_str("/properties");
    postRelationshipTypeProperty->add_param("property");
    postRelationshipTypeProperty->add_param("data_type");
    routes.add(postRelationshipTypeProperty, seastar::httpd::operation_type::POST);

    auto deleteRelationshipTypeProperty = new seastar::httpd::match_rule(&deleteRelationshipTypePropertyHandler);
    deleteRelationshipTypeProperty->add_str("/db/" + graph.GetName() + "/schema/relationships");
    deleteRelationshipTypeProperty->add_param("type");
    deleteRelationshipTypeProperty->add_str("/properties");
    deleteRelationshipTypeProperty->add_param("property");
    routes.add(deleteRelationshipTypeProperty, seastar::httpd::operation_type::DELETE);

}

future<std::unique_ptr<seastar::http::reply>>
Schema::ClearGraphHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    parent.graph.Log(req->_method, req->get_url());
    parent.graph.Clear();
    rep->set_status(seastar::http::reply::status_type::accepted);
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::GetNodeTypesHandler::handle([[maybe_unused]] const seastar::sstring &path, [[maybe_unused]] std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    std::set<std::string> types = parent.graph.shard.local().NodeTypesGet();
    rep->write_body("json", seastar::json::stream_object(std::vector<std::string>( types.begin(), types.end() )));
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::GetRelationshipTypesHandler::handle([[maybe_unused]] const seastar::sstring &path, [[maybe_unused]] std::unique_ptr<seastar::http::request> req,
                                            std::unique_ptr<seastar::http::reply> rep) {
    std::set<std::string> types = parent.graph.shard.local().RelationshipTypesGet();
    rep->write_body("json", seastar::json::stream_object(std::vector<std::string>( types.begin(), types.end() )));
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::GetNodeTypeHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if (valid_type) {
        std::map<std::string, std::string> type = parent.graph.shard.local().NodeTypeGet(req->param[Utilities::TYPE]);
        rep->write_body("json", seastar::json::stream_object(properties_json(type)));
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::PostNodeTypeHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if (valid_type) {
        parent.graph.Log(req->_method, req->get_url());
        // The node type needs to be set by Shard 0 and propagated
        return parent.graph.shard.invoke_on(0, [type = req->param[Utilities::TYPE]](ragedb::Shard &local_shard) {
            return local_shard.NodeTypeInsertPeered(type);
        }).then(
                [rep = std::move(rep)](bool success) mutable {
                    if (success) {
                        rep->set_status(seastar::http::reply::status_type::created);
                    } else {
                        rep->set_status(seastar::http::reply::status_type::not_modified);
                    }
                    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                });
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::DeleteNodeTypeHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if(valid_type) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().DeleteNodeTypePeered(req->param[Utilities::TYPE]).then([rep = std::move(rep)](bool success) mutable {
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

future<std::unique_ptr<seastar::http::reply>>
Schema::GetRelationshipTypeHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req,
                                           std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if(valid_type) {
        std::map<std::string, std::string> type = parent.graph.shard.local().RelationshipTypeGet(req->param[Utilities::TYPE]);
        rep->write_body("json", seastar::json::stream_object(properties_json(type)));
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::PostRelationshipTypeHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req,
                                            std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if(valid_type) {
        parent.graph.Log(req->_method, req->get_url());
        // The relationship type needs to be set by Shard 0 and propagated
        return parent.graph.shard.invoke_on(0, [type = req->param[Utilities::TYPE]](ragedb::Shard &local_shard) {
            return local_shard.RelationshipTypeInsertPeered(type);
        }).then([rep = std::move(rep)](bool success) mutable {
                    if (success) {
                        rep->set_status(seastar::http::reply::status_type::created);
                    } else {
                        rep->set_status(seastar::http::reply::status_type::not_modified);
                    }
                    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
                });
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::DeleteRelationshipTypeHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req,
                                              std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if(valid_type) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().DeleteRelationshipTypePeered(req->param[Utilities::TYPE]).then([rep = std::move(rep)](bool success) mutable {
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

future<std::unique_ptr<seastar::http::reply>>
Schema::GetNodeTypePropertyHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req,
                                           std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if(valid_type && valid_property) {
        std::string data_type = parent.graph.shard.local().NodePropertyTypeGet(req->param[Utilities::TYPE], req->param[Utilities::PROPERTY]);
        json_properties_builder json;
        json.add_property(req->param[Utilities::PROPERTY], data_type);
        rep->write_body("json", seastar::sstring(json.as_json()));
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::PostNodeTypePropertyHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req,
                                            std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");
    bool valid_data_type = Utilities::validate_parameter(Utilities::DATA_TYPE, req, rep, "Invalid data type");
    bool allowed_data_type = Utilities::validate_allowed_data_type(req, rep);

    if(valid_type && valid_property && valid_data_type && allowed_data_type) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().NodePropertyTypeAddPeered(req->param[Utilities::TYPE], req->param[Utilities::PROPERTY], req->param[Utilities::DATA_TYPE]).then([rep = std::move(rep)](uint8_t property_type_id) mutable {
            if (property_type_id > 0) {
                rep->set_status(seastar::http::reply::status_type::created);
            } else {
                rep->set_status(seastar::http::reply::status_type::not_modified);
            }
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::DeleteNodeTypePropertyHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req,
                                              std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if(valid_type && valid_property) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().NodePropertyTypeDeletePeered(req->param[Utilities::TYPE],
                                                                       req->param[Utilities::PROPERTY]).then([rep = std::move(rep)](bool success) mutable {
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

future<std::unique_ptr<seastar::http::reply>>
Schema::GetRelationshipTypePropertyHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req,
                                                   std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if(valid_type && valid_property) {
        std::string data_type = parent.graph.shard.local().RelationshipPropertyTypeGet(req->param[Utilities::TYPE], req->param[Utilities::PROPERTY]);
        json_properties_builder json;
        json.add_property(req->param[Utilities::PROPERTY], data_type);
        rep->write_body("json", seastar::sstring(json.as_json()));
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::PostRelationshipTypePropertyHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req,
                                                    std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");
    bool valid_data_type = Utilities::validate_parameter(Utilities::DATA_TYPE, req, rep, "Invalid data type");
    bool allowed_data_type = Utilities::validate_allowed_data_type(req, rep);

    if(valid_type && valid_property && valid_data_type && allowed_data_type) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().RelationshipPropertyTypeAddPeered(req->param[Utilities::TYPE], req->param[Utilities::PROPERTY], req->param[Utilities::DATA_TYPE]).then([rep = std::move(rep)](uint8_t property_type_id) mutable {
            if (property_type_id != 0) {
                rep->set_status(seastar::http::reply::status_type::created);
            } else {
                rep->set_status(seastar::http::reply::status_type::not_modified);
            }
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::DeleteRelationshipTypePropertyHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req,
                                                      std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if(valid_type && valid_property) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().RelationshipPropertyTypeDeletePeered(req->param[Utilities::TYPE], req->param[Utilities::PROPERTY]).then([rep = std::move(rep)](bool success) mutable {
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
