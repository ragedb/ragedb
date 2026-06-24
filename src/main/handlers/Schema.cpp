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
#include "../../gql/GqlVirtualCatalog.h"

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

    auto getNodeIndexes = new seastar::httpd::match_rule(&getNodeIndexesHandler);
    getNodeIndexes->add_str("/db/" + graph.GetName() + "/schema/nodes/indexes");
    routes.add(getNodeIndexes, seastar::httpd::operation_type::GET);

    auto getNodeTypeIndexes = new seastar::httpd::match_rule(&getNodeTypeIndexesHandler);
    getNodeTypeIndexes->add_str("/db/" + graph.GetName() + "/schema/nodes");
    getNodeTypeIndexes->add_param("type");
    getNodeTypeIndexes->add_str("/indexes");
    routes.add(getNodeTypeIndexes, seastar::httpd::operation_type::GET);

    auto getRelationshipIndexes = new seastar::httpd::match_rule(&getRelationshipIndexesHandler);
    getRelationshipIndexes->add_str("/db/" + graph.GetName() + "/schema/relationships/indexes");
    routes.add(getRelationshipIndexes, seastar::httpd::operation_type::GET);

    auto getRelationshipTypeIndexes = new seastar::httpd::match_rule(&getRelationshipTypeIndexesHandler);
    getRelationshipTypeIndexes->add_str("/db/" + graph.GetName() + "/schema/relationships");
    getRelationshipTypeIndexes->add_param("type");
    getRelationshipTypeIndexes->add_str("/indexes");
    routes.add(getRelationshipTypeIndexes, seastar::httpd::operation_type::GET);

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

    auto postNodeTypePropertyIndex = new seastar::httpd::match_rule(&postNodeTypePropertyIndexHandler);
    postNodeTypePropertyIndex->add_str("/db/" + graph.GetName() + "/schema/nodes");
    postNodeTypePropertyIndex->add_param("type");
    postNodeTypePropertyIndex->add_str("/properties");
    postNodeTypePropertyIndex->add_param("property");
    postNodeTypePropertyIndex->add_str("/index");
    routes.add(postNodeTypePropertyIndex, seastar::httpd::operation_type::POST);

    auto deleteNodeTypePropertyIndex = new seastar::httpd::match_rule(&deleteNodeTypePropertyIndexHandler);
    deleteNodeTypePropertyIndex->add_str("/db/" + graph.GetName() + "/schema/nodes");
    deleteNodeTypePropertyIndex->add_param("type");
    deleteNodeTypePropertyIndex->add_str("/properties");
    deleteNodeTypePropertyIndex->add_param("property");
    deleteNodeTypePropertyIndex->add_str("/index");
    routes.add(deleteNodeTypePropertyIndex, seastar::httpd::operation_type::DELETE);

    auto postRelationshipTypePropertyIndex = new seastar::httpd::match_rule(&postRelationshipTypePropertyIndexHandler);
    postRelationshipTypePropertyIndex->add_str("/db/" + graph.GetName() + "/schema/relationships");
    postRelationshipTypePropertyIndex->add_param("type");
    postRelationshipTypePropertyIndex->add_str("/properties");
    postRelationshipTypePropertyIndex->add_param("property");
    postRelationshipTypePropertyIndex->add_str("/index");
    routes.add(postRelationshipTypePropertyIndex, seastar::httpd::operation_type::POST);

    auto deleteRelationshipTypePropertyIndex = new seastar::httpd::match_rule(&deleteRelationshipTypePropertyIndexHandler);
    deleteRelationshipTypePropertyIndex->add_str("/db/" + graph.GetName() + "/schema/relationships");
    deleteRelationshipTypePropertyIndex->add_param("type");
    deleteRelationshipTypePropertyIndex->add_str("/properties");
    deleteRelationshipTypePropertyIndex->add_param("property");
    deleteRelationshipTypePropertyIndex->add_str("/index");
    routes.add(deleteRelationshipTypePropertyIndex, seastar::httpd::operation_type::DELETE);

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

    auto postRelationshipTypeAlgebra = new seastar::httpd::match_rule(&postRelationshipTypeAlgebraHandler);
    postRelationshipTypeAlgebra->add_str("/db/" + graph.GetName() + "/schema/relationships");
    postRelationshipTypeAlgebra->add_param("type");
    postRelationshipTypeAlgebra->add_str("/algebra");
    routes.add(postRelationshipTypeAlgebra, seastar::httpd::operation_type::POST);
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
        std::map<std::string, std::string> type = parent.graph.shard.local().NodeTypeGet(req->get_path_param(Utilities::TYPE));
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
        return parent.graph.shard.invoke_on(0, [type = req->get_path_param(Utilities::TYPE)](ragedb::Shard &local_shard) {
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
        return parent.graph.shard.local().DeleteNodeTypePeered(req->get_path_param(Utilities::TYPE)).then([rep = std::move(rep)](bool success) mutable {
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
        std::map<std::string, std::string> type = parent.graph.shard.local().RelationshipTypeGet(req->get_path_param(Utilities::TYPE));
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
        return parent.graph.shard.invoke_on(0, [type = req->get_path_param(Utilities::TYPE)](ragedb::Shard &local_shard) {
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
        return parent.graph.shard.local().DeleteRelationshipTypePeered(req->get_path_param(Utilities::TYPE)).then([rep = std::move(rep)](bool success) mutable {
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
        std::string data_type = parent.graph.shard.local().NodePropertyTypeGet(req->get_path_param(Utilities::TYPE), req->get_path_param(Utilities::PROPERTY));
        json_properties_builder json;
        json.add_property(req->get_path_param(Utilities::PROPERTY), data_type);
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
        return parent.graph.shard.local().NodePropertyTypeAddPeered(req->get_path_param(Utilities::TYPE), req->get_path_param(Utilities::PROPERTY), req->get_path_param(Utilities::DATA_TYPE)).then([rep = std::move(rep)](uint8_t property_type_id) mutable {
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
        return parent.graph.shard.local().NodePropertyTypeDeletePeered(req->get_path_param(Utilities::TYPE),
                                                                       req->get_path_param(Utilities::PROPERTY)).then([rep = std::move(rep)](bool success) mutable {
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
        std::string data_type = parent.graph.shard.local().RelationshipPropertyTypeGet(req->get_path_param(Utilities::TYPE), req->get_path_param(Utilities::PROPERTY));
        json_properties_builder json;
        json.add_property(req->get_path_param(Utilities::PROPERTY), data_type);
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
        return parent.graph.shard.local().RelationshipPropertyTypeAddPeered(req->get_path_param(Utilities::TYPE), req->get_path_param(Utilities::PROPERTY), req->get_path_param(Utilities::DATA_TYPE)).then([rep = std::move(rep)](uint8_t property_type_id) mutable {
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
        return parent.graph.shard.local().RelationshipPropertyTypeDeletePeered(req->get_path_param(Utilities::TYPE), req->get_path_param(Utilities::PROPERTY)).then([rep = std::move(rep)](bool success) mutable {
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
Schema::PostNodeTypePropertyIndexHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if (valid_type && valid_property) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().NodeIndexCreatePeered(req->get_path_param(Utilities::TYPE), req->get_path_param(Utilities::PROPERTY)).then([rep = std::move(rep)](bool success) mutable {
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
Schema::DeleteNodeTypePropertyIndexHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if (valid_type && valid_property) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().NodeIndexDeletePeered(req->get_path_param(Utilities::TYPE), req->get_path_param(Utilities::PROPERTY)).then([rep = std::move(rep)](bool success) mutable {
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
Schema::PostRelationshipTypePropertyIndexHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if (valid_type && valid_property) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().RelationshipIndexCreatePeered(req->get_path_param(Utilities::TYPE), req->get_path_param(Utilities::PROPERTY)).then([rep = std::move(rep)](bool success) mutable {
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
Schema::DeleteRelationshipTypePropertyIndexHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if (valid_type && valid_property) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().RelationshipIndexDeletePeered(req->get_path_param(Utilities::TYPE), req->get_path_param(Utilities::PROPERTY)).then([rep = std::move(rep)](bool success) mutable {
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
Schema::GetNodeIndexesHandler::handle([[maybe_unused]] const seastar::sstring &path, [[maybe_unused]] std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    auto node_indexes = parent.graph.shard.local().NodeIndexesGet();
    std::string json = "{";
    bool first_type = true;
    for (const auto& [type_name, properties] : node_indexes) {
        if (!first_type) json += ", ";
        first_type = false;
        json += "\"" + type_name + "\": [";
        bool first_prop = true;
        for (const auto& property : properties) {
            if (!first_prop) json += ", ";
            first_prop = false;
            json += "\"" + property + "\"";
        }
        json += "]";
    }
    json += "}";
    rep->write_body("json", seastar::sstring(json));
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::GetNodeTypeIndexesHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if (valid_type) {
        std::string type_name = req->get_path_param(Utilities::TYPE);
        auto node_indexes = parent.graph.shard.local().NodeIndexesGet();
        std::string json = "[";
        auto type_it = node_indexes.find(type_name);
        if (type_it != node_indexes.end()) {
            bool first_prop = true;
            for (const auto& property : type_it->second) {
                if (!first_prop) json += ", ";
                first_prop = false;
                json += "\"" + property + "\"";
            }
        }
        json += "]";
        rep->write_body("json", seastar::sstring(json));
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::GetRelationshipIndexesHandler::handle([[maybe_unused]] const seastar::sstring &path, [[maybe_unused]] std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    auto rel_indexes = parent.graph.shard.local().RelationshipIndexesGet();
    std::string json = "{";
    bool first_type = true;
    for (const auto& [type_name, properties] : rel_indexes) {
        if (!first_type) json += ", ";
        first_type = false;
        json += "\"" + type_name + "\": [";
        bool first_prop = true;
        for (const auto& property : properties) {
            if (!first_prop) json += ", ";
            first_prop = false;
            json += "\"" + property + "\"";
        }
        json += "]";
    }
    json += "}";
    rep->write_body("json", seastar::sstring(json));
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::GetRelationshipTypeIndexesHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if (valid_type) {
        std::string type_name = req->get_path_param(Utilities::TYPE);
        auto rel_indexes = parent.graph.shard.local().RelationshipIndexesGet();
        std::string json = "[";
        auto type_it = rel_indexes.find(type_name);
        if (type_it != rel_indexes.end()) {
            bool first_prop = true;
            for (const auto& property : type_it->second) {
                if (!first_prop) json += ", ";
                first_prop = false;
                json += "\"" + property + "\"";
            }
        }
        json += "]";
        rep->write_body("json", seastar::sstring(json));
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>>
Schema::PostRelationshipTypeAlgebraHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req,
                                                  std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");

    if (valid_type) {
        parent.graph.Log(req->_method, req->get_url(), req->content);
        std::string rel_type = req->get_path_param(Utilities::TYPE);

        // Parse JSON array of properties from req->content
        std::unordered_set<std::string> props;
        try {
            simdjson::dom::parser parser;
            simdjson::dom::element value;
            simdjson::error_code error = parser.parse(req->content).get(value);
            if (!error && value.type() == simdjson::dom::element_type::ARRAY) {
                simdjson::dom::array array = value.get_array();
                for (auto val : array) {
                    props.insert(std::string(val.get_string().value()));
                }
            } else {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                rep->write_body("json", seastar::json::stream_object("Invalid JSON array of strings"));
                return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
            }
        } catch (const std::exception& e) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->write_body("json", seastar::json::stream_object(std::string("Error parsing JSON: ") + e.what()));
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        }

        // Broadcast to all reactors to update thread-local GqlVirtualCatalog
        return parent.graph.shard.invoke_on_all([rel_type, props](ragedb::Shard&) {
            ragedb::gql::GqlVirtualCatalog::local().set_relationship_algebraic_properties(rel_type, props);
        }).then([rep = std::move(rep)]() mutable {
            rep->set_status(seastar::http::reply::status_type::ok);
            rep->write_body("json", seastar::json::stream_object("Algebraic properties updated"));
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

