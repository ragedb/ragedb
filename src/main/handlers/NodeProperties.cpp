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
#include "NodeProperties.h"

void NodeProperties::set_routes(seastar::routes &routes) {
    auto getNodeProperty = new seastar::match_rule(&getNodePropertyHandler);
    getNodeProperty->add_str("/db/" + graph.GetName() + "/node");
    getNodeProperty->add_param("type");
    getNodeProperty->add_param("key");
    getNodeProperty->add_str("/property");
    getNodeProperty->add_param("property");
    routes.add(getNodeProperty, seastar::operation_type::GET);

    auto getNodePropertyById = new seastar::match_rule(&getNodePropertyByIdHandler);
    getNodePropertyById->add_str("/db/" + graph.GetName() + "/node");
    getNodePropertyById->add_param("id");
    getNodePropertyById->add_str("/property");
    getNodePropertyById->add_param("property");
    routes.add(getNodePropertyById, seastar::operation_type::GET);

    auto putNodeProperty = new seastar::match_rule(&putNodePropertyHandler);
    putNodeProperty->add_str("/db/" + graph.GetName() + "/node");
    putNodeProperty->add_param("type");
    putNodeProperty->add_param("key");
    putNodeProperty->add_str("/property");
    putNodeProperty->add_param("property");
    routes.add(putNodeProperty, seastar::operation_type::PUT);

    auto putNodePropertyById = new seastar::match_rule(&putNodePropertyByIdHandler);
    putNodePropertyById->add_str("/db/" + graph.GetName() + "/node");
    putNodePropertyById->add_param("id");
    putNodePropertyById->add_str("/property");
    putNodePropertyById->add_param("property");
    routes.add(putNodePropertyById, seastar::operation_type::PUT);

    auto deleteNodeProperty = new seastar::match_rule(&deleteNodePropertyHandler);
    deleteNodeProperty->add_str("/db/" + graph.GetName() + "/node");
    deleteNodeProperty->add_param("type");
    deleteNodeProperty->add_param("key");
    deleteNodeProperty->add_str("/property");
    deleteNodeProperty->add_param("property");
    routes.add(deleteNodeProperty, seastar::operation_type::DELETE);

    auto deleteNodePropertyById = new seastar::match_rule(&deleteNodePropertyByIdHandler);
    deleteNodePropertyById->add_str("/db/" + graph.GetName() + "/node");
    deleteNodePropertyById->add_param("id");
    deleteNodePropertyById->add_str("/property");
    deleteNodePropertyById->add_param("property");
    routes.add(deleteNodePropertyById, seastar::operation_type::DELETE);

    auto getNodeProperties = new seastar::match_rule(&getNodePropertiesHandler);
    getNodeProperties->add_str("/db/" + graph.GetName() + "/node");
    getNodeProperties->add_param("type");
    getNodeProperties->add_param("key");
    getNodeProperties->add_str("/properties");
    routes.add(getNodeProperties, seastar::operation_type::GET);

    auto getNodePropertiesById = new seastar::match_rule(&getNodePropertiesByIdHandler);
    getNodePropertiesById->add_str("/db/" + graph.GetName() + "/node");
    getNodePropertiesById->add_param("id");
    getNodePropertiesById->add_str("/properties");
    routes.add(getNodePropertiesById, seastar::operation_type::GET);

    auto postNodeProperties = new seastar::match_rule(&postNodePropertiesHandler);
    postNodeProperties->add_str("/db/" + graph.GetName() + "/node");
    postNodeProperties->add_param("type");
    postNodeProperties->add_param("key");
    postNodeProperties->add_str("/properties");
    routes.add(postNodeProperties, seastar::operation_type::POST);

    auto postNodePropertiesById = new seastar::match_rule(&postNodePropertiesByIdHandler);
    postNodePropertiesById->add_str("/db/" + graph.GetName() + "/node");
    postNodePropertiesById->add_param("id");
    postNodePropertiesById->add_str("/properties");
    routes.add(postNodePropertiesById, seastar::operation_type::POST);

    auto putNodeProperties = new seastar::match_rule(&putNodePropertiesHandler);
    putNodeProperties->add_str("/db/" + graph.GetName() + "/node");
    putNodeProperties->add_param("type");
    putNodeProperties->add_param("key");
    putNodeProperties->add_str("/properties");
    routes.add(putNodeProperties, seastar::operation_type::PUT);

    auto putNodePropertiesById = new seastar::match_rule(&putNodePropertiesByIdHandler);
    putNodePropertiesById->add_str("/db/" + graph.GetName() + "/node");
    putNodePropertiesById->add_param("id");
    putNodePropertiesById->add_str("/properties");
    routes.add(putNodePropertiesById, seastar::operation_type::PUT);

    auto deleteNodeProperties = new seastar::match_rule(&deleteNodePropertiesHandler);
    deleteNodeProperties->add_str("/db/" + graph.GetName() + "/node");
    deleteNodeProperties->add_param("type");
    deleteNodeProperties->add_param("key");
    deleteNodeProperties->add_str("/properties");
    routes.add(deleteNodeProperties, seastar::operation_type::DELETE);

    auto deleteNodePropertiesById = new seastar::match_rule(&deleteNodePropertiesByIdHandler);
    deleteNodePropertiesById->add_str("/db/" + graph.GetName() + "/node");
    deleteNodePropertiesById->add_param("id");
    deleteNodePropertiesById->add_str("/properties");
    routes.add(deleteNodePropertiesById, seastar::operation_type::DELETE);
}

future<std::unique_ptr<seastar::http::reply>> NodeProperties::GetNodePropertyHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if(valid_type && valid_key && valid_property) {
        return parent.graph.shard.local().NodeGetPropertyPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], req->param[Utilities::PROPERTY])
        .then([req = std::move(req), rep = std::move(rep)] (const property_type_t& property) mutable {
            json_properties_builder json;
            json.add_property(req->param[Utilities::PROPERTY], property);
            rep->write_body("json", seastar::sstring(json.as_json()));
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}


future<std::unique_ptr<seastar::http::reply>> NodeProperties::GetNodePropertyByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if (id > 0 && valid_property) {
        return parent.graph.shard.local().NodeGetPropertyPeered(id, req->param[Utilities::PROPERTY])
        .then([req = std::move(req), rep = std::move(rep)] (const property_type_t& property) mutable {
            json_properties_builder json;
            json.add_property(req->param[Utilities::PROPERTY], property);
            rep->write_body("json", seastar::sstring(json.as_json()));
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> NodeProperties::PutNodePropertyHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if(valid_type && valid_key && valid_property) {
        parent.graph.Log(req->_method, req->get_url(), req->content);
        return parent.graph.shard.invoke_on(seastar::this_shard_id(), [req = std::move(req)] (ragedb::Shard &local_shard) {
            return local_shard.NodeSetPropertyFromJsonPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], req->param[Utilities::PROPERTY], req->content.c_str());
        }).then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<seastar::http::reply>> NodeProperties::PutNodePropertyByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if (id > 0 && valid_property) {
        parent.graph.Log(req->_method, req->get_url(), req->content);
        uint16_t node_shard_id = Shard::CalculateShardId(id);
        return parent.graph.shard.invoke_on(node_shard_id, [id, req = std::move(req)] (ragedb::Shard &local_shard) {
            return local_shard.NodeSetPropertyFromJson(id, req->param[Utilities::PROPERTY], req->content.c_str());
        }).then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<seastar::http::reply>> NodeProperties::DeleteNodePropertyHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if(valid_type && valid_key && valid_property) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.invoke_on(seastar::this_shard_id(), [req = std::move(req)] (ragedb::Shard &local_shard) {
            return local_shard.NodeDeletePropertyPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], req->param[Utilities::PROPERTY]);
        }).then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<seastar::http::reply>> NodeProperties::DeleteNodePropertyByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if (id > 0 && valid_property) {
        parent.graph.Log(req->_method, req->get_url());
        uint16_t node_shard_id = Shard::CalculateShardId(id);
        return parent.graph.shard.invoke_on(node_shard_id, [id, req = std::move(req)] (ragedb::Shard &local_shard) {
            return local_shard.NodeDeleteProperty(id, req->param[Utilities::PROPERTY]);
        }).then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<seastar::http::reply>> NodeProperties::GetNodePropertiesHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

    if(valid_type && valid_key) {
        return parent.graph.shard.invoke_on(seastar::this_shard_id(), [req = std::move(req)] (ragedb::Shard &local_shard) {
            return local_shard.NodeGetPropertiesPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY]);
        }).then([rep = std::move(rep)] (const std::map<std::string, property_type_t>& properties) mutable {
            json_properties_builder json;
            json.add_properties(properties);
            rep->write_body("json", seastar::sstring(json.as_json()));
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> NodeProperties::GetNodePropertiesByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        uint16_t node_shard_id = Shard::CalculateShardId(id);
        return parent.graph.shard.invoke_on(node_shard_id, [id] (ragedb::Shard &local_shard) {
            return local_shard.NodeGetProperties(id);
        }).then([rep = std::move(rep)] (const std::map<std::string, property_type_t>& properties) mutable {
            json_properties_builder json;
            json.add_properties(properties);
            rep->write_body("json", seastar::sstring(json.as_json()));
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> NodeProperties::PostNodePropertiesHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

    if(valid_type && valid_key) {
        if (Utilities::validate_json(req, rep)) {
            parent.graph.Log(req->_method, req->get_url(), req->content);
            return parent.graph.shard.invoke_on(seastar::this_shard_id(), [req = std::move(req)](ragedb::Shard &local_shard) {
                return local_shard.NodeResetPropertiesFromJsonPeered(req->param[Utilities::TYPE],
                                         req->param[Utilities::KEY],
                                         req->content.c_str());
            }).then([rep = std::move(rep)](bool success) mutable {
                if (success) {
                    rep->set_status(seastar::http::reply::status_type::no_content);
                } else {
                    rep->set_status(seastar::http::reply::status_type::not_modified);
                }
                return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
            });
        }
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> NodeProperties::PostNodePropertiesByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        if (Utilities::validate_json(req, rep)) {
            parent.graph.Log(req->_method, req->get_url(), req->content);
            uint16_t node_shard_id = Shard::CalculateShardId(id);
            return parent.graph.shard.invoke_on(node_shard_id, [id, req = std::move(req)](ragedb::Shard &local_shard) {
                return local_shard.NodeResetPropertiesFromJson(id, req->content.c_str());
            }).then([rep = std::move(rep)](bool success) mutable {
                if (success) {
                    rep->set_status(seastar::http::reply::status_type::no_content);
                } else {
                    rep->set_status(seastar::http::reply::status_type::not_modified);
                }
                return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
            });
        }
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> NodeProperties::PutNodePropertiesHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

    if(valid_type && valid_key) {
        if (Utilities::validate_json(req, rep)) {
            parent.graph.Log(req->_method, req->get_url(), req->content);
            return parent.graph.shard.invoke_on(seastar::this_shard_id(), [req = std::move(req)](ragedb::Shard &local_shard) {
                return local_shard.NodeSetPropertiesFromJsonPeered(req->param[Utilities::TYPE],
                                         req->param[Utilities::KEY],
                                         req->content.c_str());
            }).then([rep = std::move(rep)](bool success) mutable {
                if (success) {
                    rep->set_status(seastar::http::reply::status_type::no_content);
                } else {
                    rep->set_status(seastar::http::reply::status_type::not_modified);
                }
                return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
            });
        }
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> NodeProperties::PutNodePropertiesByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        if (Utilities::validate_json(req, rep)) {
            parent.graph.Log(req->_method, req->get_url(), req->content);
            uint16_t node_shard_id = Shard::CalculateShardId(id);
            return parent.graph.shard.invoke_on(node_shard_id, [id, req = std::move(req)](ragedb::Shard &local_shard) {
                return local_shard.NodeSetPropertiesFromJson(id, req->content.c_str());
            }).then([rep = std::move(rep)](bool success) mutable {
                if (success) {
                    rep->set_status(seastar::http::reply::status_type::no_content);
                } else {
                    rep->set_status(seastar::http::reply::status_type::not_modified);
                }
                return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
            });
        }
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> NodeProperties::DeleteNodePropertiesHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
    bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

    if(valid_type && valid_key) {
        parent.graph.Log(req->_method, req->get_url(), req->content);
        return parent.graph.shard.invoke_on(seastar::this_shard_id(), [req = std::move(req)] (ragedb::Shard &local_shard) {
            return local_shard.NodeDeletePropertiesPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY]);
        }).then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<seastar::http::reply>> NodeProperties::DeleteNodePropertiesByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, const std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        parent.graph.Log(req->_method, req->get_url(), req->content);
        uint16_t node_shard_id = Shard::CalculateShardId(id);
        return parent.graph.shard.invoke_on(node_shard_id, [id] (ragedb::Shard &local_shard) {
            return local_shard.NodeDeleteProperties(id);
        }).then([rep = std::move(rep)] (bool success) mutable {
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
