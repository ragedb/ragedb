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
#include "RelationshipProperties.h"

void RelationshipProperties::set_routes(seastar::routes &routes) {

    auto getRelationshipPropertyById = new seastar::match_rule(&getRelationshipPropertyByIdHandler);
    getRelationshipPropertyById->add_str("/db/" + graph.GetName() + "/relationship");
    getRelationshipPropertyById->add_param("id");
    getRelationshipPropertyById->add_str("/property");
    getRelationshipPropertyById->add_param("property");
    routes.add(getRelationshipPropertyById, seastar::operation_type::GET);

    auto putRelationshipPropertyById = new seastar::match_rule(&putRelationshipPropertyByIdHandler);
    putRelationshipPropertyById->add_str("/db/" + graph.GetName() + "/relationship");
    putRelationshipPropertyById->add_param("id");
    putRelationshipPropertyById->add_str("/property");
    putRelationshipPropertyById->add_param("property");
    routes.add(putRelationshipPropertyById, seastar::operation_type::PUT);

    auto deleteRelationshipPropertyById = new seastar::match_rule(&deleteRelationshipPropertyByIdHandler);
    deleteRelationshipPropertyById->add_str("/db/" + graph.GetName() + "/relationship");
    deleteRelationshipPropertyById->add_param("id");
    deleteRelationshipPropertyById->add_str("/property");
    deleteRelationshipPropertyById->add_param("property");
    routes.add(deleteRelationshipPropertyById, seastar::operation_type::DELETE);

    auto getRelationshipPropertiesById = new seastar::match_rule(&getRelationshipPropertiesByIdHandler);
    getRelationshipPropertiesById->add_str("/db/" + graph.GetName() + "/relationship");
    getRelationshipPropertiesById->add_param("id");
    getRelationshipPropertiesById->add_str("/properties");
    routes.add(getRelationshipPropertiesById, seastar::operation_type::GET);

    auto postRelationshipPropertiesById = new seastar::match_rule(&postRelationshipPropertiesByIdHandler);
    postRelationshipPropertiesById->add_str("/db/" + graph.GetName() + "/relationship");
    postRelationshipPropertiesById->add_param("id");
    postRelationshipPropertiesById->add_str("/properties");
    routes.add(postRelationshipPropertiesById, seastar::operation_type::POST);

    auto putRelationshipPropertiesById = new seastar::match_rule(&putRelationshipPropertiesByIdHandler);
    putRelationshipPropertiesById->add_str("/db/" + graph.GetName() + "/relationship");
    putRelationshipPropertiesById->add_param("id");
    putRelationshipPropertiesById->add_str("/properties");
    routes.add(putRelationshipPropertiesById, seastar::operation_type::PUT);

    auto deleteRelationshipPropertiesById = new seastar::match_rule(&deleteRelationshipPropertiesByIdHandler);
    deleteRelationshipPropertiesById->add_str("/db/" + graph.GetName() + "/relationship");
    deleteRelationshipPropertiesById->add_param("id");
    deleteRelationshipPropertiesById->add_str("/properties");
    routes.add(deleteRelationshipPropertiesById, seastar::operation_type::DELETE);
}

future<std::unique_ptr<seastar::httpd::reply>> RelationshipProperties::GetRelationshipPropertyByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        return parent.graph.shard.local().RelationshipGetPropertyPeered(id, req->param[Utilities::PROPERTY])
        .then([req = std::move(req), rep = std::move(rep)] (const property_type_t& property) mutable {
                    json_properties_builder json;
                    json.add_property(req->param[Utilities::PROPERTY], property);
                    rep->write_body("json", seastar::sstring(json.as_json()));
                    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                });
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::httpd::reply>> RelationshipProperties::PutRelationshipPropertyByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if (id > 0 && valid_property) {
        parent.graph.Log(req->_method, req->get_url(), req->content);
        return parent.graph.shard.local().RelationshipSetPropertyFromJsonPeered(id, req->param[Utilities::PROPERTY], req->content.c_str())
                .then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<seastar::httpd::reply>> RelationshipProperties::DeleteRelationshipPropertyByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if (id > 0 && valid_property) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().RelationshipDeletePropertyPeered(id, req->param[Utilities::PROPERTY])
                .then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<seastar::httpd::reply>> RelationshipProperties::GetRelationshipPropertiesByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        return parent.graph.shard.local().RelationshipGetPropertiesPeered(id)
                .then([rep = std::move(rep)] (const std::map<std::string, property_type_t>& properties) mutable {
                    json_properties_builder json;
                    json.add_properties(properties);
                    rep->write_body("json", seastar::sstring(json.as_json()));
                    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                });
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::httpd::reply>> RelationshipProperties::PostRelationshipPropertiesByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        if (Utilities::validate_json(req, rep)) {
            parent.graph.Log(req->_method, req->get_url(), req->content);
            return parent.graph.shard.local().RelationshipResetPropertiesFromJsonPeered(id, req->content.c_str())
                    .then([rep = std::move(rep)] (bool success) mutable {
                        if(success) {
                            rep->set_status(seastar::httpd::reply::status_type::no_content);
                        } else {
                            rep->set_status(seastar::httpd::reply::status_type::not_modified);
                        }
                        return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                    });
        }
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::httpd::reply>> RelationshipProperties::PutRelationshipPropertiesByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        if (Utilities::validate_json(req, rep)) {
            parent.graph.Log(req->_method, req->get_url(), req->content);
            return parent.graph.shard.local().RelationshipSetPropertiesFromJsonPeered(id, req->content.c_str())
                    .then([rep = std::move(rep)](bool success) mutable {
                        if (success) {
                            rep->set_status(seastar::httpd::reply::status_type::no_content);
                        } else {
                            rep->set_status(seastar::httpd::reply::status_type::not_modified);
                        }
                        return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                    });
        }
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::httpd::reply>> RelationshipProperties::DeleteRelationshipPropertiesByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().RelationshipDeletePropertiesPeered(id)
                .then([rep = std::move(rep)] (bool success) mutable {
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