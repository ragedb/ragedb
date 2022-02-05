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

void RelationshipProperties::set_routes(routes &routes) {

    auto getRelationshipPropertyById = new match_rule(&getRelationshipPropertyByIdHandler);
    getRelationshipPropertyById->add_str("/db/" + graph.GetName() + "/relationship");
    getRelationshipPropertyById->add_param("id");
    getRelationshipPropertyById->add_str("/property");
    getRelationshipPropertyById->add_param("property");
    routes.add(getRelationshipPropertyById, operation_type::GET);

    auto putRelationshipPropertyById = new match_rule(&putRelationshipPropertyByIdHandler);
    putRelationshipPropertyById->add_str("/db/" + graph.GetName() + "/relationship");
    putRelationshipPropertyById->add_param("id");
    putRelationshipPropertyById->add_str("/property");
    putRelationshipPropertyById->add_param("property");
    routes.add(putRelationshipPropertyById, operation_type::PUT);

    auto deleteRelationshipPropertyById = new match_rule(&deleteRelationshipPropertyByIdHandler);
    deleteRelationshipPropertyById->add_str("/db/" + graph.GetName() + "/relationship");
    deleteRelationshipPropertyById->add_param("id");
    deleteRelationshipPropertyById->add_str("/property");
    deleteRelationshipPropertyById->add_param("property");
    routes.add(deleteRelationshipPropertyById, operation_type::DELETE);

    auto getRelationshipPropertiesById = new match_rule(&getRelationshipPropertiesByIdHandler);
    getRelationshipPropertiesById->add_str("/db/" + graph.GetName() + "/relationship");
    getRelationshipPropertiesById->add_param("id");
    getRelationshipPropertiesById->add_str("/properties");
    routes.add(getRelationshipPropertiesById, operation_type::GET);

    auto postRelationshipPropertiesById = new match_rule(&postRelationshipPropertiesByIdHandler);
    postRelationshipPropertiesById->add_str("/db/" + graph.GetName() + "/relationship");
    postRelationshipPropertiesById->add_param("id");
    postRelationshipPropertiesById->add_str("/properties");
    routes.add(postRelationshipPropertiesById, operation_type::POST);

    auto putRelationshipPropertiesById = new match_rule(&putRelationshipPropertiesByIdHandler);
    putRelationshipPropertiesById->add_str("/db/" + graph.GetName() + "/relationship");
    putRelationshipPropertiesById->add_param("id");
    putRelationshipPropertiesById->add_str("/properties");
    routes.add(putRelationshipPropertiesById, operation_type::PUT);

    auto deleteRelationshipPropertiesById = new match_rule(&deleteRelationshipPropertiesByIdHandler);
    deleteRelationshipPropertiesById->add_str("/db/" + graph.GetName() + "/relationship");
    deleteRelationshipPropertiesById->add_param("id");
    deleteRelationshipPropertiesById->add_str("/properties");
    routes.add(deleteRelationshipPropertiesById, operation_type::DELETE);
}

future<std::unique_ptr<reply>> RelationshipProperties::GetRelationshipPropertyByIdHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        return parent.graph.shard.local().RelationshipPropertyGetPeered(id, req->param[Utilities::PROPERTY])
        .then([req = std::move(req), rep = std::move(rep)] (const property_type_t& property) mutable {
                    json_properties_builder json;
                    json.add_property(req->param[Utilities::PROPERTY], property);
                    rep->write_body("json", sstring(json.as_json()));
                    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                });
    }

    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> RelationshipProperties::PutRelationshipPropertyByIdHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if (id > 0 && valid_property) {
        parent.graph.Log(req->_method, req->get_url(), req->content);
        return parent.graph.shard.local().RelationshipPropertySetFromJsonPeered(id, req->param[Utilities::PROPERTY], req->content.c_str())
                .then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<reply>> RelationshipProperties::DeleteRelationshipPropertyByIdHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);
    bool valid_property = Utilities::validate_parameter(Utilities::PROPERTY, req, rep, "Invalid property");

    if (id > 0 && valid_property) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().RelationshipPropertyDeletePeered(id, req->param[Utilities::PROPERTY])
                .then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<reply>> RelationshipProperties::GetRelationshipPropertiesByIdHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        return parent.graph.shard.local().RelationshipPropertiesGetPeered(id)
                .then([rep = std::move(rep)] (const std::map<std::string, property_type_t>& properties) mutable {
                    json_properties_builder json;
                    json.add_properties(properties);
                    rep->write_body("json", sstring(json.as_json()));
                    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                });
    }

    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> RelationshipProperties::PostRelationshipPropertiesByIdHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        if (Utilities::validate_json(req, rep)) {
            parent.graph.Log(req->_method, req->get_url(), req->content);
            return parent.graph.shard.local().RelationshipPropertiesResetFromJsonPeered(id, req->content.c_str())
                    .then([rep = std::move(rep)] (bool success) mutable {
                        if(success) {
                            rep->set_status(reply::status_type::no_content);
                        } else {
                            rep->set_status(reply::status_type::not_modified);
                        }
                        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                    });
        }
    }

    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> RelationshipProperties::PutRelationshipPropertiesByIdHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        if (Utilities::validate_json(req, rep)) {
            parent.graph.Log(req->_method, req->get_url(), req->content);
            return parent.graph.shard.local().RelationshipPropertiesSetFromJsonPeered(id, req->content.c_str())
                    .then([rep = std::move(rep)](bool success) mutable {
                        if (success) {
                            rep->set_status(reply::status_type::no_content);
                        } else {
                            rep->set_status(reply::status_type::not_modified);
                        }
                        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                    });
        }
    }

    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> RelationshipProperties::DeleteRelationshipPropertiesByIdHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    uint64_t id = Utilities::validate_id(req, rep);

    if (id > 0) {
        parent.graph.Log(req->_method, req->get_url());
        return parent.graph.shard.local().RelationshipPropertiesDeletePeered(id)
                .then([rep = std::move(rep)] (bool success) mutable {
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