/*
 * Copyright RageDB Contributors. All Rights Reserved.
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

#include "Gql.h"
#include "../../gql/GqlParser.h"
#include "../../gql/GqlOptimizer.h"
#include "../../gql/GqlExecutor.h"
#include <iostream>

void Gql::set_routes(seastar::httpd::routes &routes) {
    auto postGql = new seastar::httpd::match_rule(&postGqlHandler);
    postGql->add_str("/db/" + graph.GetName() + "/gql");
    routes.add(postGql, seastar::httpd::operation_type::POST);
}

seastar::future<std::unique_ptr<seastar::http::reply>> Gql::PostGqlHandler::handle(
    [[maybe_unused]] const seastar::sstring &path,
    std::unique_ptr<seastar::http::request> req,
    std::unique_ptr<seastar::http::reply> rep) {

    // Validate request body presence
    if (req->content.empty()) {
        rep->write_body("json", seastar::json::stream_object("Empty query"));
        rep->set_status(seastar::http::reply::status_type::bad_request);
        return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
    }

    // Enforce query string size limit of 64KB
    if (req->content.size() > 65536) {
        rep->write_body("json", seastar::json::stream_object("Query exceeds maximum allowed size of 64KB"));
        rep->set_status(seastar::http::reply::status_type::bad_request);
        return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
    }

    parent.graph.Log(req->_method, req->get_url(), req->content);

    try {
        std::string query_str = req->content;
        auto query = ragedb::gql::GqlParser::parse(query_str);
        ragedb::gql::GqlOptimizer::optimize(query);

        return ragedb::gql::GqlExecutor::execute(parent.graph, std::move(query))
        .then([rep = std::move(rep)](std::string result_json) mutable {
            rep->write_body("json", seastar::sstring(result_json));
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        })
        .handle_exception([rep = std::move(rep)](std::exception_ptr eptr) mutable {
            try {
                std::rethrow_exception(eptr);
            } catch (const std::exception& e) {
                std::cerr << "GQL execution error: " << e.what() << std::endl;
            }
            rep->write_body("json", seastar::json::stream_object("Internal GQL execution error"));
            rep->set_status(seastar::http::reply::status_type::internal_server_error);
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });

    } catch (const std::exception& e) {
        std::cerr << "GQL compile error: " << e.what() << std::endl;
        rep->write_body("json", seastar::json::stream_object("GQL query compile or syntax error"));
        rep->set_status(seastar::http::reply::status_type::bad_request);
    } catch (...) {
        std::cerr << "Unknown GQL compile error" << std::endl;
        rep->write_body("json", seastar::json::stream_object("GQL query compile or syntax error"));
        rep->set_status(seastar::http::reply::status_type::bad_request);
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}
