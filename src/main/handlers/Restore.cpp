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

#include "Restore.h"
#include "../json/JSON.h"

void Restore::set_routes(seastar::httpd::routes &routes) {
    auto restore = new seastar::httpd::match_rule(&restoreHandler);
    restore->add_str("/db/" + graph.GetName() + "/restore");
    routes.add(restore, seastar::httpd::operation_type::POST);
}

future<std::unique_ptr<seastar::http::reply>> Restore::RestoreHandler::handle(
        [[maybe_unused]] const seastar::sstring &path,
        [[maybe_unused]] std::unique_ptr<seastar::http::request> req,
        std::unique_ptr<seastar::http::reply> rep) {

    return parent.graph.shard.local().RestorePeered(parent.graph.GetName()).then([rep = std::move(rep)] (std::string restores) mutable {
        json_properties_builder json;
        json.add("message", restores);
        rep->write_body("json", seastar::sstring(json.as_json()));
        return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
    });
}