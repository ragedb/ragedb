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

void Restore::set_routes(seastar::routes &routes) {
    auto restore = new seastar::match_rule(&restoreHandler);
    restore->add_str("/db/" + graph.GetName() + "/restore");
    routes.add(restore, seastar::operation_type::POST);
}

future<std::unique_ptr<seastar::httpd::reply>> Restore::RestoreHandler::handle(
        [[maybe_unused]] const seastar::sstring &path,
        [[maybe_unused]] std::unique_ptr<seastar::request> req,
        std::unique_ptr<seastar::httpd::reply> rep) {

    auto restores = co_await parent.graph.shard.local().RestorePeered(parent.graph.GetName());
    json_properties_builder json;
    json.add("message", restores);
    rep->write_body("json", seastar::sstring(json.as_json()));
    co_return rep;
}