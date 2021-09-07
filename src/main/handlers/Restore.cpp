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

void Restore::set_routes(routes &routes) {
    auto restore = new match_rule(&restoreHandler);
    restore->add_str("/db/" + graph.GetName() + "/restore");
    routes.add(restore, operation_type::POST);
}

future<std::unique_ptr<reply>> Restore::RestoreHandler::handle(
        [[maybe_unused]] const sstring &path,
        [[maybe_unused]] std::unique_ptr<request> req,
        std::unique_ptr<reply> rep) {

    auto restores = co_await parent.graph.shard.local().RestorePeered(parent.graph.GetName());
    json_properties_builder json;
    json.add("message", restores);
    rep->write_body("json", sstring(json.as_json()));
    co_return rep;
}