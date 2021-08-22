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

#include "HealthCheck.h"

void HealthCheck::set_routes(routes &routes) {
    auto healthCheck = new match_rule(&healthCheckHandler);
    healthCheck->add_str("/db/" + graph.GetName() + "/health_check");
    routes.add(healthCheck, operation_type::GET);
}

future<std::unique_ptr<reply>> HealthCheck::HealthCheckHandler::handle(
        [[maybe_unused]] const sstring &path,
        [[maybe_unused]] std::unique_ptr<request> req,
        std::unique_ptr<reply> rep) {

    auto checks = co_await parent.graph.shard.local().HealthCheckPeered();
    rep->write_body("json", json::stream_object(checks));
    co_return rep;
}
