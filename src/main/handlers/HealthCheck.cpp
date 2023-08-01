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

void HealthCheck::set_routes(seastar::httpd::routes &routes) {
    auto healthCheck = new seastar::httpd::match_rule(&healthCheckHandler);
    healthCheck->add_str("/db/" + graph.GetName() + "/health_check");
    routes.add(healthCheck, seastar::httpd::operation_type::GET);
}

seastar::future<std::unique_ptr<seastar::http::reply>> HealthCheck::HealthCheckHandler::handle(
  [[maybe_unused]] const seastar::sstring &path,
  [[maybe_unused]] std::unique_ptr<seastar::http::request> req,
  std::unique_ptr<seastar::http::reply> rep) {

    return parent.graph.shard.local().HealthCheckPeered()
      .then([rep = std::move(rep)] (const std::vector<std::string>& checks) mutable {
          rep->write_body("json", seastar::json::stream_object(checks));
          return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
      });
}
