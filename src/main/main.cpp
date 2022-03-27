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

#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include "util/stop_signal.hh"
#include "handlers/HealthCheck.h"
#include "handlers/Nodes.h"
#include "handlers/Relationships.h"
#include "handlers/NodeProperties.h"
#include "handlers/RelationshipProperties.h"
#include "handlers/Degrees.h"
#include "handlers/Neighbors.h"
#include "handlers/Schema.h"
#include "handlers/Utilities.h"
#include "handlers/Lua.h"
#include "handlers/Restore.h"
#include "handlers/Connected.h"
#include "Database.h"
#include <seastar/http/httpd.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/file_handler.hh>
#include <seastar/net/inet_address.hh>
#include <iostream>
#include <Graph.h>

namespace bpo = boost::program_options;

int main(int argc, char** argv) {
    seastar::app_template app;

    //Options
    app.add_options()("address", bpo::value<seastar::sstring>()->default_value("0.0.0.0"), "HTTP Server address");
    app.add_options()("port", bpo::value<uint16_t>()->default_value(7243), "HTTP Server port");

    try {
        app.run(argc, argv, [&] {
            std::cout << "Hello world!\n";
            std::cout << "This server has " << seastar::smp::count << " cores.\n";

            return seastar::async([&] {
                seastar_apps_lib::stop_signal stop_signal;
                auto &&config = app.configuration();

                // Start Server
                seastar::net::inet_address addr(config["address"].as<seastar::sstring>());
                uint16_t port = config["port"].as<uint16_t>();
                auto server = new seastar::http_server_control();
                server->start().get();

                // Initialize utilities to create a json parser for each core
                Utilities utilities;

                std::map<std::string, Database> databases;
                databases.emplace("rage", "rage");

                for (auto& [name, graph]: databases) {

                  server->set_routes([graph = &graph](routes &r) { graph->relationshipProperties.set_routes(r); }).get();
                  server->set_routes([graph =&graph](routes &r) { graph->nodeProperties.set_routes(r); }).get();
                  server->set_routes([graph =&graph](routes &r) { graph->degrees.set_routes(r); }).get();
                  server->set_routes([graph =&graph](routes &r) { graph->neighbors.set_routes(r); }).get();
                  server->set_routes([graph =&graph](routes &r) { graph->connected.set_routes(r); }).get();
                  server->set_routes([graph =&graph](routes &r) { graph->relationships.set_routes(r); }).get();
                  server->set_routes([graph =&graph](routes &r) { graph->nodes.set_routes(r); }).get();
                  server->set_routes([graph =&graph](routes &r) { graph->schema.set_routes(r); }).get();
                  server->set_routes([graph =&graph](routes &r) { graph->lua.set_routes(r); }).get();
                  server->set_routes([graph =&graph](routes &r) { graph->healthCheck.set_routes(r); }).get();
                  server->set_routes([graph =&graph](routes &r) { graph->restore.set_routes(r); }).get();
                }

                server->set_routes([](seastar::routes &r) {
                        r.add(seastar::operation_type::GET,
                          seastar::url("/hello"),
                          new seastar::function_handler([]([[maybe_unused]] seastar::const_req req) {
                            return "hello";
                          }));
                      })
                  .get();

                server->set_routes([](seastar::routes &r) {
                  r.add(operation_type::GET, url("").remainder("path"),
                    new directory_handler("./public/"));
                }).get();


                server->listen(seastar::socket_address{addr, port}).get();
                std::cout << "RageDB HTTP server listening on " << addr << ":" << port << " ...\n";
                //graph.Restore().get();
                seastar::engine().at_exit([&server, &databases] {
                    std::cout << "Stopping RageDB HTTP server" << std::endl;
                    std::vector<seastar::future<>> futures;
                    for (auto& [name, database]: databases) {
                      futures.push_back(database.graph.Stop());
                    }
                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end()).then([p, server] () {
                      return server->stop();
                    });

                });

                stop_signal.wait().get();  // this will wait till we receive SIGINT or SIGTERM signal
            });
        });
    } catch (...) {
        std::cerr << "Failed to start RageDB: "
                  << std::current_exception() << "\n";
        return 1;
    }
    return 0;
}