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
#include "handlers/Utilities.h"
#include "Databases.h"
#include "handlers/Management.h"
#include <seastar/http/httpd.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/file_handler.hh>
#include <seastar/net/inet_address.hh>
#include <iostream>

namespace bpo = boost::program_options;

int main(int argc, char** argv) {
    seastar::app_template app;

    //Options
    app.add_options()("address", bpo::value<seastar::sstring>()->default_value("0.0.0.0"), "HTTP Server address");
    app.add_options()("port", bpo::value<uint16_t>()->default_value(7243), "HTTP Server port");

    try {
        app.run(argc, argv, [&app] {
            std::cout << "RageDB running on " << seastar::smp::count << " cores.\n";

            return seastar::async([&app] {
                seastar_apps_lib::stop_signal stop_signal;
                const auto &config = app.configuration();

                // Start Server
                seastar::net::inet_address addr(config["address"].as<seastar::sstring>());
                uint16_t port = config["port"].as<uint16_t>();

                seastar::httpd::http_server_control srv;
                auto server = &srv;
                server->start().get();
                Databases databases(server);
                databases.add("rage").get();

                // Initialize utilities to create a json parser for each core
                Utilities utilities;

                Management dbms(databases);
                server->set_routes([&dbms](seastar::httpd::routes &r) { dbms.set_routes(r); }).get();

                server->set_routes([](seastar::httpd::routes &r) {
                        r.add(seastar::httpd::operation_type::GET,
                          seastar::httpd::url("/hello"),
                          new seastar::httpd::function_handler([]([[maybe_unused]] seastar::httpd::const_req req) {
                            return "hello";
                          }));
                      })
                  .get();

                server->set_routes([](seastar::httpd::routes &r) {
                  r.add(seastar::httpd::operation_type::GET, seastar::httpd::url("").remainder("path"),
                    new seastar::httpd::directory_handler("./public/"));
                }).get();


                server->listen(seastar::socket_address{addr, port}).get();
                std::cout << "RageDB HTTP server listening on " << addr << ":" << port << " ...\n";

                seastar::engine().at_exit([&server, &databases] {
                    std::cout << "Stopping RageDB HTTP server" << std::endl;
                    return databases.stop().then([server](bool success) {
                      if (success) {
                        std::cout << "Stopped RageDB Management" << std::endl;
                      }
                      // TODO: This is in the right place, but the install can only be done once or
                      //  must be uninstalled and redone every time a graph is created
                      // reckless::uninstall_crash_handler();
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