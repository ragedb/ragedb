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


#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/routes.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tcp.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/seastar_test.hh>

//#include <boost/test/included/unit_test.hpp>

#include <exception>
#include <seastar/net/inet_address.hh>
#include "../../src/main/handlers/Connected.h"
#include <Graph.h>

using namespace std::chrono_literals;

//SEASTAR_TEST_CASE(handler_connected_set_routes) {
//  return seastar::async([] {
//    ragedb::Graph graph = ragedb::Graph("test");
//    Connected connected(graph);
//    auto server = new seastar::http_server_control();
//    server->set_routes([&connected](routes &r) { connected.set_routes(r); }).get();
//    server->stop().get();
//  });
//}

SEASTAR_TEST_CASE(handler_connected_handle) {
  return seastar::async([] {
    ragedb::Graph graph = ragedb::Graph("test");
    Connected* connected = new Connected(graph);
    seastar::sstring address = "0.0.0.0";
    uint16_t port = 7243;
    seastar::net::inet_address addr(address);
    auto server = new seastar::httpd::http_server_control();
    server->start().get();
    server->set_routes([&connected](seastar::httpd::routes &r) { connected->set_routes(r); }).get();
    server->listen(seastar::socket_address{addr, port}).get();
//delete connected;
    graph.Stop().get();
    server->stop().get();
    delete server;
  });
}


