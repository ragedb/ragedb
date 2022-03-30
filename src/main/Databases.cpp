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

#include "Databases.h"
#include "json/JSON.h"

std::vector<std::string> Databases::list() {
  std::vector<std::string> names;
  names.reserve(databases.size());
  for (auto&& [name, database] : databases) {
    names.push_back(name);
  }
  return names;
}

std::string Databases::get(std::string key) {
  std::set<std::string> nodes = databases.at(key).graph.shard.local().NodeTypesGet();
  std::vector<std::string> node_types;
  node_types.assign(nodes.begin(), nodes.end());

  std::set<std::string> rels = databases.at(key).graph.shard.local().RelationshipTypesGet();
  std::vector<std::string> rel_types;
  rel_types.assign(rels.begin(), rels.end());

  database_json database_json(key, node_types, rel_types);
  return database_json.to_json();
}

Database &Databases::at(std::string key) {
  return databases.at(key);
}

bool Databases::contains(std::string key) {
  return databases.contains(key);
}

seastar::future<bool> Databases::add(std::string key) {
  if (databases.contains(key)) {
    return seastar::make_ready_future<bool>(false);
  }

  databases.emplace(key, key);
  auto &database = databases.at(key);

  std::vector<seastar::future<>> futures;

  futures.emplace_back(server->set_routes([&database](routes &r) { database.relationshipProperties.set_routes(r); }));
  futures.emplace_back(server->set_routes([&database](routes &r) { database.nodeProperties.set_routes(r); }));
  futures.emplace_back(server->set_routes([&database](routes &r) { database.degrees.set_routes(r); }));
  futures.emplace_back(server->set_routes([&database](routes &r) { database.neighbors.set_routes(r); }));
  futures.emplace_back(server->set_routes([&database](routes &r) { database.connected.set_routes(r); }));
  futures.emplace_back(server->set_routes([&database](routes &r) { database.relationships.set_routes(r); }));
  futures.emplace_back(server->set_routes([&database](routes &r) { database.nodes.set_routes(r); }));
  futures.emplace_back(server->set_routes([&database](routes &r) { database.schema.set_routes(r); }));
  futures.emplace_back(server->set_routes([&database](routes &r) { database.lua.set_routes(r); }));
  futures.emplace_back(server->set_routes([&database](routes &r) { database.healthCheck.set_routes(r); }));
  futures.emplace_back(server->set_routes([&database](routes &r) { database.restore.set_routes(r); }));
  futures.emplace_back(databases.at(key).start());

  auto p = make_shared(std::move(futures));
  return seastar::when_all_succeed(p->begin(), p->end()).then([p] () {
    return seastar::make_ready_future<bool>(true);
  });
}

seastar::future<bool> Databases::reset(std::string key) {
  if (!databases.contains(key)) {
    return seastar::make_ready_future<bool>(false);
  }
  databases.at(key).graph.Clear();
  return seastar::make_ready_future<bool>(true);
}

seastar::future<bool> Databases::remove(std::string key) {
  if (!databases.contains(key)) {
    return seastar::make_ready_future<bool>(false);
  }
  databases.at(key).graph.Clear();
  return databases.at(key).graph.Stop().then([key, this] {
    databases.erase(key);
    return seastar::make_ready_future<bool>(true);
  });
}

seastar::future<bool> Databases::stop() {
  std::vector<seastar::future<>> futures;
  for (auto& [name, database]: databases) {
    futures.push_back(database.graph.Stop());
  }
  auto p = make_shared(std::move(futures));
  return seastar::when_all_succeed(p->begin(), p->end()).then([] () {
    return seastar::make_ready_future<bool>(true);
  });
}
