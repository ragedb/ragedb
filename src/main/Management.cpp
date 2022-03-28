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

#include "Management.h"
#include "json/JSON.h"

std::vector<std::string> Management::list() {
  std::vector<std::string> names;
  names.reserve(databases.size());
  for (auto&& [name, database] : databases) {
    names.push_back(name);
  }
  return names;
}

std::string Management::get(std::string key) {
  std::map<std::string, std::map<std::string, std::string>> nodes;
  std::map<std::string, std::map<std::string, std::string>> rels;

  for(auto type : databases.at(key).graph.shard.local().NodeTypesGet() ) {
    nodes.emplace(type, databases.at(key).graph.shard.local().NodeTypeGet(type));
  }
  for(auto type : databases.at(key).graph.shard.local().RelationshipTypesGet() ) {
    rels.emplace(type, databases.at(key).graph.shard.local().RelationshipTypeGet(type));
  }
//  database_json database_json(key, nodes, rels);
//  return database_json.to_json();
  return "tmp";
}

Database &Management::at(std::string key) {
  return databases.at(key);
}

bool Management::contains(std::string key) {
  return databases.contains(key);
}

seastar::future<bool> Management::add(std::string key) {
  if (databases.contains(key)) {
    return seastar::make_ready_future<bool>(false);
  }
  databases.emplace(key, key);
  auto & graph = databases.at(key);
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

  return databases.at(key).start().then([] {
    return seastar::make_ready_future<bool>(true);
  });

}

seastar::future<bool> Management::reset(std::string key) {
  if (!databases.contains(key)) {
    return seastar::make_ready_future<bool>(false);
  }
  databases.at(key).graph.Clear();
  return seastar::make_ready_future<bool>(true);
}

seastar::future<bool> Management::remove(std::string key) {
  if (!databases.contains(key)) {
    return seastar::make_ready_future<bool>(false);
  }
  databases.at(key).graph.Clear();
  return databases.at(key).graph.Stop().then([key, this] {
    databases.erase(key);
    return seastar::make_ready_future<bool>(true);
  });
}

seastar::future<bool> Management::stop() {

  std::vector<seastar::future<>> futures;
  for (auto& [name, database]: databases) {
    futures.push_back(database.graph.Stop());
  }
  auto p = make_shared(std::move(futures));
  return seastar::when_all_succeed(p->begin(), p->end()).then([] () {
    return seastar::make_ready_future<bool>(true);
  });
}
