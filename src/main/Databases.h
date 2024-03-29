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

#ifndef RAGEDB_DATABASES_H
#define RAGEDB_DATABASES_H


#include <string>
#include "Database.h"
class Databases {
  std::map<std::string, Database> databases;
  seastar::httpd::http_server_control* server;

public:
  explicit Databases(seastar::httpd::http_server_control*& _server) : server(_server) {}
  std::vector<std::string> list() const;
  std::string get(std::string const &key);
  bool contains(std::string const &key) const;
  seastar::future<bool> add(std::string key);
  Database &at(std::string const &key);
  seastar::future<bool> reset(std::string const &key);
  seastar::future<bool> remove(std::string const &key);
  seastar::future<bool> stop();
};


#endif// RAGEDB_DATABASES_H
