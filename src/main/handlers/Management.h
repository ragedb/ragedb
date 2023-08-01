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

#ifndef RAGEDB_MANAGEMENT_H
#define RAGEDB_MANAGEMENT_H


#include <string>
#include "../Database.h"
#include "../Databases.h"

class Management {

  class GetDatabasesHandler : public seastar::httpd::handler_base {
  public:
    explicit GetDatabasesHandler(Management & management) : parent(management) {};
  private:
    Management & parent;
    seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
  };

  class GetDatabaseHandler : public seastar::httpd::handler_base {
  public:
    explicit GetDatabaseHandler(Management & management) : parent(management) {};
  private:
    Management & parent;
    seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
  };

  class PostDatabaseHandler : public seastar::httpd::handler_base {
  public:
    explicit PostDatabaseHandler(Management & management) : parent(management) {};
  private:
    Management & parent;
    seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
  };

  class PutDatabaseHandler : public seastar::httpd::handler_base {
  public:
    explicit PutDatabaseHandler(Management & management) : parent(management) {};
  private:
    Management & parent;
    seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
  };

  class DeleteDatabaseHandler : public seastar::httpd::handler_base {
  public:
    explicit DeleteDatabaseHandler(Management & management) : parent(management) {};
  private:
    Management & parent;
    seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override;
  };

  Databases &databases;
  GetDatabasesHandler getDatabasesHandler;
  GetDatabaseHandler getDatabaseHandler;
  PostDatabaseHandler postDatabaseHandler;
  PutDatabaseHandler putDatabaseHandler;
  DeleteDatabaseHandler deleteDatabaseHandler;

public:
  Management(Databases &_databases) : databases(_databases), getDatabasesHandler(*this), getDatabaseHandler(*this),
                                      postDatabaseHandler(*this), putDatabaseHandler(*this), deleteDatabaseHandler(*this) {}
  void set_routes(seastar::httpd::routes& routes);
};


#endif// RAGEDB_MANAGEMENT_H
