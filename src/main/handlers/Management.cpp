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
#include "Utilities.h"
#include "../json/JSON.h"

void Management::set_routes(seastar::httpd::routes &routes) {
  auto getDatabases = new seastar::httpd::match_rule(&getDatabasesHandler);
  getDatabases->add_str("/dbs");
  routes.add(getDatabases, seastar::httpd::operation_type::GET);
  
  auto getDatabase = new seastar::httpd::match_rule(&getDatabaseHandler);
  getDatabase->add_str("/dbs");
  getDatabase->add_param("key");
  routes.add(getDatabase, seastar::httpd::operation_type::GET);
  
  auto postDatabase = new seastar::httpd::match_rule(&postDatabaseHandler);
  postDatabase->add_str("/dbs");
  postDatabase->add_param("key");
  routes.add(postDatabase, seastar::httpd::operation_type::POST);

  auto putDatabase = new seastar::httpd::match_rule(&putDatabaseHandler);
  putDatabase->add_str("/dbs");
  putDatabase->add_param("key");
  routes.add(putDatabase, seastar::httpd::operation_type::PUT);

  auto deleteDatabase = new seastar::httpd::match_rule(&deleteDatabaseHandler);
  deleteDatabase->add_str("/dbs");
  deleteDatabase->add_param("key");
  routes.add(deleteDatabase, seastar::httpd::operation_type::DELETE);
}

future<std::unique_ptr<seastar::http::reply>> Management::GetDatabasesHandler::handle([[maybe_unused]] const seastar::sstring &path, [[maybe_unused]] std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
  rep->write_body("json", seastar::json::stream_object(parent.databases.list()));
  return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> Management::GetDatabaseHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
  bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");
  if (valid_key && parent.databases.contains(req->param[Utilities::KEY])) {
    rep->write_body("json", seastar::sstring(parent.databases.get(req->param[Utilities::KEY])));
  }
  return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> Management::PostDatabaseHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
  bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");
  if (valid_key) {
    std::string key = req->param[Utilities::KEY];
    return parent.databases.add(key).then([key, req = std::move(req), rep = std::move(rep), this] (bool success) mutable {
      if (success) {
        rep->write_body("json", seastar::sstring(parent.databases.get(key)));
        parent.databases.at(key).graph.Log(req->_method, req->get_url());
      } else {
        rep->write_body("json", seastar::json::stream_object("Database already exists"));
        rep->set_status(seastar::http::reply::status_type::bad_request);
      }
      return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
    });
  }
  return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> Management::PutDatabaseHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
  bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

  if (valid_key) {
    std::string key = req->param[Utilities::KEY];
    return parent.databases.reset(key).then([key, req = std::move(req), rep = std::move(rep), this] (bool success) mutable {
      if (success) {
        rep->write_body("json", seastar::sstring(parent.databases.get(key)));
        parent.databases.at(key).graph.Log(req->_method, req->get_url());
      } else {
        rep->write_body("json", seastar::json::stream_object("Database does not exist"));
        rep->set_status(seastar::http::reply::status_type::bad_request);
      }
      return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
    });
  }
  return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}
future<std::unique_ptr<seastar::http::reply>> Management::DeleteDatabaseHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
  bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");

  if (valid_key) {
    std::string key = req->param[Utilities::KEY];
    return parent.databases.remove(key).then([key, req = std::move(req), rep = std::move(rep), this] (bool success) mutable {
      if (!success) {
        rep->set_status(seastar::http::reply::status_type::no_content);
        parent.databases.at(key).graph.Log(req->_method, req->get_url());
      } else {
        rep->write_body("json", seastar::json::stream_object("Database does not exist"));
        rep->set_status(seastar::http::reply::status_type::not_modified);
      }
      return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
    });
  }
  return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}
