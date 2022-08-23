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

#include "Lua.h"

const std::string EXCEPTION = "An exception has occurred: ";

void Lua::set_routes(seastar::routes &routes) {

    auto postLua = new seastar::match_rule(&postLuaHandler);
    postLua->add_str("/db/" + graph.GetName() + "/lua");
    routes.add(postLua, seastar::operation_type::POST);

    auto postLuaRW = new seastar::match_rule(&postLuaRWHandler);
    postLuaRW->add_str("/rw/" + graph.GetName() + "/lua");
    routes.add(postLuaRW, seastar::operation_type::POST);

    auto postLuaRO = new seastar::match_rule(&postLuaROHandler);
    postLuaRO->add_str("/ro/" + graph.GetName() + "/lua");
    routes.add(postLuaRO, seastar::operation_type::POST);
}

bool forAll(std::string const &str) {
    if (str.length() < 5) {
        return false;
    }
    return str.compare(0,5,"--ALL") == 0;
}

seastar::future<std::unique_ptr<seastar::httpd::reply>> Lua::PostLuaHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    // If the script is missing
    if (req->content.empty()) {
        rep->write_body("json", seastar::json::stream_object("Empty script"));
        rep->set_status(seastar::reply::status_type::bad_request);
    } else {
        parent.graph.Log(req->_method, req->get_url(), req->content);
        std::string body = req->content;
        if(forAll(body)) {
            body.append("\n\"Sent to All\"");

            return parent.graph.shard.map([body](ragedb::Shard &local_shard) {
                return local_shard.RunAdminLua(body);
            }).then([] (std::vector<std::string> results) {
                  return results[0];
            }).then([rep = std::move(rep)] (const std::string& result) mutable {
                  if(result.rfind(EXCEPTION,0) == 0) {
                      rep->write_body("html", seastar::sstring(result));
                      rep->set_status(seastar::reply::status_type::bad_request);
                      return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                  }

                  rep->write_body("json", seastar::sstring(result));
                  return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
              });
        }
        return parent.graph.shard.invoke_on(seastar::this_shard_id(), [body](ragedb::Shard &local_shard) {
            return local_shard.RunAdminLua(body);
        }).then([rep = std::move(rep)] (const std::string& result) mutable {
            if(result.rfind(EXCEPTION,0) == 0) {
                rep->write_body("html", seastar::sstring(result));
                rep->set_status(seastar::reply::status_type::bad_request);
                return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
            }

            rep->write_body("json", seastar::sstring(result));
            return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
        });
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}

seastar::future<std::unique_ptr<seastar::httpd::reply>> Lua::PostLuaRWHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
  // If the script is missing
  if (req->content.empty()) {
    rep->write_body("json", seastar::json::stream_object("Empty script"));
    rep->set_status(seastar::httpd::reply::status_type::bad_request);
  } else {
    parent.graph.Log(req->_method, req->get_url(), req->content);
    std::string body = req->content;
    return parent.graph.shard.invoke_on(seastar::this_shard_id(), [body](ragedb::Shard &local_shard) {
                               return local_shard.RunRWLua(body);
                             }).then([rep = std::move(rep)] (const std::string& result) mutable {
        if(result.rfind(EXCEPTION,0) == 0) {
          rep->write_body("html", seastar::sstring(result));
          rep->set_status(seastar::httpd::reply::status_type::bad_request);
          return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
        }

        rep->write_body("json", seastar::sstring(result));
        return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
      });
  }

  return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}

seastar::future<std::unique_ptr<seastar::httpd::reply>> Lua::PostLuaROHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
  // If the script is missing
  if (req->content.empty()) {
    rep->write_body("json", seastar::json::stream_object("Empty script"));
    rep->set_status(seastar::httpd::reply::status_type::bad_request);
  } else {
    parent.graph.Log(req->_method, req->get_url(), req->content);
    std::string body = req->content;
    return parent.graph.shard.invoke_on(seastar::this_shard_id(), [body](ragedb::Shard &local_shard) {
                               return local_shard.RunROLua(body);
                             }).then([rep = std::move(rep)] (const std::string& result) mutable {
        if(result.rfind(EXCEPTION,0) == 0) {
          rep->write_body("html", seastar::sstring(result));
          rep->set_status(seastar::httpd::reply::status_type::bad_request);
          return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
        }

        rep->write_body("json", seastar::sstring(result));
        return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
      });
  }

  return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}