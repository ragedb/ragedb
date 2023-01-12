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

#include "Utilities.h"
#include "../json/JSON.h"
#include "Connected.h"

void Connected::set_routes(seastar::routes &routes) {
  auto getConnected = new seastar::match_rule(&getConnectedHandler);
  getConnected->add_str("/db/" + graph.GetName() + "/node");
  getConnected->add_param("type");
  getConnected->add_param("key");
  getConnected->add_str("/connected");
  getConnected->add_param("type2");
  getConnected->add_param("key2");
  getConnected->add_param("options", true);
  routes.add(getConnected, seastar::operation_type::GET);

  auto getConnectedById = new seastar::match_rule(&getConnectedByIdHandler);
  getConnectedById->add_str("/db/" + graph.GetName() + "/node");
  getConnectedById->add_param("id");
  getConnectedById->add_str("/connected");
  getConnectedById->add_param("id2");
  getConnectedById->add_param("options", true);
  routes.add(getConnectedById, seastar::operation_type::GET);
}

future<std::unique_ptr<seastar::http::reply>> Connected::GetConnectedHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
  bool valid_type = Utilities::validate_parameter(Utilities::TYPE, req, rep, "Invalid type");
  bool valid_key = Utilities::validate_parameter(Utilities::KEY, req, rep, "Invalid key");
  bool valid_type2 = Utilities::validate_parameter(Utilities::TYPE2, req, rep, "Invalid type 2");
  bool valid_key2 = Utilities::validate_parameter(Utilities::KEY2, req, rep, "Invalid key 2");

  if(valid_type && valid_key && valid_type2 && valid_key2) {

    // Gather Options
    std::string options_string;
    Direction direction = Direction::BOTH;
    options_string = req->param.at(Utilities::OPTIONS).c_str();

    if(options_string.empty()) {
      // Get Connected
      return parent.graph.shard.local().NodeGetConnectedPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], req->param[Utilities::TYPE2], req->param[Utilities::KEY2])
        .then([rep = std::move(rep)] (const std::vector<Relationship>& relationships) mutable {
          std::vector<relationship_json> json_array;
          json_array.reserve(relationships.size());
          for(Relationship r : relationships) {
            json_array.emplace_back(r);
          }
          rep->write_body("json", seastar::json::stream_object(json_array));
          return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }

    std::vector<std::string> options;
    boost::split(options, options_string, [](char c){return c == '/';});

    // Erase empty first element from leading slash
    options.erase(options.begin());

    // Parse Direction
    boost::algorithm::to_lower(options[0]);
    if (options[0] == "in") {
      direction = Direction::IN;
    } else if (options[0] == "out") {
      direction = Direction::OUT;
    }

    switch(options.size()) {
    case 1:
      // Get Node Degree with Direction
      return parent.graph.shard.local().NodeGetConnectedPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], req->param[Utilities::TYPE2], req->param[Utilities::KEY2], direction)
        .then([rep = std::move(rep)] (const std::vector<Relationship>& relationships) mutable {
          std::vector<relationship_json> json_array;
          json_array.reserve(relationships.size());
          for(Relationship r : relationships) {
            json_array.emplace_back(r);
          }
          rep->write_body("json", seastar::json::stream_object(json_array));
          return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    case 2: {
      // Get Node Degree with Direction and Type(s)
      std::vector<std::string> rel_types;
      // Deal with both escaped and unescaped "&"
      boost::split(rel_types, options[1], boost::is_any_of("&,%26"), boost::token_compress_on);
      // Single Relationship Type
      if (rel_types.size() == 1) {
        return parent.graph.shard.local().NodeGetConnectedPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], req->param[Utilities::TYPE2], req->param[Utilities::KEY2], direction, rel_types[0])
          .then([rep = std::move(rep), rel_type = rel_types[0]] (const std::vector<Relationship>& relationships) mutable {
            std::vector<relationship_json> json_array;
            json_array.reserve(relationships.size());
            for(Relationship r : relationships) {
              json_array.emplace_back(r);
            }
            rep->write_body("json", seastar::json::stream_object(json_array));
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
          });
      }

      // Multiple Relationship Types
      return parent.graph.shard.local().NodeGetConnectedPeered(req->param[Utilities::TYPE], req->param[Utilities::KEY], req->param[Utilities::TYPE2], req->param[Utilities::KEY2], direction, rel_types)
        .then([rep = std::move(rep)] (const std::vector<Relationship>& relationships) mutable {
          std::vector<relationship_json> json_array;
          json_array.reserve(relationships.size());
          for(Relationship r : relationships) {
            json_array.emplace_back(r);
          }
          rep->write_body("json", seastar::json::stream_object(json_array));
          return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }

    default:  {
      rep->write_body("json", seastar::json::stream_object("Invalid request"));
      rep->set_status(seastar::http::reply::status_type::bad_request);
      return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
    }
    }
  }

  return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

future<std::unique_ptr<seastar::http::reply>> Connected::GetConnectedByIdHandler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
  uint64_t id = Utilities::validate_id(req, rep);
  uint64_t id2 = Utilities::validate_id2(req, rep);

  if (id > 0 && id2 > 0) {
    // Gather Options
    std::string options_string;
    Direction direction = Direction::BOTH;
    options_string = req->param.at(Utilities::OPTIONS).c_str();

    if(options_string.empty()) {
      // Get Node Relationships
      return parent.graph.shard.local().NodeGetConnectedPeered(id, id2)
        .then([rep = std::move(rep)] (const std::vector<Relationship>& relationships) mutable {
          std::vector<relationship_json> json_array;
          json_array.reserve(relationships.size());
          for(Relationship r : relationships) {
            json_array.emplace_back(r);
          }
          rep->write_body("json", seastar::json::stream_object(json_array));
          return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }

    std::vector<std::string> options;
    boost::split(options, options_string, [](char c){return c == '/';});

    // Erase empty first element from leading slash
    options.erase(options.begin());

    // Parse Direction
    boost::algorithm::to_lower(options[0]);
    if (options[0] == "in") {
      direction = Direction::IN;
    } else if (options[0] == "out") {
      direction = Direction::OUT;
    }

    switch(options.size()) {
    case 1:
      // Get Node Relationships with Direction
      return parent.graph.shard.local().NodeGetConnectedPeered(id, id2, direction)
        .then([rep = std::move(rep)] (const std::vector<Relationship>& relationships) mutable {
          std::vector<relationship_json> json_array;
          json_array.reserve(relationships.size());
          for(Relationship r : relationships) {
            json_array.emplace_back(r);
          }
          rep->write_body("json", seastar::json::stream_object(json_array));
          return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    case 2: {
      // Get Node Degree with Direction and Type(s)
      std::vector<std::string> rel_types;
      boost::split(rel_types, options[1], boost::is_any_of("&,%26"), boost::token_compress_on);
      // Single Relationship Type
      if (rel_types.size() == 1) {
        return parent.graph.shard.local().NodeGetConnectedPeered(id, id2, direction, rel_types[0])
          .then([rep = std::move(rep), rel_type = rel_types[0]] (const std::vector<Relationship>& relationships) mutable {
            std::vector<relationship_json> json_array;
            json_array.reserve(relationships.size());
            for(Relationship r : relationships) {
              json_array.emplace_back(r);
            }
            rep->write_body("json", seastar::json::stream_object(json_array));
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
          });
      }

      // Multiple Relationship Types
      return parent.graph.shard.local().NodeGetConnectedPeered(id, id2, direction, rel_types)
        .then([rep = std::move(rep)] (const std::vector<Relationship>& relationships) mutable {
          std::vector<relationship_json> json_array;
          json_array.reserve(relationships.size());
          for(Relationship r : relationships) {
            json_array.emplace_back(r);
          }
          rep->write_body("json", seastar::json::stream_object(json_array));
          return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }

    default:  {
      rep->write_body("json", seastar::json::stream_object("Invalid request"));
      rep->set_status(seastar::http::reply::status_type::bad_request);
      return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
    }
    }
  }

  return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
}

Connected::GetConnectedHandler::GetConnectedHandler(Connected& connected) : parent(connected) {}
Connected::GetConnectedByIdHandler::GetConnectedByIdHandler(Connected& connected) : parent(connected) {}
Connected::Connected (ragedb::Graph &_graph) : graph(_graph), getConnectedHandler(*this), getConnectedByIdHandler(*this) {}

//Connected::GetConnectedHandler::~GetConnectedHandler() {}
