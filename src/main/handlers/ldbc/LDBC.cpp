/*
 * Copyright Max De Marzi. All Rights Reserved.
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

#include "LDBC.h"
#include "../Utilities.h"
#include "../../json/JSON.h"
#include "types/Date.h"

void LDBC::set_routes(seastar::routes &routes) {
    auto query2 = new seastar::match_rule(&query2Handler);
    query2->add_str("/db/" + graph.GetName() + "/ldbc");
    query2->add_str("/q2");
    query2->add_param("personId");
    query2->add_param("maxDate");
    routes.add(query2, seastar::operation_type::GET);
}

class q2Comparator {
  public:
    bool operator()(const std::map<std::string, property_type_t> &p1, const std::map<std::string, property_type_t> &p2) {
        if (p1.at("message.creationDate") == p2.at("message.creationDate")) return p1.at("message.id") < p2.at("message.id");
        return p1.at("message.creationDate") > p2.at("message.creationDate");
    }
};

future<std::unique_ptr<seastar::httpd::reply>> LDBC::Query2Handler::handle([[maybe_unused]] const seastar::sstring &path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::httpd::reply> rep) {
    bool valid_personId = Utilities::validate_parameter(LDBC::PERSON_ID, req, rep, "Invalid personId");
    bool valid_maxDate = Utilities::validate_parameter(LDBC::MAX_DATE, req, rep, "Invalid maxDate");
    double maxDate = Utilities::convert_parameter_to_double(LDBC::MAX_DATE, req, rep);

    if(valid_personId && valid_maxDate && rep->_status != seastar::httpd::reply::status_type::bad_request) {
        return parent.graph.shard.local().NodeGetLinksPeered("Person", req->param[LDBC::PERSON_ID], "KNOWS")
          .then([req = std::move(req), rep = std::move(rep), maxDate, this] (const std::vector<Link> friend_links) mutable {
             return parent.graph.shard.local().NodesGetPropertiesPeered(friend_links)
                .then([req = std::move(req), rep = std::move(rep), friend_links, maxDate, this] (const std::map<Link, std::map<std::string, property_type_t>> friend_properties) mutable {
                return parent.graph.shard.local().LinksGetLinksPeered(friend_links, Direction::IN, "HAS_CREATOR")
                .then([req = std::move(req), rep = std::move(rep), friend_links, friend_properties, maxDate, this] (const std::map<Link, std::vector<Link>> messages) mutable {

                          std::vector<seastar::future<std::vector<std::map<std::string, property_type_t>>>> futures;
                          std::vector<std::map<std::string, property_type_t>> friend_properties_vector;
                          for (const auto& [friend_link, messages_links] : messages) {
                              std::map<std::string, property_type_t> props = friend_properties.at(friend_link);
                              friend_properties_vector.emplace_back(props);
                              std::vector<uint64_t> message_node_ids;
                              message_node_ids.reserve(friend_links.size());

                              for (const auto& link : messages_links) {
                                  message_node_ids.emplace_back(link.node_id);
                              }

                              auto future = parent.graph.shard.local().FilterNodePropertiesPeered(message_node_ids, "Message", "creationDate", Operation::LT, maxDate, 0, 10000000);
                              futures.push_back(std::move(future));
                          }
                          auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([p, messages, friend_properties_vector, rep = std::move(rep)] (const std::vector<std::vector<std::map<std::string, property_type_t>>> &vector_of_nodes) mutable {
                                   std::vector<std::map<std::string, property_type_t>> results;
                                   std::priority_queue<std::map<std::string, property_type_t>, std::vector<std::map<std::string, property_type_t>>, q2Comparator> pq;
                                   for (size_t i = 0; i < friend_properties_vector.size(); i++) {
                                       auto properties = friend_properties_vector.at(i);
                                       for (const auto& message_properties : vector_of_nodes[i] ) {
                                           std::map<std::string, property_type_t> result;
                                           if (pq.size() <= 20) {
                                               result.emplace("friend_position", static_cast<long>(i));
                                               result.emplace("message.id", message_properties.at("id"));
                                               result.emplace("message.creationDate", message_properties.at("creationDate"));
                                               if (message_properties.contains("content")) {
                                                   result.emplace("message.content", message_properties.at("content"));
                                               } else {
                                                   result.emplace("message.imageFile", message_properties.at("imageFile"));
                                               }
                                               pq.push(result);
                                           } else {
                                               result.emplace("message.id", message_properties.at("id"));
                                               result.emplace("message.creationDate", message_properties.at("creationDate"));
                                               if (pq.top() < result) {
                                                   result.emplace("friend_position", static_cast<long>(i));
                                                   if (message_properties.contains("content")) {
                                                       result.emplace("message.content", message_properties.at("content"));
                                                   } else {
                                                       result.emplace("message.imageFile", message_properties.at("imageFile"));
                                                   }
                                                   pq.pop();
                                                   pq.push(result);
                                               }
                                           }
                                       }
                                   }
                                    if (pq.size() > 20) {
                                        pq.pop();
                                    }
                                    results.reserve(pq.size());
                                    while (!pq.empty()) {
                                        results.push_back(pq.top());
                                        pq.pop();
                                    }
                                    std::vector<std::map<std::string, property_type_t>> smaller;
                                    smaller.reserve(results.size());
                                    //auto twenty_or_less = std::min(20UL, results.size());
                                    //smaller.reserve(twenty_or_less);
                                    //std::vector<std::map<std::string, property_type_t>> smaller = {results.begin(),  results.begin() + static_cast<long>(twenty_or_less)};

                                    std::reverse_copy(results.begin(), results.end(), std::back_inserter(smaller));
                                    // I don't need sort? Maybe

//                                   sort(results.begin(), results.end(), [&](const std::map<std::string, property_type_t> &k1, const std::map<std::string, property_type_t> &k2)-> bool {
//                                       if (k1.at("message.creationDate") > k2.at("message.creationDate")) {
//                                           return true;
//                                       }
//                                       if (k1.at("message.creationDate") == k2.at("message.creationDate")) {
//                                           return k1.at("message.id") < k2.at("message.id");
//                                       }
//                                       return false;
//                                   });

                                   // Take top 20 - TODO use a priority queue
                                   //auto twenty_or_less = std::min(20UL, results.size());
                                   //std::vector<std::map<std::string, property_type_t>> smaller = {results.begin(),  results.begin() + static_cast<long>(twenty_or_less)};

                                   // convert creation date, fill in friend properties
                                   for(auto& result : smaller) {
                                       std::string text_date = Date::toISO(get<double>(result.at("message.creationDate")));
                                       result["message.creationDate"] = text_date;
                                       size_t position = static_cast<size_t>(get<long>(result.at("friend_position")));
                                       result.erase("friend_position");
                                       result["friend.id"] = friend_properties_vector[position].at("id");
                                       result["friend.firstName"] = friend_properties_vector[position].at("firstName");
                                       result["friend.lastName"] = friend_properties_vector[position].at("lastName");
                                   }

                                   std::vector<seastar::sstring> json_array;
                                   json_array.reserve(results.size());
                                   for(auto& result : smaller) {
                                       json_properties_builder json;
                                       json.add_properties(result);
                                       json_array.emplace_back(seastar::sstring(json.as_json()));
                                   }

                                   rep->write_body("json", seastar::json::stream_object(json_array));
                                   return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
                           });

                       });
                      });
                });

          } else {
              rep->set_status(seastar::httpd::reply::status_type::bad_request);
    }

    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
}