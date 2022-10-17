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

#ifndef RAGEDB_LDBC_H
#define RAGEDB_LDBC_H


#include <Graph.h>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>

class LDBC {

    class Query2Handler : public seastar::httpd::handler_base {
      public:
        explicit Query2Handler(LDBC& degrees) : parent(degrees) {};
      private:
        LDBC& parent;
        seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(const seastar::sstring& path, std::unique_ptr<seastar::request> req, std::unique_ptr<seastar::reply> rep) override;
    };

    ragedb::Graph& graph;
    Query2Handler query2Handler;

  public:
    explicit LDBC (ragedb::Graph &_graph) : graph(_graph), query2Handler(*this) {}
    void set_routes(seastar::routes& routes);

    static inline const seastar::sstring PERSON_ID = seastar::sstring ("personId");
    static inline const seastar::sstring MAX_DATE = seastar::sstring ("maxDate");
};


#endif// RAGEDB_LDBC_H
