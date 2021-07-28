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

void Lua::set_routes(routes &routes) {

    auto postLua = new match_rule(&postLuaHandler);
    postLua->add_str("/db/" + graph.GetName() + "/lua");
    routes.add(postLua, operation_type::POST);

}

future<std::unique_ptr<reply>> Lua::PostLuaHandler::handle([[maybe_unused]] const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    // If the script is missing
    if (req->content.empty()) {
        rep->write_body("json", json::stream_object("Empty script"));
        rep->set_status(reply::status_type::bad_request);
    } else {
        std::string body = req->content;

        return parent.graph.shard.invoke_on(this_shard_id(), [body](Shard &local_shard) {
            return local_shard.RunLua(body);
        }).then([rep = std::move(rep)] (const std::string& result) mutable {
            if(result.rfind(EXCEPTION,0) == 0) {
                rep->write_body("json", sstring(result));
                rep->set_status(reply::status_type::bad_request);
                return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            }

            rep->write_body("json", sstring(result));
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        });
    }

    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}