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

#ifndef RAGEDB_SHARD_H
#define RAGEDB_SHARD_H

#include <seastar/core/sharded.hh>

namespace ragedb {

    class Shard : public seastar::peering_sharded_service<Shard> {

    private:
        uint cpus;
        uint shard_id;

    public:
        explicit Shard(uint _cpus) : cpus(_cpus), shard_id(seastar::this_shard_id()) {
            std::stringstream ss;
            ss << "Starting Shard " << shard_id << '\n';
            std::cout << ss.str();
        }

        static seastar::future<> stop();
        void Clear();


        // *****************************************************************************************************************************
        //                                               Single Shard
        // *****************************************************************************************************************************

        static seastar::future<std::string> HealthCheck();

        // *****************************************************************************************************************************
        //                                               Peered
        // *****************************************************************************************************************************

        seastar::future<std::vector<std::string>> HealthCheckPeered();


    };
}


#endif //RAGEDB_SHARD_H
