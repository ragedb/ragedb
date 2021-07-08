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

#include <iostream>
#include "Graph.h"

namespace ragedb {

    /**
     * Get the name of this Graph
     *
     * @return name
     */
    std::string Graph::GetName() {
        return name;
    }

    /**
     * Start the Graph by creating a shard on each core
     *
     * @return future
     */
    seastar::future<> Graph::Start() {
        return shard.start(seastar::smp::count);
    }

    /**
     * Stop the Graph by stopping the shards
     *
     * @return future
     */
    seastar::future<> Graph::Stop() {
        return shard.stop();
    }

    /**
     * Empty the Graph of all data by clearing all the shards
     */
    void Graph::Clear() {
        seastar::future<> clear = shard.invoke_on_all([](Shard &local_shard) {
            return local_shard.Clear();
        });
        static_cast<void>(seastar::when_all_succeed(std::move(clear))
                .discard_result()
                .handle_exception([](const std::exception_ptr& e) {
                    std::cerr << "Exception in Graph::Clear\n";
                }));
    }

}