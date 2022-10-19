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
#include <seastar/core/when_all.hh>
#include "Graph.h"

namespace ragedb {

    /**
     * Get the name of this Graph
     *
     * @return name
     */
    std::string Graph::GetName() const {
        return name;
    }

    /**
     * Start the Graph by creating a shard on each core
     *
     * @return future
     */
    seastar::future<> Graph::Start() {
        StartLogging();
        return shard.start(seastar::smp::count);
    }

    void Graph::StartLogging() const {
        std::cout << "Logging disabled " << '\n';
        //std::cout << "Logging to " << log_path <<  '\n';
        //std::filesystem::path from{log_path};

        // If the log file exists, move it to restore
//        if (std::filesystem::exists(from)) {
//            std::filesystem::path to{GetName() + ".restore"};
//            copy_file(from, to, std::filesystem::copy_options::overwrite_existing);
//            std::filesystem::resize_file(from, 0);
//        }
        // TODO: crash handler expects to be called once, so we either uninstall and call for each or ???
        // reckless::install_crash_handler(&r_logger);
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
                              std::cerr << "Exception in Graph::Clear\n" << e << "\n";
                }));
    }

    constexpr char* format((char*)"%s&method=%s");
    constexpr char* with_body_format((char*)"%s&method=%s&body=%s");

    void Graph::Log(const seastar::sstring method, const seastar::sstring url) {
        //r_logger.write(format, std::string(url.c_str()), std::string(method.c_str()));
    }

    void Graph::Log(const seastar::sstring method, const seastar::sstring url, const seastar::sstring body) {
        //r_logger.write(with_body_format, std::string(url.c_str()), std::string(method.c_str()), base64::encode(body));
    }

}