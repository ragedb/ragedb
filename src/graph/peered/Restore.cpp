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

#include "../Shard.h"
#include <thread>

namespace ragedb {

    std::map<std::string, std::string, std::less<>> parseURL(std::string url) {
        std::map<std::string, std::string, std::less<>> result;
        size_t start;
        size_t pos;
        const std::string http_delimiter = "http";
        const std::string method_delimiter = "&method=";
        const std::string body_delimiter = "&body=";

        start = url.find(http_delimiter);

        pos = url.find(method_delimiter);
        std::string address = url.substr(start, pos - start);
        result.insert({"address", address});
        url.erase(0, pos + method_delimiter.length());

        pos = url.find(body_delimiter);
        if (pos == std::string::npos) {
            // There is no body
            result.emplace("method", url);
        } else {
            result.emplace("method", url.substr(0, pos));
            result.emplace("body", url.substr(pos + body_delimiter.length()));
        }
        return result;
    }

    void Restore(const std::string& name) {
        auto start = std::chrono::high_resolution_clock::now();

        uint64_t count = 0;
        std::fstream restore_file;
        restore_file.open(name + ".restore", std::ios::in);
        if (restore_file.is_open()) {
            if (restore_file.peek() != std::ifstream::traits_type::eof()) {

                // If this is too slow
                // Alternative log an id for the URL type
                // in a Case statement execute the actual peered method
                // in a thread, wait for it via .get()

                cpr::Session session;
                session.SetTimeout(cpr::Timeout{1000});

                std::string line;
                while (std::getline(restore_file, line)) {
                    try {
                        std::map<std::string, std::string, std::less<>> parsed = parseURL(line);
                        session.SetUrl(cpr::Url{parsed["address"]});

                        auto body_search = parsed.find("body");
                        if (body_search != std::end(parsed)) {
                             session.SetBody(cpr::Body{base64::decode<std::string>(body_search->second)});
                        } else {
                            session.SetBody(cpr::Body{});
                        }

                        cpr::Response r;

                        if (parsed["method"] == "POST")
                            r = session.Post();
                        else if (parsed["method"] == "PUT")
                            r = session.Put();
                        else if (parsed["method"] == "DELETE")
                            r = session.Delete();

                        if (r.status_code >= 300) {
                            std::cout << "HTTP Error: (" << r.status_code << ") on line " << count << " : " << line << std::endl;
                        }

                        count++;
                    } catch (const std::exception &e) {
                        std::cout << "Exception on line "  << count << " : "  << line <<  " message: " << e.what() << std::endl;
                    }
                }

            }
            restore_file.close(); //close the file object.
        }
    }

    seastar::future<std::string> Shard::RestorePeered(const std::string& name) {
        std::thread t(Restore, name);
        t.detach();

        std::string message = "Restoring " + name;
        return seastar::make_ready_future<std::string>(message);
    }

}