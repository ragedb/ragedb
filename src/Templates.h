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

#ifndef RAGEDB_TEMPLATES_H
#define RAGEDB_TEMPLATES_H

#include <vector>

template<typename T>
std::vector<std::vector<T>> chunkVector(const std::vector<T>& source, size_t chunkSize)
{
    std::vector<std::vector<T>> result;
    result.reserve((source.size() + chunkSize - 1) / chunkSize);

    auto start = source.begin();
    auto end = source.end();

    while (start != end) {
        auto next = std::distance(start, end) >= chunkSize
                      ? start + chunkSize
                      : end;

        result.emplace_back(start, next);
        start = next;
    }

    return result;
}

#endif// RAGEDB_TEMPLATES_H
