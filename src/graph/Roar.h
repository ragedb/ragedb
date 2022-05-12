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

#ifndef RAGEDB_ROAR_H
#define RAGEDB_ROAR_H

#include <roaring/roaring64map.hh>
#include <vector>
#include <cstdint>
#include <sol/types.hpp>
#include "Link.h"

namespace ragedb {
  class Roar {

    private:
    roaring::Roaring64Map map = roaring::Roaring64Map();

  public:
    Roar();
    Roar(const Roar &roar) = default;
    Roar (Roar && roar) noexcept = default;
    void addIds(std::vector<uint64_t> ids);
    void addNodeIds(std::vector<Link> links);
    void addRelationshipIds(std::vector<Link> links);
    std::vector<uint64_t> getIds() const;
    sol::as_table_t<std::vector<uint64_t>> getIdsLua();

    std::vector<Link> getNodeHalfLinks() const;
    sol::as_table_t<std::vector<Link>> getNodeHalfLinksLua();

    std::vector<Link> getRelationshipHalfLinks() const;
    sol::as_table_t<std::vector<Link>> getRelationshipHalfLinksLua();
    void add(uint64_t x);
    void remove(uint64_t x);
    bool addChecked(uint64_t x);
    bool removeChecked(uint64_t x);
    void clear();
    uint64_t maximum() const;
    uint64_t minimum() const;
    bool contains(uint64_t x) const;
    Roar &operator&=(const Roar &r);
    Roar &operator-=(const Roar &r);
    Roar &operator|=(const Roar &r);
    Roar &operator^=(const Roar &r);
    Roar swap(Roar &r) noexcept;
    uint64_t cardinality() const;
    bool isEmpty() const;
    bool isFull() const;
    bool isSubset(const Roar &r) const;
    bool isStrictSubset(const Roar &r) const;
    bool operator==(const Roar &r) const = default;
    void flip(uint64_t range_start, uint64_t range_end);
    bool removeRunCompression();
    bool runOptimize();
    size_t shrinkToFit();
    uint64_t rank(uint64_t x) const;
    Roar operator&(const Roar &r);
    Roar operator-(const Roar &r);
    Roar operator|(const Roar &r);
    Roar operator^(const Roar &r);
    std::string toString() const;

    friend std::ostream& operator<<(std::ostream& os, const Roar& roar);
  };
}

#endif// RAGEDB_ROAR_H
