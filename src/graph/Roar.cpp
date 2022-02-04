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

#include "Roar.h"

namespace ragedb {
  Roar::Roar() {
    map = Roaring64Map();
  }

  void Roar::add(uint64_t x) {
    map.add(x);
  }

  void Roar::remove(uint64_t x) {
    map.remove(x);
  }

  bool Roar::addChecked(uint64_t x) {
    return map.addChecked(x);
  }

  bool Roar::removeChecked(uint64_t x) {
    return map.removeChecked(x);
  }

  void Roar::clear() {
    return map.clear();
  }

  uint64_t Roar::maximum() const {
    return map.maximum();
  }

  uint64_t Roar::minimum() const {
    return map.minimum();
  }

  bool Roar::contains(uint64_t x) const {
    return map.contains(x);
  }

  Roar &Roar::operator&=(const Roar &r) {
    map &= r.map;
    return *this;
  }

  Roar &Roar::operator-=(const Roar &r) {
    map -= r.map;
    return *this;
  }

  Roar &Roar::operator|=(const Roar &r) {
    map |= r.map;
    return *this;
  }

  Roar &Roar::operator^=(const Roar &r) {
    map ^= r.map;
    return *this;
  }

  Roar Roar::swap(Roar &r) {
    map.swap(r.map);
    return *this;
  }
  uint64_t Roar::cardinality() const {
    return map.cardinality();
  }

  bool Roar::isEmpty() const {
    return map.isEmpty();
  }

  bool Roar::isFull() const {
    return map.isFull();
  }

  bool Roar::isSubset(const Roar &r) const {
    return map.isSubset(r.map);
  }

  bool Roar::isStrictSubset(const Roar &r) const {
    return false;
  }

  bool Roar::operator==(const Roar &r) const {
    return false;
  }

  void Roar::flip(uint64_t range_start, uint64_t range_end) {
    map.flip(range_start, range_end);
  }

  bool Roar::removeRunCompression() {
    return map.removeRunCompression();
  }

  bool Roar::runOptimize() {
    return map.runOptimize();
  }

  size_t Roar::shrinkToFit() {
    return map.shrinkToFit();
  }

  uint64_t Roar::rank(uint64_t x) const {
    return map.rank(x);
  }

  Roar Roar::operator&(const Roar &r) {
    return Roar(*this) &= r;
  }

  Roar Roar::operator-(const Roar &r) {
    return Roar(*this) -= r;
  }

  Roar Roar::operator|(const Roar &r) {
    return Roar(*this) |= r;
  }

  Roar Roar::operator^(const Roar &r) {
    return Roar(*this) ^= r;
  }

  std::string Roar::toString() const {
    return map.toString();
  }

  void Roar::addIds(std::vector<uint64_t> ids) {
    map.addMany(ids.size(), ids.data());
  }

  void Roar::addNodeIds(std::vector<Link> links) {
    uint64_t arr[links.size()];
    for (auto i = 0; i < links.size(); i++) {
      arr[i] = links[i].node_id;
    }
    map.addMany(links.size(), arr);
  }

  void Roar::addRelationshipIds(std::vector<Link> links) {
    uint64_t arr[links.size()];
    for (auto i = 0; i < links.size(); i++) {
      arr[i] = links[i].rel_id;
    }
    map.addMany(links.size(), arr);
  }

  std::ostream &operator<<(std::ostream &os, const Roar &roar) {
    os << '[';
    bool nested_initial = true;
    for(auto i = std::begin(roar.map); i  != std::end(roar.map); i++){
      if (!nested_initial) {
        os << ", ";
      }
      os << i.operator*();
      nested_initial = false;
    }

    os << ']';
    return os;
  }

  std::vector<uint64_t> Roar::getIds() {
    std::vector<uint64_t> ids;
    ids.reserve(map.cardinality());
    uint64_t array[map.cardinality()];
    map.toUint64Array(array);
    ids.assign(array, array + map.cardinality());
    return ids;
  }

  sol::as_table_t<std::vector<uint64_t>> Roar::getIdsLua() {
    return sol::as_table(getIds());
  }

  std::vector<Link> Roar::getNodeHalfLinks() {
    std::vector<Link> links;
    links.reserve(map.cardinality());

    uint64_t array[map.cardinality()];
    map.toUint64Array(array);

    for (auto element : array) {
      links.push_back( Link({element,0}));
    }

    return links;
  }

  sol::as_table_t<std::vector<Link>> Roar::getNodeHalfLinksLua() {
    return sol::as_table(getNodeHalfLinks());
  }

  std::vector<Link> Roar::getRelationshipHalfLinks() {
    std::vector<Link> links;
    links.reserve(map.cardinality());

    uint64_t array[map.cardinality()];
    map.toUint64Array(array);

    for (auto element : array) {
      links.push_back( Link({0,element}));
    }

    return links;
  }

  sol::as_table_t<std::vector<Link>> Roar::getRelationshipHalfLinksLua() {
    return sol::as_table(getRelationshipHalfLinks());
  }

}