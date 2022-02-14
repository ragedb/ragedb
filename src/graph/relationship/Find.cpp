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

#include "../RelationshipTypes.h"
#include "../Shard.h"

namespace ragedb {

  roaring::Roaring64Map RelationshipTypes::getBlanks(uint16_t type_id, const std::string &property) {
    roaring::Roaring64Map blank;
    blank |= properties[type_id].getDeletedMap(property);
    blank |= getDeletedMap(type_id);
    return blank;
  }

  uint64_t RelationshipTypes::countBooleans(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isBooleanProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const bool typedValue = get<bool>(value);
      const std::vector<bool> &vec = properties[type_id].getBooleans(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::Evaluate<bool>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t RelationshipTypes::countIntegers(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isIntegerProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const int64_t typedValue = get<int64_t>(value);
      const std::vector<int64_t> &vec = properties[type_id].getIntegers(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::Evaluate<int64_t>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t RelationshipTypes::countDoubles(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isDoubleProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const double typedValue = get<double>(value);
      const std::vector<double> &vec = properties[type_id].getDoubles(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::Evaluate<double>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t RelationshipTypes::countStrings(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isStringProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::string typedValue = get<std::string>(value);
      const std::vector<std::string> &vec = properties[type_id].getStrings(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateString(operation, vec[internal_id], typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t RelationshipTypes::countBooleanLists(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isBooleanListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<bool> typedValue = get<std::vector<bool>>(value);
      const std::vector<std::vector<bool>> &vec = properties[type_id].getBooleansList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<bool>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t RelationshipTypes::countIntegerLists(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isIntegerListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<int64_t> typedValue = get<std::vector<int64_t>>(value);
      const std::vector<std::vector<int64_t>> &vec = properties[type_id].getIntegersList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<int64_t>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t RelationshipTypes::countDoubleLists(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isDoubleListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<double> typedValue = get<std::vector<double>>(value);
      const std::vector<std::vector<double>> &vec = properties[type_id].getDoublesList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<double>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t RelationshipTypes::countStringLists(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isStringListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<std::string> typedValue = get<std::vector<std::string>>(value);
      const std::vector<std::vector<std::string>> &vec = properties[type_id].getStringsList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<std::string>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t RelationshipTypes::findCount(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    if (!ValidTypeId(type_id)) return 0;

    if(operation == Operation::IS_NULL) {
      uint64_t deleted_rels = getDeletedCount(type_id);
      uint64_t deleted_properties = properties[type_id].getDeletedCount(property);
      return (deleted_properties - deleted_rels);
    }

    if(operation == Operation::NOT_IS_NULL) {
      const uint64_t max_id = starting_node_ids[type_id].size();
      uint64_t deleted_rels = getDeletedCount(type_id);
      uint64_t deleted_properties = properties[type_id].getDeletedCount(property);
      return max_id - (deleted_properties - deleted_rels);
    }

    const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);
    switch (property_type_id) {
      case Properties::getBooleanPropertyType(): {
        return countBooleans(type_id, property, operation, value);
      }
      case Properties::getIntegerPropertyType(): {
        return countIntegers(type_id, property, operation, value);
      }
      case Properties::getDoublePropertyType(): {
        return countDoubles(type_id, property, operation, value);
      }
      case Properties::getStringPropertyType(): {
        return countStrings(type_id, property, operation, value);
      }
      case Properties::getBooleanListPropertyType(): {
        return countBooleanLists(type_id, property, operation, value);
      }
      case Properties::getIntegerListPropertyType(): {
        return countIntegerLists(type_id, property, operation, value);
      }
      case Properties::getDoubleListPropertyType(): {
        return countDoubleLists(type_id, property, operation, value);
      }
      case Properties::getStringListPropertyType(): {
        return countStringLists(type_id, property, operation, value);
      }
      default: {
        return 0;
      }
     }
  }

  std::vector<uint64_t> RelationshipTypes::findNullIds(uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    uint64_t current = 1;
    roaring::Roaring64Map blank;
    blank |= properties[type_id].getDeletedMap(property);
    blank -= getDeletedMap(type_id);
    for (roaring::Roaring64MapSetBitForwardIterator iterator = blank.begin(); iterator != blank.end(); ++iterator) {
      if (current > (skip + limit)) {
        break;
      }
      if (current++ > skip ) {
        ids.emplace_back(Shard::internalToExternal(type_id, *iterator));
      }
    }
    return ids;
  }

  std::vector<uint64_t> RelationshipTypes::findNotNullIds(uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    uint64_t current = 1;
    roaring::Roaring64Map blank;
    const uint64_t max_id = starting_node_ids[type_id].size();
    blank.flip(0, max_id);
    blank -= getDeletedMap(type_id);
    blank -= properties[type_id].getDeletedMap(property);

    for (roaring::Roaring64MapSetBitForwardIterator iterator = blank.begin(); iterator != blank.end(); ++iterator) {
      if (current > (skip + limit)) {
        break;
      }
      if (current++ > skip ) {
        ids.emplace_back(Shard::internalToExternal(type_id, *iterator));
      }
    }
    return ids;
  }

  std::vector<uint64_t> RelationshipTypes::findBooleanIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isBooleanProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const bool typedValue = get<bool>(value);
      const std::vector<bool> &vec = properties[type_id].getBooleans(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::Evaluate<bool>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(Shard::internalToExternal(type_id, internal_id));
        }
        current++;
      }
    }
    return ids;
  }
  
  std::vector<uint64_t> RelationshipTypes::findIntegerIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isIntegerProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const int64_t typedValue = get<int64_t>(value);
      const std::vector<int64_t> &vec = properties[type_id].getIntegers(property);
      std::vector<std::uint64_t> indexes;

      switch(operation) {
      case Operation::EQ: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x == typedValue; });
        break;
      }
      case Operation::NEQ: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x != typedValue; });
        break;
      }
      case Operation::GT: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x > typedValue; });
        break;
      }
      case Operation::GTE: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x >= typedValue; });
        break;
      }
      case Operation::LT: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x < typedValue; });
        break;
      }
      case Operation::LTE: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x <= typedValue; });
        break;
      }
      default:
        break;
      }

      auto it = remove_if(indexes.begin(), indexes.end(), [blank](uint64_t i) { return blank.contains(i); });

      indexes.erase(it, indexes.end());

      for(auto idx : indexes) {
        if(current++ > skip) {
          ids.emplace_back(Shard::internalToExternal(type_id, idx));
        }
        if (current > (skip + limit)) {
          return ids;
        }
      }
    }
    return ids;
  }

  std::vector<uint64_t> RelationshipTypes::findDoubleIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    // Handle values that are parsed as Integers (230 vs 230.0)
    if (Properties::isIntegerProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const double typedValue = static_cast<double>(get<int64_t>(value));
      const std::vector<double> &vec = properties[type_id].getDoubles(property);
      std::vector<std::uint64_t> indexes;

      switch(operation) {
      case Operation::EQ: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x == typedValue; });
        break;
      }
      case Operation::NEQ: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x != typedValue; });
        break;
      }
      case Operation::GT: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x > typedValue; });
        break;
      }
      case Operation::GTE: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x >= typedValue; });
        break;
      }
      case Operation::LT: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x < typedValue; });
        break;
      }
      case Operation::LTE: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x <= typedValue; });
        break;
      }
      default:
        break;
      }

      auto it = remove_if(indexes.begin(), indexes.end(), [blank](uint64_t i) { return blank.contains(i); });

      indexes.erase(it, indexes.end());

      for(auto idx : indexes) {
        if(current++ > skip) {
          ids.emplace_back(Shard::internalToExternal(type_id, idx));
        }
        if (current > (skip + limit)) {
          return ids;
        }
      }
      return ids;
    }

    if (Properties::isDoubleProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const double typedValue = get<double>(value);
      const std::vector<double> &vec = properties[type_id].getDoubles(property);
      std::vector<std::uint64_t> indexes;

      switch(operation) {
      case Operation::EQ: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x == typedValue; });
        break;
      }
      case Operation::NEQ: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x != typedValue; });
        break;
      }
      case Operation::GT: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x > typedValue; });
        break;
      }
      case Operation::GTE: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x >= typedValue; });
        break;
      }
      case Operation::LT: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x < typedValue; });
        break;
      }
      case Operation::LTE: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x <= typedValue; });
        break;
      }
      default:
        break;
      }

      auto it = remove_if(indexes.begin(), indexes.end(), [blank](uint64_t i) { return blank.contains(i); });

      indexes.erase(it, indexes.end());

      for(auto idx : indexes) {
        if(current++ > skip) {
          ids.emplace_back(Shard::internalToExternal(type_id, idx));
        }
        if (current > (skip + limit)) {
          return ids;
        }
      }
      return ids;
    }
    return ids;
  }

  std::vector<uint64_t> RelationshipTypes::findStringIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isStringProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::string typedValue = get<std::string>(value);
      const std::vector<std::string> &vec = properties[type_id].getStrings(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateString(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(Shard::internalToExternal(type_id, internal_id));
        }
        current++;
      }
    }
    return ids;
  }

  std::vector<uint64_t> RelationshipTypes::findBooleanListIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<bool> typedValue = get<std::vector<bool>>(value);
      const std::vector<std::vector<bool>> &vec = properties[type_id].getBooleansList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<bool>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(Shard::internalToExternal(type_id, internal_id));
        }
        current++;
      }
    }
    return ids;
  }

  std::vector<uint64_t> RelationshipTypes::findIntegerListIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isIntegerListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<int64_t> typedValue = get<std::vector<int64_t>>(value);
      const std::vector<std::vector<int64_t>> &vec = properties[type_id].getIntegersList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<int64_t>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(Shard::internalToExternal(type_id, internal_id));
        }
        current++;
      }
    }
    return ids;
  }

  std::vector<uint64_t> RelationshipTypes::findDoubleListIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isIntegerListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      std::vector<int64_t> integerTypedValue = get<std::vector<int64_t>>(value);
      const std::vector<double> typedValue(integerTypedValue.begin(), integerTypedValue.end());
      const std::vector<std::vector<double>> &vec = properties[type_id].getDoublesList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<double>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(Shard::internalToExternal(type_id, internal_id));
        }
        current++;
      }
      return ids;
    }

    if (Properties::isDoubleListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<double> typedValue = get<std::vector<double>>(value);
      const std::vector<std::vector<double>> &vec = properties[type_id].getDoublesList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<double>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(Shard::internalToExternal(type_id, internal_id));
        }
        current++;
      }
      return ids;
    }
    return ids;
  }

  std::vector<uint64_t> RelationshipTypes::findStringListIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isStringListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<std::string> typedValue = get<std::vector<std::string>>(value);
      const std::vector<std::vector<std::string>> &vec = properties[type_id].getStringsList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<std::string>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(Shard::internalToExternal(type_id, internal_id));
        }
        current++;
      }
      return ids;
    }
    return ids;
  }

  std::vector<uint64_t> RelationshipTypes::findIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    // If the type is invalid, we can't find any so return an empty array
    if (!ValidTypeId(type_id)) return std::vector<uint64_t>();

    if(operation == Operation::IS_NULL) {
      return findNullIds(type_id, property, skip, limit);
    }

    if(operation == Operation::NOT_IS_NULL) {
      return findNotNullIds(type_id, property, skip, limit);
    }

    const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);
    switch (property_type_id) {
      case Properties::getBooleanPropertyType(): {
        return findBooleanIds(type_id, property, operation, value, skip, limit);
      }
      case Properties::getIntegerPropertyType(): {
        return findIntegerIds(type_id, property, operation, value, skip, limit);
      }
      case Properties::getDoublePropertyType(): {
        return findDoubleIds(type_id, property, operation, value, skip, limit);
      }
      case Properties::getStringPropertyType(): {
        return findStringIds(type_id, property, operation, value, skip, limit);
      }
      case Properties::getBooleanListPropertyType(): {
        return findBooleanListIds(type_id, property, operation, value, skip, limit);
      }
      case Properties::getIntegerListPropertyType(): {
        return findIntegerListIds(type_id, property, operation, value, skip, limit);
      }
      case Properties::getDoubleListPropertyType(): {
        return findDoubleListIds(type_id, property, operation, value, skip, limit);
      }
      case Properties::getStringListPropertyType(): {
        return findStringListIds(type_id, property, operation, value, skip, limit);
      }
      default: {
        return std::vector<uint64_t>();
      }
    }
  }

  // Start blank, union with deleted properties, remove deleted relationships
  std::vector<Relationship> RelationshipTypes::findNullRelationships(uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit) {
    std::vector<Relationship> relationships;
    int current = 1;

    roaring::Roaring64Map blank;
    blank |= properties[type_id].getDeletedMap(property);
    blank -= getDeletedMap(type_id);
    for (roaring::Roaring64MapSetBitForwardIterator iterator = blank.begin(); iterator != blank.end(); ++iterator) {
      if (current > (skip + limit)) {
        break;
      }
      if (current++ > skip ) {
        relationships.emplace_back(getRelationship(type_id, *iterator));
      }
    }

    return relationships;
  }

  // Start blank, fill from 0 to max_id, remove deleted relationships, remove deleted properties
  std::vector<Relationship> RelationshipTypes::findNotNullRelationships(uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit) {
    std::vector<Relationship> relationships;
    int current = 1;
    const uint64_t max_id = starting_node_ids[type_id].size();
    roaring::Roaring64Map blank;
    blank.flip(0, max_id);
    blank -= getDeletedMap(type_id);
    blank -= properties[type_id].getDeletedMap(property);

    for (roaring::Roaring64MapSetBitForwardIterator iterator = blank.begin(); iterator != blank.end(); ++iterator) {
      if (current > (skip + limit)) {
        break;
      }
      if (current++ > skip ) {
        relationships.emplace_back(getRelationship(type_id, *iterator));
      }
    }
    return relationships;
  }

  std::vector<Relationship> RelationshipTypes::findBooleanRelationships(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Relationship> relationships;
    int current = 1;
    if (Properties::isBooleanProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const bool typedValue = get<bool>(value);
      const std::vector<bool> &vec = properties[type_id].getBooleans(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return relationships;
        }
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::Evaluate<bool>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          relationships.emplace_back(getRelationship(type_id, internal_id));
        }
        current++;
      }
    }
    return relationships;
  }

  std::vector<Relationship> RelationshipTypes::findIntegerRelationships(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Relationship> relationships;
    int current = 1;
    if (Properties::isIntegerProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const int64_t typedValue = get<int64_t>(value);
      const std::vector<int64_t> &vec = properties[type_id].getIntegers(property);
      std::vector<std::uint64_t> indexes;

      switch(operation) {
      case Operation::EQ: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x == typedValue; });
        break;
      }
      case Operation::NEQ: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x != typedValue; });
        break;
      }
      case Operation::GT: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x > typedValue; });
        break;
      }
      case Operation::GTE: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x >= typedValue; });
        break;
      }
      case Operation::LT: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x < typedValue; });
        break;
      }
      case Operation::LTE: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x <= typedValue; });
        break;
      }
      default:
        break;
      }

      auto it = remove_if(indexes.begin(), indexes.end(), [blank](uint64_t i) { return blank.contains(i); });

      indexes.erase(it, indexes.end());

      for(auto idx : indexes) {
        if(current++ > skip) {
          relationships.emplace_back(getRelationship(type_id, idx));
        }
        if (current > (skip + limit)) {
          return relationships;
        }
      }
    }
    return relationships;
  }

  std::vector<Relationship> RelationshipTypes::findDoubleRelationships(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Relationship> relationships;
    int current = 1;
    // Handle values that are parsed as Integers (230 vs 230.0)
    if (Properties::isIntegerProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const double typedValue = static_cast<double>(get<int64_t>(value));
      const std::vector<double> &vec = properties[type_id].getDoubles(property);
      std::vector<std::uint64_t> indexes;

      switch(operation) {
      case Operation::EQ: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x == typedValue; });
        break;
      }
      case Operation::NEQ: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x != typedValue; });
        break;
      }
      case Operation::GT: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x > typedValue; });
        break;
      }
      case Operation::GTE: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x >= typedValue; });
        break;
      }
      case Operation::LT: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x < typedValue; });
        break;
      }
      case Operation::LTE: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x <= typedValue; });
        break;
      }
      default:
        break;
      }

      auto it = remove_if(indexes.begin(), indexes.end(), [blank](uint64_t i) { return blank.contains(i); });

      indexes.erase(it, indexes.end());

      for(auto idx : indexes) {
        if(current++ > skip) {
          relationships.emplace_back(getRelationship(type_id, idx));
        }
        if (current > (skip + limit)) {
          return relationships;
        }
      }
      return relationships;
    }

    if (Properties::isDoubleProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const double typedValue = get<double>(value);
      const std::vector<double> &vec = properties[type_id].getDoubles(property);
      std::vector<std::uint64_t> indexes;

      switch(operation) {
      case Operation::EQ: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x == typedValue; });
        break;
      }
      case Operation::NEQ: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x != typedValue; });
        break;
      }
      case Operation::GT: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x > typedValue; });
        break;
      }
      case Operation::GTE: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x >= typedValue; });
        break;
      }
      case Operation::LT: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x < typedValue; });
        break;
      }
      case Operation::LTE: {
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x <= typedValue; });
        break;
      }
      default:
        break;
      }

      auto it = remove_if(indexes.begin(), indexes.end(), [blank](uint64_t i) { return blank.contains(i); });

      indexes.erase(it, indexes.end());

      for(auto idx : indexes) {
        if(current++ > skip) {
          relationships.emplace_back(getRelationship(type_id, idx));
        }
        if (current > (skip + limit)) {
          return relationships;
        }
      }
    }
    return relationships;
  }

  std::vector<Relationship> RelationshipTypes::findStringRelationships(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Relationship> relationships;
    int current = 1;
    if (Properties::isStringProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::string typedValue = get<std::string>(value);
      const std::vector<std::string> &vec = properties[type_id].getStrings(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return relationships;
        }
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateString(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          relationships.emplace_back(getRelationship(type_id, internal_id));
        }
        current++;
      }
    }
    return relationships;
  }

  std::vector<Relationship> RelationshipTypes::findBooleanListRelationships(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Relationship> relationships;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<bool> typedValue = get<std::vector<bool>>(value);
      const std::vector<std::vector<bool>> &vec = properties[type_id].getBooleansList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return relationships;
        }
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<bool>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          relationships.emplace_back(getRelationship(type_id, internal_id));
        }
        current++;
      }
    }
    return relationships;
  }

  std::vector<Relationship> RelationshipTypes::findIntegerListRelationships(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Relationship> relationships;
    int current = 1;
    if (Properties::isIntegerListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<int64_t> typedValue = get<std::vector<int64_t>>(value);
      const std::vector<std::vector<int64_t>> &vec = properties[type_id].getIntegersList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return relationships;
        }
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<int64_t>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          relationships.emplace_back(getRelationship(type_id, internal_id));
        }
        current++;
      }
    }
    return relationships;
  }

  std::vector<Relationship> RelationshipTypes::findDoubleListRelationships(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Relationship> relationships;
    int current = 1;

    if (Properties::isIntegerListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      std::vector<int64_t> integerTypedValue = get<std::vector<int64_t>>(value);
      const std::vector<double> typedValue(integerTypedValue.begin(), integerTypedValue.end());
      const std::vector<std::vector<double>> &vec = properties[type_id].getDoublesList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return relationships;
        }
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<double>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          relationships.emplace_back(getRelationship(type_id, internal_id));
        }

        current++;
      }
      return relationships;
    }
    if (Properties::isDoubleListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<double> typedValue = get<std::vector<double>>(value);
      const std::vector<std::vector<double>> &vec = properties[type_id].getDoublesList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return relationships;
        }
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<double>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          relationships.emplace_back(getRelationship(type_id, internal_id));
        }
        current++;
      }
    }
    return relationships;
  }

  std::vector<Relationship> RelationshipTypes::findStringListRelationships(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Relationship> relationships;
    int current = 1;
    if (Properties::isStringListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<std::string> typedValue = get<std::vector<std::string>>(value);
      const std::vector<std::vector<std::string>> &vec = properties[type_id].getStringsList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return relationships;
        }
        // If the relationship or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<std::string>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          relationships.emplace_back(getRelationship(type_id, internal_id));
        }
        current++;
      }
    }
    return relationships;
  }

  std::vector<Relationship> RelationshipTypes::findRelationships(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    // If the type is invalid, we can't find any so return an empty array
    if (!ValidTypeId(type_id)) return std::vector<Relationship>();

    if(operation == Operation::IS_NULL) {
      return findNullRelationships(type_id, property, skip, limit);
    }

    if(operation == Operation::NOT_IS_NULL) {
      return findNotNullRelationships(type_id, property, skip, limit);
    }

    const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);

    switch (property_type_id) {
    case Properties::getBooleanPropertyType(): {
      return findBooleanRelationships(type_id, property, operation, value, skip, limit);
    }
    case Properties::getIntegerPropertyType(): {
      return findIntegerRelationships(type_id, property, operation, value, skip, limit);
    }
    case Properties::getDoublePropertyType(): {
      return findDoubleRelationships(type_id, property, operation, value, skip, limit);
    }
    case Properties::getStringPropertyType(): {
      return findStringRelationships(type_id, property, operation, value, skip, limit);
    }
    case Properties::getBooleanListPropertyType(): {
      return findBooleanListRelationships(type_id, property, operation, value, skip, limit);
    }
    case Properties::getIntegerListPropertyType(): {
      return findIntegerListRelationships(type_id, property, operation, value, skip, limit);
    }
    case Properties::getDoubleListPropertyType(): {
      return findDoubleListRelationships(type_id, property, operation, value, skip, limit);
    }
    case Properties::getStringListPropertyType(): {
      return findStringListRelationships(type_id, property, operation, value, skip, limit);
    }
    default: {
      return std::vector<Relationship>();
    }
    }
  }


}