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

#include "../NodeTypes.h"
#include "../Shard.h"

namespace ragedb {

  void NodeTypes::removeDeletedIds(uint16_t type_id, std::vector<uint64_t> &list) {
    auto new_end = std::remove_if(list.begin(), list.end(),
      [deleted = deleted_ids[type_id] ](auto id)
      { return deleted.contains(Shard::externalToInternal(id)); });

    list.erase(new_end, list.end());
  }

  void NodeTypes::removeDeletedProperties(uint16_t type_id, std::vector<uint64_t> &list, const std::string &property) {
    auto new_end = std::remove_if(list.begin(), list.end(),
      [deleted = properties[type_id].getDeletedMap(property) ](auto id)
      { return deleted.contains(Shard::externalToInternal(id)); });

    list.erase(new_end, list.end());
  }

  uint64_t NodeTypes::filterCountBooleans(std::vector<uint64_t> ids, uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isBooleanProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const bool typedValue = get<bool>(value);
      for(auto id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains( Shard::externalToInternal(id))) {
          continue;
        }
        if (!Expression::Evaluate<bool>(operation, properties[type_id].getBooleanProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t NodeTypes::filterCountIntegers(std::vector<uint64_t> ids, uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isIntegerProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const int64_t typedValue = get<int64_t>(value);
      for(auto id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains( Shard::externalToInternal(id))) {
          continue;
        }
        if (!Expression::Evaluate<int64_t>(operation, properties[type_id].getIntegerProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t NodeTypes::filterCountDoubles(std::vector<uint64_t> ids, uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isDoubleProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const double typedValue = get<double>(value);
      for(auto id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains( Shard::externalToInternal(id))) {
          continue;
        }
        if (!Expression::Evaluate<double>(operation, properties[type_id].getDoubleProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t NodeTypes::filterCountStrings(std::vector<uint64_t> ids, uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isStringProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::string typedValue = get<std::string>(value);
      for(auto id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains( Shard::externalToInternal(id))) {
          continue;
        }
        if (!Expression::EvaluateString(operation, properties[type_id].getStringProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t NodeTypes::filterCountBooleanLists(std::vector<uint64_t> ids, uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isBooleanListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<bool> typedValue = get<std::vector<bool>>(value);
      for(auto id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains( Shard::externalToInternal(id))) {
          continue;
        }
        if (!Expression::EvaluateVector<bool>(operation, properties[type_id].getListOfBooleanProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t NodeTypes::filterCountIntegerLists(std::vector<uint64_t> ids, uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isIntegerListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<int64_t> typedValue = get<std::vector<int64_t>>(value);
      for(auto id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains( Shard::externalToInternal(id))) {
          continue;
        }
        if (!Expression::EvaluateVector<int64_t>(operation, properties[type_id].getListOfIntegerProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t NodeTypes::filterCountDoubleLists(std::vector<uint64_t> ids, uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isDoubleListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<double> typedValue = get<std::vector<double>>(value);
      for(auto id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains( Shard::externalToInternal(id))) {
          continue;
        }
        if (!Expression::EvaluateVector<double>(operation, properties[type_id].getListOfDoubleProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t NodeTypes::filterCountStringLists(std::vector<uint64_t> ids, uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isStringListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<std::string> typedValue = get<std::vector<std::string>>(value);
      for(auto id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains( Shard::externalToInternal(id))) {
          continue;
        }
        if (!Expression::EvaluateVector<std::string>(operation, properties[type_id].getListOfStringProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        count++;
      }
    }
    return count;
  }

  uint64_t NodeTypes::filterCount(std::vector<uint64_t> unfiltered, uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    // If the type is invalid, we can't filter any so return zero
    if (!ValidTypeId(type_id)) return 0;

    if(operation == Operation::IS_NULL) {
      uint64_t count = 0;
      for (auto id : unfiltered) {
        count += properties[type_id].isDeleted(property, id);
      }
      return count;
    }

    if(operation == Operation::NOT_IS_NULL) {
      uint64_t count = 0;
      for (auto id : unfiltered) {
        count += (1 - properties[type_id].isDeleted(property, id));
      }
      return count;
    }

    const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);

    switch (property_type_id) {
      case Properties::getBooleanPropertyType(): {
        return filterCountBooleans(unfiltered, type_id, property, operation, value);
      }
      case Properties::getIntegerPropertyType(): {
        return filterCountIntegers(unfiltered, type_id, property, operation, value);
      }
      case Properties::getDoublePropertyType(): {
        return filterCountDoubles(unfiltered, type_id, property, operation, value);
      }
      case Properties::getStringPropertyType(): {
        return filterCountStrings(unfiltered, type_id, property, operation, value);
      }
      case Properties::getBooleanListPropertyType(): {
        return filterCountBooleanLists(unfiltered, type_id, property, operation, value);
      }
      case Properties::getIntegerListPropertyType(): {
        return filterCountIntegerLists(unfiltered, type_id, property, operation, value);
      }
      case Properties::getDoubleListPropertyType(): {
        return filterCountDoubleLists(unfiltered, type_id, property, operation, value);
      }
      case Properties::getStringListPropertyType(): {
        return filterCountStringLists(unfiltered, type_id, property, operation, value);
      }
      default: {
        return 0;
      }
    }
  }

  std::vector<uint64_t> NodeTypes::filterNullIds(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    uint64_t current = 1;
    for (auto id : list) {
      if (current > (skip + limit)) {
        break;
      }

      if (properties[type_id].isDeleted(property, Shard::externalToInternal(id))) {
        if (current++ > skip ) {
          ids.emplace_back(id);
        }
      }
    }
    return ids;
  }

  std::vector<uint64_t> NodeTypes::filterNotNullIds(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    uint64_t current = 1;
    for (auto id : list) {
      if (current > (skip + limit)) {
        break;
      }

      if (!properties[type_id].isDeleted(property, Shard::externalToInternal(id))) {
        if (current++ > skip ) {
          ids.emplace_back(id);
        }
      }
    }
    return ids;
  }

  std::vector<uint64_t> NodeTypes::filterBooleanIds(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isBooleanProperty(value)) {
      const bool typedValue = get<bool>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }
        if (!Expression::Evaluate<bool>(operation, properties[type_id].getBooleanProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(id);
        }
        current++;
      }
    }
    return ids;
  }
  
  std::vector<uint64_t> NodeTypes::filterIntegerIds(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isIntegerProperty(value)) {
      const int64_t typedValue = get<int64_t>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }

        if (!Expression::Evaluate<int64_t>(operation, properties[type_id].getIntegerProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(id);
        }
        current++;
      }
    }
    return ids;
  }
  
  std::vector<uint64_t> NodeTypes::filterDoubleIds(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isDoubleProperty(value)) {
      const double typedValue = get<double>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }

        if (!Expression::Evaluate<double>(operation, properties[type_id].getDoubleProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(id);
        }
        current++;
      }
    }
    return ids;
  }
  
  std::vector<uint64_t> NodeTypes::filterStringIds(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isStringProperty(value)) {
      const std::string typedValue = get<std::string>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }

        if (!Expression::Evaluate<std::string>(operation, properties[type_id].getStringProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(id);
        }
        current++;
      }
    }
    return ids;
  }
  
  std::vector<uint64_t> NodeTypes::filterBooleanListIds(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<bool> typedValue = get<std::vector<bool>>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }
        if (!Expression::Evaluate<std::vector<bool>>(operation, properties[type_id].getListOfBooleanProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(id);
        }
        current++;
      }
    }
    return ids;
  }
  
  std::vector<uint64_t> NodeTypes::filterIntegerListIds(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<int64_t> typedValue = get<std::vector<int64_t>>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }
        if (!Expression::Evaluate<std::vector<int64_t>>(operation, properties[type_id].getListOfIntegerProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(id);
        }
        current++;
      }
    }
    return ids;
  }
  
  std::vector<uint64_t> NodeTypes::filterDoubleListIds(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<double> typedValue = get<std::vector<double>>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }
        if (!Expression::Evaluate<std::vector<double>>(operation, properties[type_id].getListOfDoubleProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(id);
        }
        current++;
      }
    }
    return ids;
  }
  
  std::vector<uint64_t> NodeTypes::filterStringListIds(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<std::string> typedValue = get<std::vector<std::string>>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return ids;
        }
        if (!Expression::Evaluate<std::vector<std::string>>(operation, properties[type_id].getListOfStringProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          ids.emplace_back(id);
        }
        current++;
      }
    }
    return ids;
  }

  std::vector<uint64_t> NodeTypes::filterIds(std::vector<uint64_t> unfiltered, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    // If the type is invalid, we can't filter any so return an empty array
    if (!ValidTypeId(type_id)) return std::vector<uint64_t>();

    // Deleted Nodes will never return from a filter
    removeDeletedIds(type_id, unfiltered);

    if(operation == Operation::IS_NULL) {
      return filterNullIds(unfiltered, type_id, property, skip, limit);
    }

    if(operation == Operation::NOT_IS_NULL) {
      return filterNotNullIds(unfiltered, type_id, property, skip, limit);
    }

    // From here on out we don't want deleted properties
    removeDeletedProperties(type_id, unfiltered, property);

    const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);
    switch (property_type_id) {
      case Properties::getBooleanPropertyType(): {
        return filterBooleanIds(unfiltered, type_id, property, operation, value, skip, limit);
      }
      case Properties::getIntegerPropertyType(): {
        return filterIntegerIds(unfiltered, type_id, property, operation, value, skip, limit);
      }
      case Properties::getDoublePropertyType(): {
        return filterDoubleIds(unfiltered, type_id, property, operation, value, skip, limit);
      }
      case Properties::getStringPropertyType(): {
        return filterStringIds(unfiltered, type_id, property, operation, value, skip, limit);
      }
      case Properties::getBooleanListPropertyType(): {
        return filterBooleanListIds(unfiltered, type_id, property, operation, value, skip, limit);
      }
      case Properties::getIntegerListPropertyType(): {
        return filterIntegerListIds(unfiltered, type_id, property, operation, value, skip, limit);
      }
      case Properties::getDoubleListPropertyType(): {
        return filterDoubleListIds(unfiltered, type_id, property, operation, value, skip, limit);
      }
      case Properties::getStringListPropertyType(): {
        return filterStringListIds(unfiltered, type_id, property, operation, value, skip, limit);
      }
      default: {
        return std::vector<uint64_t>();
      }
    }
  }

  std::vector<Node> NodeTypes::filterNullNodes(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    uint64_t current = 1;
    for (auto id : list) {
      if (current > (skip + limit)) {
        break;
      }

      if (properties[type_id].isDeleted(property, Shard::externalToInternal(id))) {
        if (current++ > skip ) {
          nodes.emplace_back(getNode(id));
        }
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterNotNullNodes(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    uint64_t current = 1;
    for (auto id : list) {
      if (current > (skip + limit)) {
        break;
      }

      if (!properties[type_id].isDeleted(property, Shard::externalToInternal(id))) {
        if (current++ > skip ) {
          nodes.emplace_back(getNode(id));
        }
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterBooleanNodes(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isBooleanProperty(value)) {
      const bool typedValue = get<bool>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }
        if (!Expression::Evaluate<bool>(operation, properties[type_id].getBooleanProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(id));
        }
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterIntegerNodes(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isIntegerProperty(value)) {
      const int64_t typedValue = get<int64_t>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }

        if (!Expression::Evaluate<int64_t>(operation, properties[type_id].getIntegerProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(id));
        }
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterDoubleNodes(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isDoubleProperty(value)) {
      const double typedValue = get<double>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }

        if (!Expression::Evaluate<double>(operation, properties[type_id].getDoubleProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(id));
        }
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterStringNodes(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isStringProperty(value)) {
      const std::string typedValue = get<std::string>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }

        if (!Expression::Evaluate<std::string>(operation, properties[type_id].getStringProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(id));
        }
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterBooleanListNodes(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<bool> typedValue = get<std::vector<bool>>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }
        if (!Expression::Evaluate<std::vector<bool>>(operation, properties[type_id].getListOfBooleanProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(id));
        }
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterIntegerListNodes(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<int64_t> typedValue = get<std::vector<int64_t>>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }
        if (!Expression::Evaluate<std::vector<int64_t>>(operation, properties[type_id].getListOfIntegerProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(id));
        }
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterDoubleListNodes(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<double> typedValue = get<std::vector<double>>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }
        if (!Expression::Evaluate<std::vector<double>>(operation, properties[type_id].getListOfDoubleProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(id));
        }
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterStringListNodes(std::vector<uint64_t> list, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<std::string> typedValue = get<std::vector<std::string>>(value);
      for (auto id : list) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }
        if (!Expression::Evaluate<std::vector<std::string>>(operation, properties[type_id].getListOfStringProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(id));
        }
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterNodes(std::vector<uint64_t> unfiltered, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    // If the type is invalid, we can't filter any so return an empty array
    if (!ValidTypeId(type_id)) return std::vector<Node>();

    // Deleted Nodes will never return from a filter
    removeDeletedIds(type_id, unfiltered);

    if(operation == Operation::IS_NULL) {
      return filterNullNodes(unfiltered, type_id, property, skip, limit);
    }

    if(operation == Operation::NOT_IS_NULL) {
      return filterNotNullNodes(unfiltered, type_id, property, skip, limit);
    }

    const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);
    switch (property_type_id) {
      case Properties::getBooleanPropertyType(): {
        return filterBooleanNodes(unfiltered, type_id, property, operation, value, skip, limit);
      }
      case Properties::getIntegerPropertyType(): {
        return filterIntegerNodes(unfiltered, type_id, property, operation, value, skip, limit);
      }
      case Properties::getDoublePropertyType(): {
        return filterDoubleNodes(unfiltered, type_id, property, operation, value, skip, limit);
      }
      case Properties::getStringPropertyType(): {
        return filterStringNodes(unfiltered, type_id, property, operation, value, skip, limit);
      }
      case Properties::getBooleanListPropertyType(): {
        return filterBooleanListNodes(unfiltered, type_id, property, operation, value, skip, limit);
      }
      case Properties::getIntegerListPropertyType(): {
        return filterIntegerListNodes(unfiltered, type_id, property, operation, value, skip, limit);
      }
      case Properties::getDoubleListPropertyType(): {
        return filterDoubleListNodes(unfiltered, type_id, property, operation, value, skip, limit);
      }
      case Properties::getStringListPropertyType(): {
        return filterStringListNodes(unfiltered, type_id, property, operation, value, skip, limit);
      }
      default: {
        return std::vector<Node>();
      }
    }
  }

}