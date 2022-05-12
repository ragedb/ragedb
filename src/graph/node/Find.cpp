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

  roaring::Roaring64Map NodeTypes::getBlanks(uint16_t type_id, const std::string &property) {
    roaring::Roaring64Map blank;
    blank |= properties[type_id].getDeletedMap(property);
    blank |= getDeletedMap(type_id);
    return blank;
  }

  uint64_t NodeTypes::countBooleans(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isBooleanProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const bool typedValue = get<bool>(value);
      const std::vector<bool> &vec = properties[type_id].getBooleans(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the node or property has been deleted, ignore it
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

  uint64_t NodeTypes::countIntegers(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isIntegerProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const int64_t typedValue = get<int64_t>(value);
      const std::vector<int64_t> &vec = properties[type_id].getIntegers(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the node or property has been deleted, ignore it
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

  uint64_t NodeTypes::countDoubles(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isDoubleProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const double typedValue = get<double>(value);
      const std::vector<double> &vec = properties[type_id].getDoubles(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the node or property has been deleted, ignore it
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

  uint64_t NodeTypes::countStrings(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isStringProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::string typedValue = get<std::string>(value);
      const std::vector<std::string> &vec = properties[type_id].getStrings(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the node or property has been deleted, ignore it
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

  uint64_t NodeTypes::countBooleanLists(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isBooleanListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<bool> typedValue = get<std::vector<bool>>(value);
      const std::vector<std::vector<bool>> &vec = properties[type_id].getBooleansList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the node or property has been deleted, ignore it
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

  uint64_t NodeTypes::countIntegerLists(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isIntegerListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<int64_t> typedValue = get<std::vector<int64_t>>(value);
      const std::vector<std::vector<int64_t>> &vec = properties[type_id].getIntegersList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the node or property has been deleted, ignore it
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

  uint64_t NodeTypes::countDoubleLists(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isDoubleListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<double> typedValue = get<std::vector<double>>(value);
      const std::vector<std::vector<double>> &vec = properties[type_id].getDoublesList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the node or property has been deleted, ignore it
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

  uint64_t NodeTypes::countStringLists(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    uint64_t count = 0;
    if (Properties::isStringListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<std::string> typedValue = get<std::vector<std::string>>(value);
      const std::vector<std::vector<std::string>> &vec = properties[type_id].getStringsList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If the node or property has been deleted, ignore it
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

  uint64_t NodeTypes::findCount(uint16_t type_id, const std::string &property, Operation operation, property_type_t value) {
    // If the type is invalid, we can't find any so return zero
    if (!ValidTypeId(type_id)) return 0;

    // Checking for null, so find out how many nodes have been deleted (since they don't count)
    // and subtract them from the properties that have been deleted
    if(operation == Operation::IS_NULL) {
      uint64_t deleted_nodes = getDeletedCount(type_id);
      uint64_t deleted_properties = properties[type_id].getDeletedCount(property);
      return (deleted_properties - deleted_nodes);
    }

    // Checking for not null so find the maximum size of the array for this property type,
    // and subtract the deleted properties (minus the deleted nodes since they don't count)
    if(operation == Operation::NOT_IS_NULL) {
      const size_t max_id = key_to_node_id[type_id].size();
      uint64_t deleted_nodes = getDeletedCount(type_id);
      uint64_t deleted_properties = properties[type_id].getDeletedCount(property);
      return max_id - (deleted_properties - deleted_nodes);
    }

    const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);

    switch (property_type_id) {
      case Properties::boolean_type:
        return countBooleans(type_id, property, operation, value);
      case Properties::integer_type:
        return countIntegers(type_id, property, operation, value);
      case Properties::double_type:
        return countDoubles(type_id, property, operation, value);
      case Properties::date_type:
        // TODO: Verify This
        return countDoubles(type_id, property, operation, value);
      case Properties::string_type:
        return countStrings(type_id, property, operation, value);
      case Properties::boolean_list_type:
        return countBooleanLists(type_id, property, operation, value);
      case Properties::integer_list_type:
        return countIntegerLists(type_id, property, operation, value);
      case Properties::double_list_type:
        return countDoubleLists(type_id, property, operation, value);
      case Properties::date_list_type:
        // TODO: Verify This
        return countDoubleLists(type_id, property, operation, value);
      case Properties::string_list_type:
        return countStringLists(type_id, property, operation, value);
      default:
        return 0;
    }
  }

  // Start blank, union with deleted properties, remove deleted nodes
  std::vector<uint64_t> NodeTypes::findNullIds(uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit) {
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

  // Start blank, fill from 0 to max_id, remove deleted nodes, remove deleted properties
  std::vector<uint64_t> NodeTypes::findNotNullIds(uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    uint64_t current = 1;
    roaring::Roaring64Map blank;
    const uint64_t max_id = key_to_node_id[type_id].size();
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

  std::vector<uint64_t> NodeTypes::findBooleanIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
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
        // If the node or property has been deleted, ignore it
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

  std::vector<uint64_t> NodeTypes::findIntegerIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isIntegerProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const int64_t typedValue = get<int64_t>(value);
      const std::vector<int64_t> &vec = properties[type_id].getIntegers(property);
      std::vector<std::uint64_t> indexes;

      switch(operation) {
        case Operation::EQ:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x == typedValue; });
          break;
        case Operation::NEQ:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x != typedValue; });
          break;
        case Operation::GT:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x > typedValue; });
          break;
        case Operation::GTE:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x >= typedValue; });
          break;
        case Operation::LT:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x < typedValue; });
          break;
        case Operation::LTE:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x <= typedValue; });
          break;
        default:
          break;
      }

      std::erase_if(indexes, [blank](uint64_t i) { return blank.contains(i); });

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

  std::vector<uint64_t> NodeTypes::findDoubleIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    int current = 1;
    // Handle values that are parsed as Integers (230 vs 230.0)
    if (Properties::isIntegerProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const double typedValue = static_cast<double>(get<int64_t>(value));
      const std::vector<double> &vec = properties[type_id].getDoubles(property);
      std::vector<std::uint64_t> indexes;

      switch(operation) {
        case Operation::EQ:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x == typedValue; });
          break;
        case Operation::NEQ:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x != typedValue; });
          break;
        case Operation::GT:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x > typedValue; });
          break;
        case Operation::GTE:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x >= typedValue; });
          break;
        case Operation::LT:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x < typedValue; });
          break;
        case Operation::LTE:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x <= typedValue; });
          break;
        default:
          break;
      }

      std::erase_if(indexes, [blank](uint64_t i) { return blank.contains(i); });

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
        case Operation::EQ:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x == typedValue; });
          break;
        case Operation::NEQ:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x != typedValue; });
          break;
        case Operation::GT:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x > typedValue; });
          break;
        case Operation::GTE:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x >= typedValue; });
          break;
        case Operation::LT:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x < typedValue; });
          break;
        case Operation::LTE:
          indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x <= typedValue; });
          break;
        default:
          break;
      }

      std::erase_if(indexes, [blank](uint64_t i) { return blank.contains(i); });

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

  std::vector<uint64_t> NodeTypes::findStringIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
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
        // If the node or property has been deleted, ignore it
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

  std::vector<uint64_t> NodeTypes::findBooleanListIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
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
        // If the node or property has been deleted, ignore it
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

  std::vector<uint64_t> NodeTypes::findIntegerListIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
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
        // If the node or property has been deleted, ignore it
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

  std::vector<uint64_t> NodeTypes::findDoubleListIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
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
        // If the node or property has been deleted, ignore it
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
        // If the node or property has been deleted, ignore it
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

  std::vector<uint64_t> NodeTypes::findStringListIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
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
        // If the node or property has been deleted, ignore it
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

  std::vector<uint64_t> NodeTypes::findIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
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
      case Properties::boolean_type:
        return findBooleanIds(type_id, property, operation, value, skip, limit);
      case Properties::integer_type:
        return findIntegerIds(type_id, property, operation, value, skip, limit);
      case Properties::double_type:
        return findDoubleIds(type_id, property, operation, value, skip, limit);
      case Properties::date_type:
        // TODO: Verify This
        return findDoubleIds(type_id, property, operation, value, skip, limit);
      case Properties::string_type:
        return findStringIds(type_id, property, operation, value, skip, limit);
      case Properties::boolean_list_type:
        return findBooleanListIds(type_id, property, operation, value, skip, limit);
      case Properties::integer_list_type:
        return findIntegerListIds(type_id, property, operation, value, skip, limit);
      case Properties::double_list_type:
        return findDoubleListIds(type_id, property, operation, value, skip, limit);
      case Properties::date_list_type:
        // TODO: Verify This
        return findDoubleListIds(type_id, property, operation, value, skip, limit);
      case Properties::string_list_type:
        return findStringListIds(type_id, property, operation, value, skip, limit);
      default:
        return std::vector<uint64_t>();
    }
  }

  // Start blank, union with deleted properties, remove deleted nodes
  std::vector<Node> NodeTypes::findNullNodes(uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;

    roaring::Roaring64Map blank;
    blank |= properties[type_id].getDeletedMap(property);
    blank -= getDeletedMap(type_id);
    for (roaring::Roaring64MapSetBitForwardIterator iterator = blank.begin(); iterator != blank.end(); ++iterator) {
      if (current > (skip + limit)) {
        break;
      }
      if (current++ > skip ) {
        nodes.emplace_back(getNode(type_id, *iterator));
      }
    }

    return nodes;
  }

  // Start blank, fill from 0 to max_id, remove deleted nodes, remove deleted properties
  std::vector<Node> NodeTypes::findNotNullNodes(uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    const uint64_t max_id = key_to_node_id[type_id].size();
    roaring::Roaring64Map blank;
    blank.flip(0, max_id);
    blank -= getDeletedMap(type_id);
    blank -= properties[type_id].getDeletedMap(property);

    for (roaring::Roaring64MapSetBitForwardIterator iterator = blank.begin(); iterator != blank.end(); ++iterator) {
      if (current > (skip + limit)) {
        break;
      }
      if (current++ > skip ) {
        nodes.emplace_back(getNode(type_id, *iterator));
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::findBooleanNodes(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isBooleanProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const bool typedValue = get<bool>(value);
      const std::vector<bool> &vec = properties[type_id].getBooleans(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }
        // If the node or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::Evaluate<bool>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(type_id, internal_id));
        }
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::findIntegerNodes(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isIntegerProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const int64_t typedValue = get<int64_t>(value);
      const std::vector<int64_t> &vec = properties[type_id].getIntegers(property);
      std::vector<std::uint64_t> indexes;

      switch(operation) {
      case Operation::EQ:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x == typedValue; });
        break;
      case Operation::NEQ:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x != typedValue; });
        break;
      case Operation::GT:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x > typedValue; });
        break;
      case Operation::GTE:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x >= typedValue; });
        break;
      case Operation::LT:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x < typedValue; });
        break;
      case Operation::LTE:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x <= typedValue; });
        break;
      default:
        break;
      }

      std::erase_if(indexes, [blank](uint64_t i) { return blank.contains(i); });

      for(auto idx : indexes) {
        if(current++ > skip) {
          nodes.emplace_back(getNode(type_id, idx));
        }
        if (current > (skip + limit)) {
          return nodes;
        }
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::findDoubleNodes(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    // Handle values that are parsed as Integers (230 vs 230.0)
    if (Properties::isIntegerProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const double typedValue = static_cast<double>(get<int64_t>(value));
      const std::vector<double> &vec = properties[type_id].getDoubles(property);
      std::vector<std::uint64_t> indexes;

      switch(operation) {
      case Operation::EQ:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x == typedValue; });
        break;
      case Operation::NEQ:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x != typedValue; });
        break;
      case Operation::GT:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x > typedValue; });
        break;
      case Operation::GTE:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x >= typedValue; });
        break;
      case Operation::LT:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x < typedValue; });
        break;
      case Operation::LTE:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x <= typedValue; });
        break;
      default:
        break;
      }

      std::erase_if(indexes, [blank](uint64_t i) { return blank.contains(i); });

      for(auto idx : indexes) {
        if(current++ > skip) {
          nodes.emplace_back(getNode(type_id, idx));
        }
        if (current > (skip + limit)) {
          return nodes;
        }
      }
      return nodes;
    }

    if (Properties::isDoubleProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const double typedValue = get<double>(value);
      const std::vector<double> &vec = properties[type_id].getDoubles(property);
      std::vector<std::uint64_t> indexes;

      switch(operation) {
      case Operation::EQ:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x == typedValue; });
        break;
      case Operation::NEQ:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x != typedValue; });
        break;
      case Operation::GT:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x > typedValue; });
        break;
      case Operation::GTE:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x >= typedValue; });
        break;
      case Operation::LT:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x < typedValue; });
        break;
      case Operation::LTE:
        indexes = ragedb::collect_indexes(vec.begin(), vec.end(), [typedValue](auto x) { return x <= typedValue; });
        break;
      default:
        break;
      }

      std::erase_if(indexes, [blank](uint64_t i) { return blank.contains(i); });

      for(auto idx : indexes) {
        if(current++ > skip) {
          nodes.emplace_back(getNode(type_id, idx));
        }
        if (current > (skip + limit)) {
          return nodes;
        }
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::findStringNodes(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isStringProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::string typedValue = get<std::string>(value);
      const std::vector<std::string> &vec = properties[type_id].getStrings(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }
        // If the node or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateString(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(type_id, internal_id));
        }
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::findBooleanListNodes(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<bool> typedValue = get<std::vector<bool>>(value);
      const std::vector<std::vector<bool>> &vec = properties[type_id].getBooleansList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }
        // If the node or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<bool>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(type_id, internal_id));
        }
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::findIntegerListNodes(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isIntegerListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<int64_t> typedValue = get<std::vector<int64_t>>(value);
      const std::vector<std::vector<int64_t>> &vec = properties[type_id].getIntegersList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }
        // If the node or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<int64_t>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(type_id, internal_id));
        }
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::findDoubleListNodes(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;

    if (Properties::isIntegerListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      std::vector<int64_t> integerTypedValue = get<std::vector<int64_t>>(value);
      const std::vector<double> typedValue(integerTypedValue.begin(), integerTypedValue.end());
      const std::vector<std::vector<double>> &vec = properties[type_id].getDoublesList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }
        // If the node or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<double>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(type_id, internal_id));
        }

        current++;
      }
      return nodes;
    }
    if (Properties::isDoubleListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<double> typedValue = get<std::vector<double>>(value);
      const std::vector<std::vector<double>> &vec = properties[type_id].getDoublesList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }
        // If the node or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<double>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(type_id, internal_id));
        }
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::findStringListNodes(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isStringListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<std::string> typedValue = get<std::vector<std::string>>(value);
      const std::vector<std::vector<std::string>> &vec = properties[type_id].getStringsList(property);
      for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
        // If we reached our limit, return
        if (current > (skip + limit)) {
          return nodes;
        }
        // If the node or property has been deleted, ignore it
        if (blank.contains(internal_id)) {
          continue;
        }
        if (!Expression::EvaluateVector<std::string>(operation, vec[internal_id], typedValue)) {
          continue;
        }
        // If it is true add it if we are over the skip, otherwise ignore it
        if (current > skip) {
          nodes.emplace_back(getNode(type_id, internal_id));
        }
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::findNodes(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit) {
    // If the type is invalid, we can't find any so return an empty array
    if (!ValidTypeId(type_id)) return std::vector<Node>();

    if(operation == Operation::IS_NULL) {
      return findNullNodes(type_id, property, skip, limit);
    }

    if(operation == Operation::NOT_IS_NULL) {
      return findNotNullNodes(type_id, property, skip, limit);
    }

    const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);

    switch (property_type_id) {
      case Properties::boolean_type:
        return findBooleanNodes(type_id, property, operation, value, skip, limit);
      case Properties::integer_type:
        return findIntegerNodes(type_id, property, operation, value, skip, limit);
      case Properties::double_type:
        return findDoubleNodes(type_id, property, operation, value, skip, limit);
      case Properties::date_type:
        // TODO: Verify this
        return findDoubleNodes(type_id, property, operation, value, skip, limit);
      case Properties::string_type:
        return findStringNodes(type_id, property, operation, value, skip, limit);
      case Properties::boolean_list_type:
        return findBooleanListNodes(type_id, property, operation, value, skip, limit);
      case Properties::integer_list_type:
        return findIntegerListNodes(type_id, property, operation, value, skip, limit);
      case Properties::double_list_type:
        return findDoubleListNodes(type_id, property, operation, value, skip, limit);
      case Properties::date_list_type:
        // TODO: Verify this
        return findDoubleListNodes(type_id, property, operation, value, skip, limit);
      case Properties::string_list_type:
        return findStringListNodes(type_id, property, operation, value, skip, limit);
      default:
        return std::vector<Node>();
    }
  }

  }