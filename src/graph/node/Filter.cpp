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
      std::erase_if(list, [deleted = deleted_ids[type_id] ](auto id) {
          return deleted.contains(Shard::externalToInternal(id));
      });
  }

  void NodeTypes::removeDeletedProperties(uint16_t type_id, std::vector<uint64_t> &list, const std::string &property) {
      std::erase_if(list, [deleted = properties[type_id].getDeletedMap(property) ](auto id) {
          return deleted.contains(Shard::externalToInternal(id));
      });
  }

  uint64_t NodeTypes::filterCountBooleans(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value) {
    uint64_t count = 0;
    if (Properties::isBooleanProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const bool typedValue = get<bool>(value);
      for (const auto& id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains(Shard::externalToInternal(id))) {
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

  uint64_t NodeTypes::filterCountIntegers(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value) {
    uint64_t count = 0;
    if (Properties::isIntegerProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const int64_t typedValue = get<int64_t>(value);
      for (const auto& id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains(Shard::externalToInternal(id))) {
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

  uint64_t NodeTypes::filterCountDoubles(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value) {
    uint64_t count = 0;
    if (Properties::isDoubleProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const double typedValue = get<double>(value);
      for (const auto& id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains(Shard::externalToInternal(id))) {
          continue;
        }
        if (!Expression::Evaluate<double>(operation, properties[type_id].getDoubleProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        count++;
      }
      return count;
    }
    if (Properties::isIntegerProperty(value)) {
        const double typedValue = static_cast<double>(get<int64_t>(value));
        const roaring::Roaring64Map blank = getBlanks(type_id, property);
        for (const auto& id : ids) {
            // If the node or property has been deleted, ignore it
            if (blank.contains(Shard::externalToInternal(id))) {
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

  uint64_t NodeTypes::filterCountStrings(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value) {
    uint64_t count = 0;
    if (Properties::isStringProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::string typedValue = get<std::string>(value);
      for (const auto& id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains(Shard::externalToInternal(id))) {
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

  uint64_t NodeTypes::filterCountBooleanLists(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value) {
    uint64_t count = 0;
    if (Properties::isBooleanListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<bool> typedValue = get<std::vector<bool>>(value);
      for (const auto& id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains(Shard::externalToInternal(id))) {
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

  uint64_t NodeTypes::filterCountIntegerLists(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value) {
    uint64_t count = 0;
    if (Properties::isIntegerListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<int64_t> typedValue = get<std::vector<int64_t>>(value);
      for (const auto& id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains(Shard::externalToInternal(id))) {
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

  uint64_t NodeTypes::filterCountDoubleLists(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value) {
    uint64_t count = 0;
    if (Properties::isDoubleListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<double> typedValue = get<std::vector<double>>(value);
      for (const auto& id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains(Shard::externalToInternal(id))) {
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

  uint64_t NodeTypes::filterCountStringLists(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value) {
    uint64_t count = 0;
    if (Properties::isStringListProperty(value)) {
      const roaring::Roaring64Map blank = getBlanks(type_id, property);
      const std::vector<std::string> typedValue = get<std::vector<std::string>>(value);
      for (const auto& id : ids) {
        // If the node or property has been deleted, ignore it
        if (blank.contains(Shard::externalToInternal(id))) {
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

  uint64_t NodeTypes::filterCount(const std::vector<uint64_t>& unfiltered, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value) {
    // If the type is invalid, we can't filter any so return zero
    if (!ValidTypeId(type_id)) return 0;

    if(operation == Operation::IS_NULL) {
      uint64_t count = 0;
      for (const auto& id : unfiltered) {
        count += static_cast<uint8_t>(properties[type_id].isDeleted(property, id));
      }
      return count;
    }

    if(operation == Operation::NOT_IS_NULL) {
      uint64_t count = 0;
      for (const auto& id : unfiltered) {
        count += (1 - static_cast<uint8_t>(properties[type_id].isDeleted(property, id)));
      }
      return count;
    }

    const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);

    switch (property_type_id) {
      case Properties::boolean_type:
        return filterCountBooleans(unfiltered, type_id, property, operation, value);
      case Properties::integer_type:
        return filterCountIntegers(unfiltered, type_id, property, operation, value);
      case Properties::double_type:
        return filterCountDoubles(unfiltered, type_id, property, operation, value);
      case Properties::date_type:
        return filterCountDoubles(unfiltered, type_id, property, operation, value);
      case Properties::string_type:
        return filterCountStrings(unfiltered, type_id, property, operation, value);
      case Properties::boolean_list_type:
        return filterCountBooleanLists(unfiltered, type_id, property, operation, value);
      case Properties::integer_list_type:
        return filterCountIntegerLists(unfiltered, type_id, property, operation, value);
      case Properties::double_list_type:
        return filterCountDoubleLists(unfiltered, type_id, property, operation, value);
      case Properties::date_list_type:
        return filterCountDoubleLists(unfiltered, type_id, property, operation, value);
      case Properties::string_list_type:
        return filterCountStringLists(unfiltered, type_id, property, operation, value);
      default: {
        return 0;
      }
    }
  }

  std::vector<uint64_t> NodeTypes::filterNullIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, uint64_t limit) {
   std::vector<uint64_t> ids;
    uint64_t current = 1;
    for (const auto& id : list) {
      if (current > limit) {
        break;
      }

      if (properties[type_id].isDeleted(property, Shard::externalToInternal(id))) {
          current++;
          ids.emplace_back(id);
      }
    }
    return ids;
  }

  std::vector<uint64_t> NodeTypes::filterNotNullIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, uint64_t limit, Sort sortOrder) {
   std::vector<uint64_t> ids;
   uint64_t current = 1;
   if (sortOrder == Sort::NONE) {
       for (const auto& id : list) {
           if (current > limit) {
               break;
           }

           if (!properties[type_id].isDeleted(property, Shard::externalToInternal(id))) {
               current++;
               ids.emplace_back(id);
           }
       }
   } else {
       std::vector<std::pair<uint64_t, property_type_t>> vec;
       for (const auto& id : list) {
           if (!properties[type_id].isDeleted(property, Shard::externalToInternal(id))) {
               vec.emplace_back(id, getNodeProperty(id, property));
           }
       }

       // Partial sort up to our limit
       if (sortOrder == Sort::ASC) {
           std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t , property_type_t> &a, const std::pair<uint64_t , property_type_t> &b) -> bool {
               return a.second < b.second;
           });
       } else {
           std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t , property_type_t> &a, const std::pair<uint64_t , property_type_t> &b) -> bool {
               return a.second > b.second;
           });
       }
       for (const auto& id : vec) {
           if (current++ > limit) {
               return ids;
           }
           ids.emplace_back(id.first);
       }
       return ids;
   }
   return ids;
  }

  std::vector<uint64_t> NodeTypes::filterBooleanIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {
   std::vector<uint64_t> ids;

   int current = 1;
   bool typedValue;
   if (Properties::isBooleanProperty(value)) {
       typedValue = get<bool>(value);
   } else {
       return ids;
   }
   if (sortOrder == Sort::NONE) {
       for (const auto& id : list) {
           // If we reached our limit, return
           if (current > limit) {
               return ids;
           }

           if (!Expression::Evaluate<bool>(operation, properties[type_id].getBooleanProperty(property,  Shard::externalToInternal(id)), typedValue)) {
               continue;
           }

           ids.emplace_back(id);
           current++;
       }
   } else {
       std::vector<uint64_t> internal_ids;
       internal_ids.reserve(list.size());
       for (const auto& id : list) {
           internal_ids.emplace_back(Shard::externalToInternal(id));
       }

       std::function<bool(property_type_t)> filter = [&](property_type_t property_value){ return Expression::Evaluate<bool>(operation, get<bool>(property_value), typedValue); };

       // Declare vector of pairs
       std::vector<std::pair<uint64_t, property_type_t>> vec = properties[type_id].getProperty(list, internal_ids, property, filter);

       // Partial sort up to our limit
       if (sortOrder == Sort::ASC) {
           std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
               return a.second < b.second;
           });
       } else {
           std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
               return a.second > b.second;
           });
       }
       // Grab the properties up to the limit
       for (const auto &[key, property_value] : vec) {
           if (current++ > limit) {
               return ids;
           }
           ids.push_back(key);
       }
   }
   return ids;
  }
  
  std::vector<uint64_t> NodeTypes::filterIntegerIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {
   std::vector<uint64_t> ids;
   int current = 1;
   int64_t typedValue;
   if (Properties::isIntegerProperty(value)) {
       typedValue = get<int64_t>(value);
   } else {
       return ids;
   }

   if (sortOrder == Sort::NONE) {
       for (const auto& id : list) {
           // If we reached our limit, return
           if (current > limit) {
               return ids;
           }

           if (!Expression::Evaluate<int64_t>(operation, properties[type_id].getIntegerProperty(property,  Shard::externalToInternal(id)), typedValue)) {
               continue;
           }

           ids.emplace_back(id);
           current++;
       }
   } else {
       std::vector<uint64_t> internal_ids;
       internal_ids.reserve(list.size());
       for (const auto& id : list) {
           internal_ids.emplace_back(Shard::externalToInternal(id));
       }

       std::function<bool(property_type_t)> filter = [&](property_type_t property_value){ return Expression::Evaluate<int64_t>(operation, get<int64_t>(property_value), typedValue); };

       // Declare vector of pairs
       std::vector<std::pair<uint64_t, property_type_t>> vec = properties[type_id].getProperty(list, internal_ids, property, filter);

       // Partial sort up to our limit
       if (sortOrder == Sort::ASC) {
           std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
               return a.second < b.second;
           });
       } else {
           std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
               return a.second > b.second;
           });
       }
       // Grab the properties up to the limit
       for (const auto &[key, property_value] : vec) {
           if (current++ > limit) {
               return ids;
           }
           ids.push_back(key);
       }
   }
   return ids;
  }
  
  std::vector<uint64_t> NodeTypes::filterDoubleIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {
   std::vector<uint64_t> ids;
   int current = 1;
   double typedValue;
   if (Properties::isDoubleProperty(value)) {
       typedValue = get<double>(value);
   } else {
       if (Properties::isIntegerProperty(value)) {
           typedValue = static_cast<double>(get<int64_t>(value));
       } else {
           return ids;
       }
   }

   if (sortOrder == Sort::NONE) {
       for (const auto& id : list) {
           // If we reached our limit, return
           if (current > limit) {
               return ids;
           }

           if (!Expression::Evaluate<double>(operation, properties[type_id].getDoubleProperty(property,  Shard::externalToInternal(id)), typedValue)) {
               continue;
           }

           ids.emplace_back(id);
           current++;
       }
   } else {
       std::vector<uint64_t> internal_ids;
       internal_ids.reserve(list.size());
       for (const auto& id : list) {
           internal_ids.emplace_back(Shard::externalToInternal(id));
       }

       std::function<bool(property_type_t)> filter = [&](property_type_t property_value){ return Expression::Evaluate<double>(operation, get<double>(property_value), typedValue); };

       // Declare vector of pairs
       std::vector<std::pair<uint64_t, property_type_t>> vec = properties[type_id].getProperty(list, internal_ids, property, filter);

       // Partial sort up to our limit
       if (sortOrder == Sort::ASC) {
           std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
               return a.second < b.second;
           });
       } else {
           std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
               return a.second > b.second;
           });
       }
       // Grab the properties up to the limit
       for (const auto &[key, property_value] : vec) {
           if (current++ > limit) {
               return ids;
           }
           ids.push_back(key);
       }
   }
   return ids;
  }
  
  std::vector<uint64_t> NodeTypes::filterStringIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {
   std::vector<uint64_t> ids;
   int current = 1;
   std::string typedValue;
   if (Properties::isStringProperty(value)) {
       typedValue = get<std::string>(value);
   } else {
       return ids;
   }

   if (sortOrder == Sort::NONE) {
       for (const auto& id : list) {
           // If we reached our limit, return
           if (current > limit) {
               return ids;
           }

           if (!Expression::Evaluate<std::string>(operation, properties[type_id].getStringProperty(property,  Shard::externalToInternal(id)), typedValue)) {
               continue;
           }

           ids.emplace_back(id);
           current++;
       }
   } else {
       std::vector<uint64_t> internal_ids;
       internal_ids.reserve(list.size());
       for (const auto& id : list) {
           internal_ids.emplace_back(Shard::externalToInternal(id));
       }

       std::function<bool(property_type_t)> filter = [&](property_type_t property_value){ return Expression::Evaluate<std::string>(operation, get<std::string>(property_value), typedValue); };

       // Declare vector of pairs
       std::vector<std::pair<uint64_t, property_type_t>> vec = properties[type_id].getProperty(list, internal_ids, property, filter);

       // Partial sort up to our limit
       if (sortOrder == Sort::ASC) {
           std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
               return a.second < b.second;
           });
       } else {
           std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
               return a.second > b.second;
           });
       }
       // Grab the properties up to the limit
       for (const auto &[key, property_value] : vec) {
           if (current++ > limit) {
               return ids;
           }
           ids.push_back(key);
       }
   }
   return ids;
  }
  
  std::vector<uint64_t> NodeTypes::filterBooleanListIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit) {
   std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<bool> typedValue = get<std::vector<bool>>(value);
      for (const auto& id : list) {
        // If we reached our limit, return
        if (current > limit) {
          return ids;
        }
        if (!Expression::Evaluate<std::vector<bool>>(operation, properties[type_id].getListOfBooleanProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        ids.emplace_back(id);
        current++;
      }
    }
    return ids;
  }
  
  std::vector<uint64_t> NodeTypes::filterIntegerListIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit) {
   std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<int64_t> typedValue = get<std::vector<int64_t>>(value);
      for (const auto& id : list) {
        // If we reached our limit, return
        if (current > limit) {
          return ids;
        }
        if (!Expression::Evaluate<std::vector<int64_t>>(operation, properties[type_id].getListOfIntegerProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        ids.emplace_back(id);
        current++;
      }
    }
    return ids;
  }
  
  std::vector<uint64_t> NodeTypes::filterDoubleListIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit) {
   std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<double> typedValue = get<std::vector<double>>(value);
      for (const auto& id : list) {
        // If we reached our limit, return
        if (current > limit) {
          return ids;
        }
        if (!Expression::Evaluate<std::vector<double>>(operation, properties[type_id].getListOfDoubleProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        ids.emplace_back(id);
        current++;
      }
    }
    return ids;
  }
  
  std::vector<uint64_t> NodeTypes::filterStringListIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit) {
   std::vector<uint64_t> ids;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<std::string> typedValue = get<std::vector<std::string>>(value);
      for (const auto& id : list) {
        // If we reached our limit, return
        if (current > limit) {
          return ids;
        }
        if (!Expression::Evaluate<std::vector<std::string>>(operation, properties[type_id].getListOfStringProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        ids.emplace_back(id);
        current++;
      }
    }
    return ids;
  }

  std::vector<uint64_t> NodeTypes::filterIds(std::vector<uint64_t> unfiltered, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {
    // If the type is invalid, we can't filter any so return an empty array
    if (!ValidTypeId(type_id)) return std::vector<uint64_t>();

    // Deleted Nodes will never return from a filter
    removeDeletedIds(type_id, unfiltered);

    if(operation == Operation::IS_NULL) {
      return filterNullIds(unfiltered, type_id, property, limit);
    }

    if(operation == Operation::NOT_IS_NULL) {
      return filterNotNullIds(unfiltered, type_id, property, limit, sortOrder);
    }

    // From here on out we don't want deleted properties
    removeDeletedProperties(type_id, unfiltered, property);

    const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);
    switch (property_type_id) {
      case Properties::boolean_type:
        return filterBooleanIds(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::integer_type:
        return filterIntegerIds(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::double_type:
        return filterDoubleIds(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::date_type:
        return filterDoubleIds(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::string_type:
        return filterStringIds(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::boolean_list_type:
        return filterBooleanListIds(unfiltered, type_id, property, operation, value, limit);
      case Properties::integer_list_type:
        return filterIntegerListIds(unfiltered, type_id, property, operation, value, limit);
      case Properties::double_list_type:
        return filterDoubleListIds(unfiltered, type_id, property, operation, value, limit);
      case Properties::date_list_type:
        return filterDoubleListIds(unfiltered, type_id, property, operation, value, limit);
      case Properties::string_list_type:
        return filterStringListIds(unfiltered, type_id, property, operation, value, limit);
      default:
        return std::vector<uint64_t>();
    }
  }

  std::vector<Node> NodeTypes::filterNullNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, uint64_t limit) {
    std::vector<Node> nodes;
    uint64_t current = 1;
    for (const auto& id : list) {
      if (current > limit) {
        break;
      }

      if (properties[type_id].isDeleted(property, Shard::externalToInternal(id))) {
          current++;
          nodes.emplace_back(getNode(id));
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterNotNullNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, uint64_t limit, Sort sortOrder) {
    std::vector<Node> nodes;
    uint64_t current = 1;
    if (sortOrder == Sort::NONE) {
        for (const auto& id : list) {
            if (current > limit) {
                break;
            }

            if (!properties[type_id].isDeleted(property, Shard::externalToInternal(id))) {
                current++;
                nodes.emplace_back(getNode(id));
            }
        }
    } else {
        std::vector<std::pair<uint64_t, property_type_t>> vec;
        for (const auto& id : list) {
            if (!properties[type_id].isDeleted(property, Shard::externalToInternal(id))) {
                vec.emplace_back(id, getNodeProperty(id, property));
            }
        }

        // Partial sort up to our limit
        if (sortOrder == Sort::ASC) {
            std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t , property_type_t> &a, const std::pair<uint64_t , property_type_t> &b) -> bool {
                return a.second < b.second;
            });
        } else {
            std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t , property_type_t> &a, const std::pair<uint64_t , property_type_t> &b) -> bool {
                return a.second > b.second;
            });
        }
        for (const auto& id : vec) {
            if (current++ > limit) {
                return nodes;
            }
            nodes.emplace_back(getNode(id.first));
        }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterBooleanNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {
    std::vector<Node> nodes;
    int current = 1;
    bool typedValue;
    if (Properties::isBooleanProperty(value)) {
        typedValue = get<bool>(value);
    } else {
        return nodes;
    }
    if (sortOrder == Sort::NONE) {
        for (const auto& id : list) {
            // If we reached our limit, return
            if (current > limit) {
                return nodes;
            }

            if (!Expression::Evaluate<bool>(operation, properties[type_id].getBooleanProperty(property,  Shard::externalToInternal(id)), typedValue)) {
                continue;
            }

            nodes.emplace_back(getNode(id));
            current++;
        }
    } else {
        std::vector<uint64_t> internal_ids;
        internal_ids.reserve(list.size());
        for (const auto& id : list) {
            internal_ids.emplace_back(Shard::externalToInternal(id));
        }

        std::function<bool(property_type_t)> filter = [&](property_type_t property_value){ return Expression::Evaluate<bool>(operation, get<bool>(property_value), typedValue); };

        // Declare vector of pairs
        std::vector<std::pair<uint64_t, property_type_t>> vec = properties[type_id].getProperty(list, internal_ids, property, filter);

        // Partial sort up to our limit
        if (sortOrder == Sort::ASC) {
            std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                return a.second < b.second;
            });
        } else {
            std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                return a.second > b.second;
            });
        }
        // Grab the properties up to the limit
        for (const auto &[key, property_value] : vec) {
            if (current++ > limit) {
                return nodes;
            }
            nodes.push_back(getNode(key));
        }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterIntegerNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {
    std::vector<Node> nodes;
    int current = 1;
    int64_t typedValue;
    if (Properties::isIntegerProperty(value)) {
        typedValue = get<int64_t>(value);
    } else {
        return nodes;
    }

    if (sortOrder == Sort::NONE) {
        for (const auto& id : list) {
            // If we reached our limit, return
            if (current > limit) {
                return nodes;
            }

            if (!Expression::Evaluate<int64_t>(operation, properties[type_id].getIntegerProperty(property,  Shard::externalToInternal(id)), typedValue)) {
                continue;
            }

            nodes.emplace_back(getNode(id));
            current++;
        }
    } else {
        std::vector<uint64_t> internal_ids;
        internal_ids.reserve(list.size());
        for (const auto& id : list) {
            internal_ids.emplace_back(Shard::externalToInternal(id));
        }

        std::function<bool(property_type_t)> filter = [&](property_type_t property_value){ return Expression::Evaluate<int64_t>(operation, get<int64_t>(property_value), typedValue); };

        // Declare vector of pairs
        std::vector<std::pair<uint64_t, property_type_t>> vec = properties[type_id].getProperty(list, internal_ids, property, filter);

        // Partial sort up to our limit
        if (sortOrder == Sort::ASC) {
            std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                return a.second < b.second;
            });
        } else {
            std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                return a.second > b.second;
            });
        }
        // Grab the properties up to the limit
        for (const auto &[key, property_value] : vec) {
            if (current++ > limit) {
                return nodes;
            }
            nodes.push_back(getNode(key));
        }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterDoubleNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {
    std::vector<Node> nodes;
    int current = 1;
    double typedValue;
    if (Properties::isDoubleProperty(value)) {
        typedValue = get<double>(value);
    } else {
        if (Properties::isIntegerProperty(value)) {
            typedValue = static_cast<double>(get<int64_t>(value));
        } else {
            return nodes;
        }
    }

    if (sortOrder == Sort::NONE) {
        for (const auto& id : list) {
            // If we reached our limit, return
            if (current > limit) {
                return nodes;
            }

            if (!Expression::Evaluate<double>(operation, properties[type_id].getDoubleProperty(property,  Shard::externalToInternal(id)), typedValue)) {
                continue;
            }

            nodes.emplace_back(getNode(id));
            current++;
        }
    } else {
        std::vector<uint64_t> internal_ids;
        internal_ids.reserve(list.size());
        for (const auto& id : list) {
            internal_ids.emplace_back(Shard::externalToInternal(id));
        }

        std::function<bool(property_type_t)> filter = [&](property_type_t property_value){ return Expression::Evaluate<double>(operation, get<double>(property_value), typedValue); };

        // Declare vector of pairs
        std::vector<std::pair<uint64_t, property_type_t>> vec = properties[type_id].getProperty(list, internal_ids, property, filter);

        // Partial sort up to our limit
        if (sortOrder == Sort::ASC) {
            std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                return a.second < b.second;
            });
        } else {
            std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                return a.second > b.second;
            });
        }
        // Grab the properties up to the limit
        for (const auto &[key, property_value] : vec) {
            if (current++ > limit) {
                return nodes;
            }
            nodes.push_back(getNode(key));
        }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterStringNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {
    std::vector<Node> nodes;
    int current = 1;
    std::string typedValue;
    if (Properties::isStringProperty(value)) {
        typedValue = get<std::string>(value);
    } else {
        return nodes;
    }

    if (sortOrder == Sort::NONE) {
        for (const auto& id : list) {
            // If we reached our limit, return
            if (current > limit) {
                return nodes;
            }

            if (!Expression::Evaluate<std::string>(operation, properties[type_id].getStringProperty(property,  Shard::externalToInternal(id)), typedValue)) {
                continue;
            }

            nodes.emplace_back(getNode(id));
            current++;
        }
    } else {
        std::vector<uint64_t> internal_ids;
        internal_ids.reserve(list.size());
        for (const auto& id : list) {
            internal_ids.emplace_back(Shard::externalToInternal(id));
        }

        std::function<bool(property_type_t)> filter = [&](property_type_t property_value){ return Expression::Evaluate<std::string>(operation, get<std::string>(property_value), typedValue); };

        // Declare vector of pairs
        std::vector<std::pair<uint64_t, property_type_t>> vec = properties[type_id].getProperty(list, internal_ids, property, filter);

        // Partial sort up to our limit
        if (sortOrder == Sort::ASC) {
            std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                return a.second < b.second;
            });
        } else {
            std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                return a.second > b.second;
            });
        }
        // Grab the properties up to the limit
        for (const auto &[key, property_value] : vec) {
            if (current++ > limit) {
                return nodes;
            }
            nodes.push_back(getNode(key));
        }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterBooleanListNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<bool> typedValue = get<std::vector<bool>>(value);
      for (const auto& id : list) {
        // If we reached our limit, return
        if (current > limit) {
          return nodes;
        }
        if (!Expression::Evaluate<std::vector<bool>>(operation, properties[type_id].getListOfBooleanProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        nodes.emplace_back(getNode(id));
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterIntegerListNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<int64_t> typedValue = get<std::vector<int64_t>>(value);
      for (const auto& id : list) {
        // If we reached our limit, return
        if (current > limit) {
          return nodes;
        }
        if (!Expression::Evaluate<std::vector<int64_t>>(operation, properties[type_id].getListOfIntegerProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        nodes.emplace_back(getNode(id));
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterDoubleListNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<double> typedValue = get<std::vector<double>>(value);
      for (const auto& id : list) {
        // If we reached our limit, return
        if (current > limit) {
          return nodes;
        }
        if (!Expression::Evaluate<std::vector<double>>(operation, properties[type_id].getListOfDoubleProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        nodes.emplace_back(getNode(id));
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterStringListNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit) {
    std::vector<Node> nodes;
    int current = 1;
    if (Properties::isBooleanListProperty(value)) {
      const std::vector<std::string> typedValue = get<std::vector<std::string>>(value);
      for (const auto& id : list) {
        // If we reached our limit, return
        if (current > limit) {
          return nodes;
        }
        if (!Expression::Evaluate<std::vector<std::string>>(operation, properties[type_id].getListOfStringProperty(property,  Shard::externalToInternal(id)), typedValue)) {
          continue;
        }
        nodes.emplace_back(getNode(id));
        current++;
      }
    }
    return nodes;
  }

  std::vector<Node> NodeTypes::filterNodes(std::vector<uint64_t> unfiltered, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {

    // If the type is invalid, we can't filter any so return an empty array
    if (!ValidTypeId(type_id)) return std::vector<Node>();

    // Deleted Nodes will never return from a filter
    removeDeletedIds(type_id, unfiltered);

    if(operation == Operation::IS_NULL) {
      return filterNullNodes(unfiltered, type_id, property, limit);
    }

    if(operation == Operation::NOT_IS_NULL) {
      return filterNotNullNodes(unfiltered, type_id, property, limit, sortOrder);
    }

    const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);
    switch (property_type_id) {
      case Properties::boolean_type:
        return filterBooleanNodes(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::integer_type:
        return filterIntegerNodes(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::double_type:
        return filterDoubleNodes(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::date_type:
        return filterDoubleNodes(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::string_type:
        return filterStringNodes(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::boolean_list_type:
        return filterBooleanListNodes(unfiltered, type_id, property, operation, value, limit);
      case Properties::integer_list_type:
        return filterIntegerListNodes(unfiltered, type_id, property, operation, value, limit);
      case Properties::double_list_type:
        return filterDoubleListNodes(unfiltered, type_id, property, operation, value, limit);
      case Properties::date_list_type:
        return filterDoubleListNodes(unfiltered, type_id, property, operation, value, limit);
      case Properties::string_list_type:
        return filterStringListNodes(unfiltered, type_id, property, operation, value, limit);
      default:
        return std::vector<Node>();
    }
  }

  std::vector<std::map<std::string, property_type_t>> NodeTypes::filterNullNodeProperties(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, uint64_t limit) {
      std::vector<std::map<std::string, property_type_t>> nodes;
      uint64_t current = 1;
      for (const auto& id : list) {
          if (current > limit) {
              break;
          }

          if (properties[type_id].isDeleted(property, Shard::externalToInternal(id))) {
              current++;
              nodes.emplace_back(getNodeProperties(id));
          }
      }
      return nodes;
  }

  std::vector<std::map<std::string, property_type_t>> NodeTypes::filterNotNullNodeProperties(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, uint64_t limit, Sort sortOrder) {
      std::vector<std::map<std::string, property_type_t>> nodes;
      uint64_t current = 1;
      if (sortOrder == Sort::NONE) {
          for (const auto& id : list) {
              if (current > limit) {
                 break;
              }

              if (!properties[type_id].isDeleted(property, Shard::externalToInternal(id))) {
                  current++;
                  nodes.emplace_back(getNodeProperties(id));
              }
          }
      } else {
          std::vector<std::pair<uint64_t, property_type_t>> vec;
          for (const auto& id : list) {
              if (!properties[type_id].isDeleted(property, Shard::externalToInternal(id))) {
                  vec.emplace_back(id, getNodeProperty(id, property));
              }
          }

          // Partial sort up to our limit
          if (sortOrder == Sort::ASC) {
              std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t , property_type_t> &a, const std::pair<uint64_t , property_type_t> &b) -> bool {
                  return a.second < b.second;
              });
          } else {
              std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t , property_type_t> &a, const std::pair<uint64_t , property_type_t> &b) -> bool {
                  return a.second > b.second;
              });
          }
          for (const auto& id : vec) {
              if (current++ > limit) {
                  return nodes;
              }
              nodes.emplace_back(getNodeProperties(id.first));
          }
      }
      return nodes;
  }

  std::vector<std::map<std::string, property_type_t>> NodeTypes::filterBooleanNodeProperties(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {
      std::vector<std::map<std::string, property_type_t>> nodes;
      int current = 1;
      bool typedValue;
      if (Properties::isBooleanProperty(value)) {
          typedValue = get<bool>(value);
      } else {
          return nodes;
      }
      if (sortOrder == Sort::NONE) {
          for (const auto& id : list) {
              // If we reached our limit, return
              if (current > limit) {
                  return nodes;
              }

              if (!Expression::Evaluate<bool>(operation, properties[type_id].getBooleanProperty(property,  Shard::externalToInternal(id)), typedValue)) {
                  continue;
              }

              nodes.emplace_back(getNodeProperties(id));
              current++;
          }
      } else {
          std::vector<uint64_t> internal_ids;
          internal_ids.reserve(list.size());
          for (const auto& id : list) {
              internal_ids.emplace_back(Shard::externalToInternal(id));
          }

          std::function<bool(property_type_t)> filter = [&](property_type_t property_value){ return Expression::Evaluate<bool>(operation, get<bool>(property_value), typedValue); };

          // Declare vector of pairs
          std::vector<std::pair<uint64_t, property_type_t>> vec = properties[type_id].getProperty(list, internal_ids, property, filter);

          // Partial sort up to our limit
          if (sortOrder == Sort::ASC) {
              std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                  return a.second < b.second;
              });
          } else {
              std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                  return a.second > b.second;
              });
          }
          // Grab the properties up to the limit
          for (const auto &[key, property_value] : vec) {
              if (current++ > limit) {
                  return nodes;
              }
              nodes.push_back(getNodeProperties(key));
          }
      }
      return nodes;
  }

  std::vector<std::map<std::string, property_type_t>> NodeTypes::filterIntegerNodeProperties(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {
      std::vector<std::map<std::string, property_type_t>> nodes;
      int current = 1;
      int64_t typedValue;
      if (Properties::isIntegerProperty(value)) {
          typedValue = get<int64_t>(value);
      } else {
          return nodes;
      }

      if (sortOrder == Sort::NONE) {
          for (const auto& id : list) {
              // If we reached our limit, return
              if (current > limit) {
                  return nodes;
              }

              if (!Expression::Evaluate<int64_t>(operation, properties[type_id].getIntegerProperty(property,  Shard::externalToInternal(id)), typedValue)) {
                  continue;
              }

              nodes.emplace_back(getNodeProperties(id));
              current++;
          }
      } else {
          std::vector<uint64_t> internal_ids;
          internal_ids.reserve(list.size());
          for (const auto& id : list) {
              internal_ids.emplace_back(Shard::externalToInternal(id));
          }

          std::function<bool(property_type_t)> filter = [&](property_type_t property_value){ return Expression::Evaluate<int64_t>(operation, get<int64_t>(property_value), typedValue); };

          // Declare vector of pairs
          std::vector<std::pair<uint64_t, property_type_t>> vec = properties[type_id].getProperty(list, internal_ids, property, filter);

          // Partial sort up to our limit
          if (sortOrder == Sort::ASC) {
              std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                  return a.second < b.second;
              });
          } else {
              std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                  return a.second > b.second;
              });
          }
          // Grab the properties up to the limit
          for (const auto &[key, property_value] : vec) {
              if (current++ > limit) {
                  return nodes;
              }
              nodes.push_back(getNodeProperties(key));
          }
      }
      return nodes;
  }

  std::vector<std::map<std::string, property_type_t>> NodeTypes::filterDoubleNodeProperties(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {
      std::vector<std::map<std::string, property_type_t>> nodes;
      int current = 1;
      double typedValue;
      if (Properties::isDoubleProperty(value)) {
          typedValue = get<double>(value);
      } else {
          if (Properties::isIntegerProperty(value)) {
          typedValue = static_cast<double>(get<int64_t>(value));
          } else {
              return nodes;
          }
      }

      if (sortOrder == Sort::NONE) {
          for (const auto& id : list) {
              // If we reached our limit, return
              if (current > limit) {
                  return nodes;
              }

              if (!Expression::Evaluate<double>(operation, properties[type_id].getDoubleProperty(property,  Shard::externalToInternal(id)), typedValue)) {
                  continue;
              }

              nodes.emplace_back(getNodeProperties(id));
              current++;
          }
      } else {
          std::vector<uint64_t> internal_ids = Shard::externalToInternal(list);
          sort(internal_ids.begin(), internal_ids.end());

          std::function<bool(property_type_t)> filter = [&](property_type_t property_value){ return Expression::Evaluate<double>(operation, get<double>(property_value), typedValue); };

          // Declare vector of pairs
          std::vector<std::pair<uint64_t, property_type_t>> vec = properties[type_id].getProperty(list, internal_ids, property, filter);

          // Partial sort up to our limit
          if (sortOrder == Sort::ASC) {
              std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                  return a.second < b.second;
              });
          } else {
              std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                  return a.second > b.second;
              });
          }
          // Grab the properties up to the limit
          for (const auto &[key, property_value] : vec) {
              if (current++ > limit) {
                  return nodes;
              }
              nodes.push_back(getNodeProperties(key));
          }
      }
      return nodes;
  }

  std::vector<std::map<std::string, property_type_t>> NodeTypes::filterStringNodeProperties(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {
      std::vector<std::map<std::string, property_type_t>> nodes;
      int current = 1;
      std::string typedValue;
      if (Properties::isStringProperty(value)) {
          typedValue = get<std::string>(value);
      } else {
          return nodes;
      }

      if (sortOrder == Sort::NONE) {
          for (const auto& id : list) {
              // If we reached our limit, return
              if (current > limit) {
                  return nodes;
              }

              if (!Expression::Evaluate<std::string>(operation, properties[type_id].getStringProperty(property,  Shard::externalToInternal(id)), typedValue)) {
                  continue;
              }

              nodes.emplace_back(getNodeProperties(id));
              current++;
          }
      } else {
          std::vector<uint64_t> internal_ids;
          internal_ids.reserve(list.size());
          for (const auto& id : list) {
              internal_ids.emplace_back(Shard::externalToInternal(id));
          }

          std::function<bool(property_type_t)> filter = [&](property_type_t property_value){ return Expression::Evaluate<std::string>(operation, get<std::string>(property_value), typedValue); };

          // Declare vector of pairs
          std::vector<std::pair<uint64_t, property_type_t>> vec = properties[type_id].getProperty(list, internal_ids, property, filter);

          // Partial sort up to our limit
          if (sortOrder == Sort::ASC) {
              std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                  return a.second < b.second;
              });
          } else {
              std::nth_element(vec.begin(), vec.begin() + limit, vec.end(), [](const std::pair<uint64_t, property_type_t> &a, const std::pair<uint64_t, property_type_t> &b) -> bool {
                  return a.second > b.second;
              });
          }
          // Grab the properties up to the limit
          for (const auto &[key, property_value] : vec) {
              if (current++ > limit) {
                  return nodes;
              }
              nodes.push_back(getNodeProperties(key));
          }
      }
      return nodes;
  }

  std::vector<std::map<std::string, property_type_t>> NodeTypes::filterBooleanListNodeProperties(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit) {
      std::vector<std::map<std::string, property_type_t>> nodes;
      int current = 1;
      if (Properties::isBooleanListProperty(value)) {
          const std::vector<bool> typedValue = get<std::vector<bool>>(value);
          for (const auto& id : list) {
              // If we reached our limit, return
              if (current > limit) {
                  return nodes;
              }
              if (!Expression::Evaluate<std::vector<bool>>(operation, properties[type_id].getListOfBooleanProperty(property,  Shard::externalToInternal(id)), typedValue)) {
                  continue;
              }
              nodes.emplace_back(getNodeProperties(id));
              current++;
          }
      }
      return nodes;
  }

  std::vector<std::map<std::string, property_type_t>> NodeTypes::filterIntegerListNodeProperties(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit) {
      std::vector<std::map<std::string, property_type_t>> nodes;
      int current = 1;
      if (Properties::isBooleanListProperty(value)) {
          const std::vector<int64_t> typedValue = get<std::vector<int64_t>>(value);
          for (const auto& id : list) {
              // If we reached our limit, return
              if (current > limit) {
                  return nodes;
              }
              if (!Expression::Evaluate<std::vector<int64_t>>(operation, properties[type_id].getListOfIntegerProperty(property,  Shard::externalToInternal(id)), typedValue)) {
                  continue;
              }
              nodes.emplace_back(getNodeProperties(id));
              current++;
          }
      }
      return nodes;
  }

  std::vector<std::map<std::string, property_type_t>> NodeTypes::filterDoubleListNodeProperties(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit) {
      std::vector<std::map<std::string, property_type_t>> nodes;
      int current = 1;
      if (Properties::isBooleanListProperty(value)) {
          const std::vector<double> typedValue = get<std::vector<double>>(value);
          for (const auto& id : list) {
              // If we reached our limit, return
              if (current > limit) {
                  return nodes;
              }
              if (!Expression::Evaluate<std::vector<double>>(operation, properties[type_id].getListOfDoubleProperty(property,  Shard::externalToInternal(id)), typedValue)) {
                  continue;
              }
              nodes.emplace_back(getNodeProperties(id));
              current++;
          }
      }
      return nodes;
  }

  std::vector<std::map<std::string, property_type_t>> NodeTypes::filterStringListNodeProperties(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit) {
      std::vector<std::map<std::string, property_type_t>> nodes;
      int current = 1;
      if (Properties::isBooleanListProperty(value)) {
          const std::vector<std::string> typedValue = get<std::vector<std::string>>(value);
          for (const auto& id : list) {
              // If we reached our limit, return
              if (current > limit) {
                  return nodes;
              }
              if (!Expression::Evaluate<std::vector<std::string>>(operation, properties[type_id].getListOfStringProperty(property,  Shard::externalToInternal(id)), typedValue)) {
                  continue;
              }
              nodes.emplace_back(getNodeProperties(id));
              current++;
          }
      }
      return nodes;
  }

  std::vector<std::map<std::string, property_type_t>> NodeTypes::filterNodeProperties(std::vector<uint64_t> unfiltered, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t limit, Sort sortOrder) {

      // If the type is invalid, we can't filter any so return an empty array
      if (!ValidTypeId(type_id)) return std::vector<std::map<std::string, property_type_t>>();

      // Deleted Nodes will never return from a filter
      removeDeletedIds(type_id, unfiltered);

      if(operation == Operation::IS_NULL) {
          return filterNullNodeProperties(unfiltered, type_id, property, limit);
      }

      if(operation == Operation::NOT_IS_NULL) {
          return filterNotNullNodeProperties(unfiltered, type_id, property, limit, sortOrder);
      }

      const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);
      switch (property_type_id) {
      case Properties::boolean_type:
          return filterBooleanNodeProperties(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::integer_type:
          return filterIntegerNodeProperties(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::double_type:
          return filterDoubleNodeProperties(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::date_type:
          return filterDoubleNodeProperties(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::string_type:
          return filterStringNodeProperties(unfiltered, type_id, property, operation, value, limit, sortOrder);
      case Properties::boolean_list_type:
          return filterBooleanListNodeProperties(unfiltered, type_id, property, operation, value, limit);
      case Properties::integer_list_type:
          return filterIntegerListNodeProperties(unfiltered, type_id, property, operation, value, limit);
      case Properties::double_list_type:
          return filterDoubleListNodeProperties(unfiltered, type_id, property, operation, value, limit);
      case Properties::date_list_type:
          return filterDoubleListNodeProperties(unfiltered, type_id, property, operation, value, limit);
      case Properties::string_list_type:
          return filterStringListNodeProperties(unfiltered, type_id, property, operation, value, limit);
      default:
          return std::vector<std::map<std::string, property_type_t>>();
      }
  }

}