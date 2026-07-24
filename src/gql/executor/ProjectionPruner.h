/*
 * Copyright RageDB Contributors. All Rights Reserved.
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

#ifndef RAGEDB_PROJECTION_PRUNER_H
#define RAGEDB_PROJECTION_PRUNER_H

#include <string>
#include <set>
#include <map>
#include "GqlAst.h"

namespace ragedb::gql {

/**
 * @brief ProjectionPruner tracks properties referenced in expressions for Node/Relationship property pruning.
 * 
 * Example Query:
 *   MATCH (p:Person)-[:FRIEND]->(f) RETURN p.name, f
 *   - "p" properties are pruned down to {"name"}.
 *   - "f" is used as a whole object (no pruning).
 */
struct ProjectionPruner {
    std::map<std::string, std::set<std::string>> accessed_props;
    std::set<std::string> whole_objects;

    bool should_prune(const std::string& var) const {
        if (var.empty()) return false;
        // A node/edge the user wrote anonymously is given an internal name (suffix "_user_empty") so its
        // bindings do not collide. That name never appears in the user query the pruner analysed, so it is
        // absent from accessed_props and whole_objects: pruning it would strip every property with an empty
        // keep-set -- including the ones the node's own inline `{prop: v}` filter still needs to match. An
        // anonymous element is never projected, so keeping its properties is always safe.
        if (is_anonymous_generated(var)) return false;
        if (whole_objects.count(var)) return false;
        return true;
    }

    static bool is_anonymous_generated(const std::string& var) {
        static const std::string suffix = "_user_empty";
        return var.size() >= suffix.size() &&
               var.compare(var.size() - suffix.size(), suffix.size(), suffix) == 0;
    }

    const std::set<std::string>& get_keys(const std::string& var) const {
        static const std::set<std::string> empty_keys;
        auto it = accessed_props.find(var);
        if (it != accessed_props.end()) {
            return it->second;
        }
        return empty_keys;
    }
};

/**
 * @brief Statically analyzes an expression to collect which variables and properties are accessed.
 */
void collect_accessed_properties(
    const Expression* expr,
    std::map<std::string, std::set<std::string>>& accessed_props,
    std::set<std::string>& whole_objects
);

} // namespace ragedb::gql

#endif // RAGEDB_PROJECTION_PRUNER_H
