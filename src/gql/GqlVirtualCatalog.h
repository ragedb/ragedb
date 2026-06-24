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

#ifndef RAGEDB_GQLVIRTUALCATALOG_H
#define RAGEDB_GQLVIRTUALCATALOG_H

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <optional>

namespace ragedb::gql {

/**
 * Thread-local metadata registry storing defined virtual views and constraints.
 * Enables query rewrite/expansion and write validation without database schema persistence.
 */
class GqlVirtualCatalog {
private:
    // Maps view names to their raw GQL query strings
    std::unordered_map<std::string, std::string> views;
    
    // Maps constraint names to their validation check query strings
    std::unordered_map<std::string, std::string> constraints;

    // Allowed relationship paths representing valid transitions: (SourceLabel, RelationshipType, TargetLabel)
    std::vector<std::tuple<std::string, std::string, std::string>> allowed_relationships;


public:
    // Returns the thread-local instance of the virtual catalog
    static GqlVirtualCatalog& local() {
        thread_local GqlVirtualCatalog instance;
        return instance;
    }

    // Adds/registers a new virtual view mapping
    void add_view(const std::string& name, const std::string& query) {
        views[name] = query;
    }

    // Removes/unregisters a virtual view mapping
    void remove_view(const std::string& name) {
        views.erase(name);
    }

    // Retrieves the raw GQL query string for a registered view name
    std::optional<std::string> get_view(const std::string& name) const {
        auto it = views.find(name);
        if (it != views.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    // Returns a reference to all registered views
    const std::unordered_map<std::string, std::string>& get_views() const {
        return views;
    }

    // Adds/registers a new validation constraint
    void add_constraint(const std::string& name, const std::string& query) {
        constraints[name] = query;
    }

    // Removes/unregisters a validation constraint
    void remove_constraint(const std::string& name) {
        constraints.erase(name);
    }

    // Retrieves the query string for a registered constraint name
    std::optional<std::string> get_constraint(const std::string& name) const {
        auto it = constraints.find(name);
        if (it != constraints.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    // Returns a reference to all registered constraints
    const std::unordered_map<std::string, std::string>& get_constraints() const {
        return constraints;
    }

    // Adds/registers an allowed relationship path
    void add_allowed_relationship(const std::string& src, const std::string& rel, const std::string& tgt) {
        allowed_relationships.push_back({src, rel, tgt});
    }

    // Returns a reference to all allowed relationships
    const std::vector<std::tuple<std::string, std::string, std::string>>& get_allowed_relationships() const {
        return allowed_relationships;
    }

private:
    // Disjointness registries
    std::unordered_map<std::string, std::unordered_set<std::string>> disjoint_labels;
    std::unordered_map<std::string, std::unordered_set<std::string>> disjoint_values;

public:
    // Adds/registers a pair of disjoint node labels
    void add_disjoint_labels(const std::string& l1, const std::string& l2) {
        disjoint_labels[l1].insert(l2);
        disjoint_labels[l2].insert(l1);
    }

    // Returns all registered disjoint labels
    const std::unordered_map<std::string, std::unordered_set<std::string>>& get_disjoint_labels() const {
        return disjoint_labels;
    }

    // Adds/registers a pair of disjoint concept property values
    void add_disjoint_values(const std::string& v1, const std::string& v2) {
        disjoint_values[v1].insert(v2);
        disjoint_values[v2].insert(v1);
    }

    // Returns all registered disjoint values
    const std::unordered_map<std::string, std::unordered_set<std::string>>& get_disjoint_values() const {
        return disjoint_values;
    }

    // Clears all views, constraints, allowed relationships, and disjoint registries
    void clear() {
        views.clear();
        constraints.clear();
        allowed_relationships.clear();
        disjoint_labels.clear();
        disjoint_values.clear();
    }
};

} // namespace ragedb::gql

#endif // RAGEDB_GQLVIRTUALCATALOG_H
