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

#include "GqlValue.h"
#include "../graph/types/Date.h"
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <optional>
#include <sstream>

namespace ragedb::gql {

/**
 * @brief Compare two property_type_t variants.
 * 
 * Supports comparison between identical variant types (int64_t, double, std::string, bool)
 * and cross-type numerical comparisons between int64_t and double.
 * 
 * @param lhs Left property variant.
 * @param rhs Right property variant.
 * @return int -1 if lhs < rhs, 1 if lhs > rhs, 0 if equal or incompatible.
 */
int compare_properties(const property_type_t& lhs, const property_type_t& rhs) {
    if (lhs.index() == 0 && rhs.index() != 0) {
        return -1;
    }
    if (lhs.index() != 0 && rhs.index() == 0) {
        return 1;
    }
    if (lhs.index() == 0 && rhs.index() == 0) {
        return 0;
    }

    if (lhs.index() == rhs.index()) {
        if (std::holds_alternative<int64_t>(lhs)) {
            int64_t l = std::get<int64_t>(lhs);
            int64_t r = std::get<int64_t>(rhs);
            return (l < r) ? -1 : ((l > r) ? 1 : 0);
        }
        if (std::holds_alternative<double>(lhs)) {
            double l = std::get<double>(lhs);
            double r = std::get<double>(rhs);
            return (l < r) ? -1 : ((l > r) ? 1 : 0);
        }
        if (std::holds_alternative<std::string>(lhs)) {
            const auto& l = std::get<std::string>(lhs);
            const auto& r = std::get<std::string>(rhs);
            return (l < r) ? -1 : ((l > r) ? 1 : 0);
        }
        if (std::holds_alternative<bool>(lhs)) {
            bool l = std::get<bool>(lhs);
            bool r = std::get<bool>(rhs);
            return (l < r) ? -1 : ((l > r) ? 1 : 0);
        }
    } else {
        // Handle cross-type numerical comparison
        if (std::holds_alternative<int64_t>(lhs) && std::holds_alternative<double>(rhs)) {
            double l = static_cast<double>(std::get<int64_t>(lhs));
            double r = std::get<double>(rhs);
            return (l < r) ? -1 : ((l > r) ? 1 : 0);
        }
        if (std::holds_alternative<double>(lhs) && std::holds_alternative<int64_t>(rhs)) {
            double l = std::get<double>(lhs);
            double r = static_cast<double>(std::get<int64_t>(rhs));
            return (l < r) ? -1 : ((l > r) ? 1 : 0);
        }
    }
    return 0;
}

/**
 * @brief Compare two GqlValues.
 * 
 * Compares by GqlValue::Type first. For properties, compares using compare_properties.
 * For nodes and relationships, compares using their underlying graph ID.
 * 
 * @param a First GqlValue.
 * @param b Second GqlValue.
 * @return int -1 if a < b, 1 if a > b, 0 if equal.
 */
int compare_gql_values(const GqlValue& a, const GqlValue& b) {
    if (a.type != b.type) return (a.type < b.type) ? -1 : 1;
    if (a.type == GqlValue::PROPERTY) {
        return compare_properties(a.property, b.property);
    }
    if (a.type == GqlValue::NODE) {
        uint64_t la = a.node->getId();
        uint64_t lb = b.node->getId();
        return (la < lb) ? -1 : ((la > lb) ? 1 : 0);
    }
    if (a.type == GqlValue::RELATIONSHIP) {
        uint64_t la = a.relationship->getId();
        uint64_t lb = b.relationship->getId();
        return (la < lb) ? -1 : ((la > lb) ? 1 : 0);
    }
    if (a.type == GqlValue::RELATIONSHIP_LIST) {
        if (a.relationship_list->size() != b.relationship_list->size()) {
            return (a.relationship_list->size() < b.relationship_list->size()) ? -1 : 1;
        }
        for (size_t i = 0; i < a.relationship_list->size(); ++i) {
            uint64_t la = (*a.relationship_list)[i].getId();
            uint64_t lb = (*b.relationship_list)[i].getId();
            if (la != lb) {
                return (la < lb) ? -1 : 1;
            }
        }
        return 0;
    }
    // Compare two Path values.
    // Paths are compared first by length. If lengths match, they are compared node-by-node by ID,
    // and then relationship-by-relationship by ID.
    if (a.type == GqlValue::PATH) {
        if (a.path->length() != b.path->length()) {
            return (a.path->length() < b.path->length()) ? -1 : 1;
        }
        const auto& a_nodes = a.path->GetNodes();
        const auto& b_nodes = b.path->GetNodes();
        for (size_t i = 0; i < a_nodes.size(); ++i) {
            if (a_nodes[i].getId() != b_nodes[i].getId()) {
                return (a_nodes[i].getId() < b_nodes[i].getId()) ? -1 : 1;
            }
        }
        const auto& a_rels = a.path->GetRelationships();
        const auto& b_rels = b.path->GetRelationships();
        for (size_t i = 0; i < a_rels.size(); ++i) {
            if (a_rels[i].getId() != b_rels[i].getId()) {
                return (a_rels[i].getId() < b_rels[i].getId()) ? -1 : 1;
            }
        }
        return 0;
    }
    // Compare two list values by length, then element by element.
    if (a.type == GqlValue::LIST) {
        const auto& al = *a.list;
        const auto& bl = *b.list;
        if (al.size() != bl.size()) return (al.size() < bl.size()) ? -1 : 1;
        for (size_t i = 0; i < al.size(); ++i) {
            int c = compare_gql_values(al[i], bl[i]);
            if (c != 0) return c;
        }
        return 0;
    }
    return 0;
}

/**
 * @brief Checks if a map of target properties matches a GQL pattern's properties.
 * 
 * Matches successfully if target contains all pattern keys, with equal values.
 * 
 * @param target The map of actual node/edge properties.
 * @param pattern The map of properties specified in the GQL query pattern.
 * @return true If the target matches the pattern.
 * @return false Otherwise.
 */
bool matches_properties(const std::map<std::string, property_type_t>& target, const std::map<std::string, property_type_t>& pattern) {
    for (const auto& [key, val] : pattern) {
        auto it = target.find(key);
        if (it == target.end()) return false;
        if (compare_properties(it->second, val) != 0) return false;
    }
    return true;
}

bool matches_filters(const std::map<std::string, property_type_t>& target, const std::vector<PropertyFilter>& filters) {
    for (const auto& filter : filters) {
        auto it = target.find(filter.property);
        if (it == target.end()) return false;

        int cmp = compare_properties(it->second, filter.value);
        switch (filter.op) {
            case Operation::EQ:
                if (cmp != 0) return false;
                break;
            case Operation::NEQ:
                if (cmp == 0) return false;
                break;
            case Operation::LT:
                if (cmp >= 0) return false;
                break;
            case Operation::LTE:
                if (cmp > 0) return false;
                break;
            case Operation::GT:
                if (cmp <= 0) return false;
                break;
            case Operation::GTE:
                if (cmp < 0) return false;
                break;
            default:
                return false;
        }
    }
    return true;
}

bool matches_label_expr(const std::string& actual_type, const std::shared_ptr<LabelExpression>& expr) {
    if (!expr) return true;
    switch (expr->kind) {
        case LabelExprKind::LITERAL:
            return actual_type == expr->name;
        case LabelExprKind::NOT:
            return !matches_label_expr(actual_type, expr->expr);
        case LabelExprKind::AND:
            return matches_label_expr(actual_type, expr->left) && matches_label_expr(actual_type, expr->right);
        case LabelExprKind::OR:
            return matches_label_expr(actual_type, expr->left) || matches_label_expr(actual_type, expr->right);
        case LabelExprKind::WILDCARD:
            return !actual_type.empty() && actual_type != "_default" && actual_type != "_";
    }
    return false;
}

GqlValue apply_cast(const GqlValue& value, CastType target) {
    // Only primitive values convert; a node, relationship, path or list has no scalar representation.
    if (value.type != GqlValue::PROPERTY || std::holds_alternative<std::monostate>(value.property)) {
        return GqlValue();
    }
    const property_type_t& p = value.property;

    switch (target) {
        case CastType::STRING: {
            if (std::holds_alternative<std::string>(p)) return value;
            if (std::holds_alternative<int64_t>(p)) return GqlValue(std::to_string(std::get<int64_t>(p)));
            if (std::holds_alternative<double>(p)) {
                std::ostringstream oss;
                oss << std::get<double>(p);
                return GqlValue(oss.str());
            }
            if (std::holds_alternative<bool>(p)) return GqlValue(std::string(std::get<bool>(p) ? "true" : "false"));
            return GqlValue();
        }
        case CastType::INTEGER: {
            if (std::holds_alternative<int64_t>(p)) return value;
            if (std::holds_alternative<bool>(p)) return GqlValue(static_cast<int64_t>(std::get<bool>(p) ? 1 : 0));
            if (std::holds_alternative<double>(p)) {
                const double d = std::get<double>(p);
                if (!std::isfinite(d)) return GqlValue();
                return GqlValue(static_cast<int64_t>(std::llround(d)));
            }
            if (std::holds_alternative<std::string>(p)) {
                // A string that is not wholly an integer has no integer value, so it casts to NULL rather
                // than to the prefix it happens to start with.
                const std::string& s = std::get<std::string>(p);
                try {
                    size_t consumed = 0;
                    const long long parsed = std::stoll(s, &consumed);
                    if (consumed != s.size()) return GqlValue();
                    return GqlValue(static_cast<int64_t>(parsed));
                } catch (...) {
                    return GqlValue();
                }
            }
            return GqlValue();
        }
        case CastType::FLOAT: {
            if (std::holds_alternative<double>(p)) return value;
            if (std::holds_alternative<int64_t>(p)) return GqlValue(static_cast<double>(std::get<int64_t>(p)));
            if (std::holds_alternative<bool>(p)) return GqlValue(std::get<bool>(p) ? 1.0 : 0.0);
            if (std::holds_alternative<std::string>(p)) {
                const std::string& s = std::get<std::string>(p);
                try {
                    size_t consumed = 0;
                    const double parsed = std::stod(s, &consumed);
                    if (consumed != s.size()) return GqlValue();
                    return GqlValue(parsed);
                } catch (...) {
                    return GqlValue();
                }
            }
            return GqlValue();
        }
        case CastType::BOOLEAN: {
            if (std::holds_alternative<bool>(p)) return value;
            if (std::holds_alternative<int64_t>(p)) return GqlValue(std::get<int64_t>(p) != 0);
            if (std::holds_alternative<double>(p)) return GqlValue(std::get<double>(p) != 0.0);
            if (std::holds_alternative<std::string>(p)) {
                std::string s = std::get<std::string>(p);
                std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
                if (s == "true") return GqlValue(true);
                if (s == "false") return GqlValue(false);
                return GqlValue();
            }
            return GqlValue();
        }
    }
    return GqlValue();
}

GqlValue apply_is_labeled(const GqlValue& value, const std::shared_ptr<LabelExpression>& label_expr, bool negated) {
    std::string type;
    if (value.type == GqlValue::NODE) {
        type = value.node->getType();
    } else if (value.type == GqlValue::RELATIONSHIP) {
        type = value.relationship->getType();
    } else {
        // Not an entity (or unbound): unknown, which propagates rather than reading as a false match.
        return GqlValue();
    }

    const bool labeled = matches_label_expr(type, label_expr);
    return GqlValue(negated ? !labeled : labeled);
}

std::optional<std::vector<GqlValue>> as_list_elements(const GqlValue& value) {
    if (value.type == GqlValue::LIST) {
        if (!value.list) return std::vector<GqlValue>{};
        return *value.list;
    }
    if (value.type == GqlValue::RELATIONSHIP_LIST) {
        std::vector<GqlValue> elements;
        if (value.relationship_list) {
            elements.reserve(value.relationship_list->size());
            for (const auto& rel : *value.relationship_list) {
                elements.push_back(GqlValue(rel));
            }
        }
        return elements;
    }
    if (value.type == GqlValue::PROPERTY) {
        // A stored list property (e.g. a string list) is a list too, even though it arrives as a variant
        // alternative rather than as the LIST type.
        std::vector<GqlValue> elements;
        if (std::holds_alternative<std::vector<bool>>(value.property)) {
            for (bool b : std::get<std::vector<bool>>(value.property)) elements.push_back(GqlValue(b));
            return elements;
        }
        if (std::holds_alternative<std::vector<int64_t>>(value.property)) {
            for (int64_t i : std::get<std::vector<int64_t>>(value.property)) elements.push_back(GqlValue(i));
            return elements;
        }
        if (std::holds_alternative<std::vector<double>>(value.property)) {
            for (double d : std::get<std::vector<double>>(value.property)) elements.push_back(GqlValue(d));
            return elements;
        }
        if (std::holds_alternative<std::vector<std::string>>(value.property)) {
            for (const auto& s : std::get<std::vector<std::string>>(value.property)) elements.push_back(GqlValue(s));
            return elements;
        }
    }
    return std::nullopt;
}

GqlValue gql_temporal_field(int64_t epoch_ms, const std::string& field) {
    // The value is an epoch-millisecond UTC datetime; extract the requested calendar/clock component.
    std::chrono::sys_time<std::chrono::milliseconds> tp{std::chrono::milliseconds{epoch_ms}};
    auto dp = std::chrono::floor<std::chrono::days>(tp);
    std::chrono::year_month_day ymd{dp};
    auto tod = tp - dp;  // time since midnight
    int64_t out;
    if (field == "year") out = static_cast<int>(ymd.year());
    else if (field == "month") out = static_cast<unsigned>(ymd.month());
    else if (field == "day") out = static_cast<unsigned>(ymd.day());
    else if (field == "hour") out = std::chrono::duration_cast<std::chrono::hours>(tod).count();
    else if (field == "minute") out = std::chrono::duration_cast<std::chrono::minutes>(tod).count() % 60;
    else if (field == "second") out = std::chrono::duration_cast<std::chrono::seconds>(tod).count() % 60;
    else return GqlValue();
    return GqlValue(out);
}

} // namespace ragedb::gql
