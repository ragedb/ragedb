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

// The scalar-function library: the allowlist and the evaluate_scalar_function dispatch (paths, temporal,
// strings, lists, numeric, null-handling, and the graph-element predicates). Split out of GqlValue.cpp;
// the entry points are declared in GqlValue.h and this unit uses no seastar/json (serialization stays in
// GqlValue.cpp).

#include "GqlValue.h"
#include "../graph/types/Date.h"
#include <algorithm>
#include <cctype>
#include <cmath>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace ragedb::gql {

namespace {
/// The scalar functions evaluate_scalar_function below actually implements. Keep the two in step: a name
/// listed here but not dispatched would evaluate to NULL, which is the very failure this guards against.
const std::vector<std::string>& scalar_function_names() {
    static const std::vector<std::string> names = {
        // paths
        "length",
        "nodes",
        "relationships",
        "rels", // openCypher alias of relationships(path)
        // temporal (epoch milliseconds -- see the note on evaluate_scalar_function)
        "zoned_datetime",
        "datetime",
        "date",
        "localdatetime",
        "duration",
        // strings
        "substring",
        "char_length",
        "character_length",
        "octet_length",
        "upper",
        "lower",
        "trim",
        "ltrim",
        "rtrim",
        // lists
        "cardinality",
        "range",
        // numeric
        "abs",
        "ceil",
        "ceiling",
        "floor",
        "sqrt",
        "power",
        "mod",
        // null handling
        "coalesce",
        "nullif",
        // graph element functions / predicates
        "element_id",
        "property_exists",
        "all_different",
        "same",
    };
    return names;
}

/// Numeric view of a value: GQL's numeric functions accept either an integer or a float.
std::optional<double> numeric_arg(const GqlValue& v) {
    if (v.type != GqlValue::PROPERTY) return std::nullopt;
    if (std::holds_alternative<int64_t>(v.property)) return static_cast<double>(std::get<int64_t>(v.property));
    if (std::holds_alternative<double>(v.property)) return std::get<double>(v.property);
    return std::nullopt;
}

/// True when the value is an integer, so an integer-in/integer-out function can stay integral.
bool is_integer_arg(const GqlValue& v) {
    return v.type == GqlValue::PROPERTY && std::holds_alternative<int64_t>(v.property);
}

std::optional<std::string> string_arg(const GqlValue& v) {
    if (v.type == GqlValue::PROPERTY && std::holds_alternative<std::string>(v.property)) {
        return std::get<std::string>(v.property);
    }
    return std::nullopt;
}
}  // namespace

bool is_supported_scalar_function(const std::string& lower_name) {
    const auto& names = scalar_function_names();
    return std::find(names.begin(), names.end(), lower_name) != names.end();
}

std::string supported_scalar_function_list() {
    std::string out;
    for (const auto& name : scalar_function_names()) {
        if (!out.empty()) out += ", ";
        out += name;
    }
    return out;
}

GqlValue evaluate_scalar_function(const GqlRow& row, const FunctionCallExpr* fc) {
    return evaluate_scalar_function_with(fc, [&row](const Expression* e) {
        return evaluate_expression(row, e);
    });
}

GqlValue evaluate_scalar_function_with(const FunctionCallExpr* fc,
                                       const std::function<GqlValue(const Expression*)>& eval_arg) {
    if (!fc) return GqlValue();
    // length(path | relationship-list): number of relationships.
    if (fc->name == "length") {
        if (fc->args.size() != 1) return GqlValue();
        GqlValue arg = eval_arg(fc->args[0].get());
        if (arg.type == GqlValue::PATH) {
            return GqlValue(static_cast<int64_t>(arg.path->length()));
        }
        if (arg.type == GqlValue::RELATIONSHIP_LIST) {
            return GqlValue(static_cast<int64_t>(arg.relationship_list->size()));
        }
        return GqlValue();
    }
    // zoned_datetime / datetime / date (string): epoch MILLISECONDS as int64 (LDBC canonical unit).
    // Date::convert yields epoch seconds (double), so scale by 1000. Date::fromString only accepts the
    // 'YYYY-MM-DDThh:mm:ss' form (it returns null/0 for a date-only or space-separated string), so
    // normalise: turn a ' ' separator into 'T' and pad a bare date to midnight.
    if (fc->name == "zoned_datetime" || fc->name == "datetime" || fc->name == "date" ||
        fc->name == "localdatetime") {
        if (fc->args.size() != 1) return GqlValue();
        GqlValue arg = eval_arg(fc->args[0].get());
        int64_t ms;
        if (arg.type == GqlValue::PROPERTY && std::holds_alternative<std::string>(arg.property)) {
            std::string s = std::get<std::string>(arg.property);
            if (s.find('T') == std::string::npos) {
                auto sp = s.find(' ');
                if (sp != std::string::npos) s[sp] = 'T';
                else s += "T00:00:00";
            }
            double seconds = Date::convert(s);
            ms = static_cast<int64_t>(std::llround(seconds * 1000.0));
        } else if (arg.type == GqlValue::PROPERTY && std::holds_alternative<int64_t>(arg.property)) {
            // Applied to an already-epoch-ms value, e.g. date(message.creationDate): reuse it directly
            // rather than returning null (which silently dropped every date-filtered row).
            ms = std::get<int64_t>(arg.property);
        } else {
            return GqlValue();
        }
        // date() is day-resolution: truncate to the start of the UTC day. The datetime variants keep the
        // full timestamp.
        if (fc->name == "date") {
            const int64_t day_ms = 86400000LL;
            ms -= ((ms % day_ms) + day_ms) % day_ms;  // floor to midnight, correct for negatives too
        }
        return GqlValue(ms);
    }

    // duration('P100D' / 'PT4H30M' / ...): an ISO-8601 duration, returned as a count of milliseconds so it
    // adds/subtracts against an epoch-ms datetime with ordinary integer arithmetic. Years and months are
    // taken as 365 and 30 days (LDBC durations use days/hours/minutes/seconds, which are exact).
    if (fc->name == "duration") {
        if (fc->args.size() != 1) return GqlValue();
        auto s = string_arg(eval_arg(fc->args[0].get()));
        if (!s || s->size() < 2 || (*s)[0] != 'P') return GqlValue();
        const std::string& str = *s;
        int64_t ms = 0;
        bool in_time = false;
        size_t i = 1;
        while (i < str.size()) {
            if (str[i] == 'T') { in_time = true; ++i; continue; }
            size_t start = i;
            while (i < str.size() && (std::isdigit(static_cast<unsigned char>(str[i])) || str[i] == '.')) ++i;
            if (i == start || i >= str.size()) return GqlValue();  // a number must be followed by a unit
            double num = std::stod(str.substr(start, i - start));
            char unit = str[i++];
            double unit_ms = 0.0;
            if (!in_time) {
                if (unit == 'Y') unit_ms = 365.0 * 86400000.0;
                else if (unit == 'M') unit_ms = 30.0 * 86400000.0;
                else if (unit == 'W') unit_ms = 7.0 * 86400000.0;
                else if (unit == 'D') unit_ms = 86400000.0;
                else return GqlValue();
            } else {
                if (unit == 'H') unit_ms = 3600000.0;
                else if (unit == 'M') unit_ms = 60000.0;
                else if (unit == 'S') unit_ms = 1000.0;
                else return GqlValue();
            }
            ms += static_cast<int64_t>(num * unit_ms);
        }
        return GqlValue(ms);
    }

    // ---- paths -------------------------------------------------------------------------------------
    // nodes(path) / relationships(path) (alias rels): the path's elements, as a LIST.
    if (fc->name == "nodes" || fc->name == "relationships" || fc->name == "rels") {
        if (fc->args.size() != 1) return GqlValue();
        GqlValue arg = eval_arg(fc->args[0].get());
        if (arg.type != GqlValue::PATH) return GqlValue();

        auto items = std::make_shared<std::vector<GqlValue>>();
        if (fc->name == "nodes") {
            for (const auto& n : arg.path->GetNodes()) items->push_back(GqlValue(n));
        } else {
            for (const auto& r : arg.path->GetRelationships()) items->push_back(GqlValue(r));
        }
        GqlValue out;
        out.type = GqlValue::LIST;
        out.list = std::move(items);
        return out;
    }

    // ---- lists -------------------------------------------------------------------------------------
    // range(start, end [, step]): the inclusive integer list [start, start+step, ..., end]. Default step 1;
    // a negative step counts down. Used e.g. as range(0, size(list)-2) to index adjacent element pairs.
    if (fc->name == "range") {
        if (fc->args.size() < 2 || fc->args.size() > 3) return GqlValue();
        auto a = numeric_arg(eval_arg(fc->args[0].get()));
        auto b = numeric_arg(eval_arg(fc->args[1].get()));
        if (!a || !b) return GqlValue();
        int64_t start = static_cast<int64_t>(*a);
        int64_t end = static_cast<int64_t>(*b);
        int64_t step = 1;
        if (fc->args.size() == 3) {
            auto s = numeric_arg(eval_arg(fc->args[2].get()));
            if (!s) return GqlValue();
            step = static_cast<int64_t>(*s);
        }
        if (step == 0) return GqlValue();
        auto items = std::make_shared<std::vector<GqlValue>>();
        if (step > 0) {
            for (int64_t v = start; v <= end; v += step) items->push_back(GqlValue(property_type_t(v)));
        } else {
            for (int64_t v = start; v >= end; v += step) items->push_back(GqlValue(property_type_t(v)));
        }
        GqlValue out;
        out.type = GqlValue::LIST;
        out.list = std::move(items);
        return out;
    }

    // ---- strings -----------------------------------------------------------------------------------
    // substring(s, start [, length]): start is 0-based, matching the benchmark's usage. A start past the
    // end yields the empty string rather than an error.
    if (fc->name == "substring") {
        if (fc->args.size() < 2 || fc->args.size() > 3) return GqlValue();
        auto s = string_arg(eval_arg(fc->args[0].get()));
        auto start = numeric_arg(eval_arg(fc->args[1].get()));
        if (!s || !start || *start < 0) return GqlValue();

        const size_t from = static_cast<size_t>(*start);
        if (from >= s->size()) return GqlValue(std::string());

        size_t count = s->size() - from;
        if (fc->args.size() == 3) {
            auto len = numeric_arg(eval_arg(fc->args[2].get()));
            if (!len || *len < 0) return GqlValue();
            count = std::min(count, static_cast<size_t>(*len));
        }
        return GqlValue(s->substr(from, count));
    }
    if (fc->name == "char_length" || fc->name == "character_length" || fc->name == "octet_length") {
        if (fc->args.size() != 1) return GqlValue();
        auto s = string_arg(eval_arg(fc->args[0].get()));
        if (!s) return GqlValue();
        return GqlValue(static_cast<int64_t>(s->size()));
    }
    if (fc->name == "upper" || fc->name == "lower") {
        if (fc->args.size() != 1) return GqlValue();
        auto s = string_arg(eval_arg(fc->args[0].get()));
        if (!s) return GqlValue();
        std::string out = *s;
        const bool up = (fc->name == "upper");
        std::transform(out.begin(), out.end(), out.begin(), [up](unsigned char c) {
            return static_cast<char>(up ? std::toupper(c) : std::tolower(c));
        });
        return GqlValue(out);
    }
    if (fc->name == "trim" || fc->name == "ltrim" || fc->name == "rtrim") {
        if (fc->args.size() != 1) return GqlValue();
        auto s = string_arg(eval_arg(fc->args[0].get()));
        if (!s) return GqlValue();
        std::string out = *s;
        const auto is_space = [](unsigned char c) { return std::isspace(c) != 0; };
        if (fc->name != "rtrim") {
            out.erase(out.begin(), std::find_if_not(out.begin(), out.end(), is_space));
        }
        if (fc->name != "ltrim") {
            out.erase(std::find_if_not(out.rbegin(), out.rend(), is_space).base(), out.end());
        }
        return GqlValue(out);
    }

    // ---- lists -------------------------------------------------------------------------------------
    // cardinality(list): element count. Also accepts the list-shaped values that are not the LIST type
    // (a relationship list, a stored list property), the same view FOR expands.
    if (fc->name == "cardinality") {
        if (fc->args.size() != 1) return GqlValue();
        const auto elements = as_list_elements(eval_arg(fc->args[0].get()));
        if (!elements) return GqlValue();
        return GqlValue(static_cast<int64_t>(elements->size()));
    }

    // ---- numeric -----------------------------------------------------------------------------------
    if (fc->name == "abs") {
        if (fc->args.size() != 1) return GqlValue();
        GqlValue arg = eval_arg(fc->args[0].get());
        if (is_integer_arg(arg)) {
            const int64_t i = std::get<int64_t>(arg.property);
            return GqlValue(i < 0 ? -i : i);
        }
        auto d = numeric_arg(arg);
        if (!d) return GqlValue();
        return GqlValue(std::fabs(*d));
    }
    if (fc->name == "ceil" || fc->name == "ceiling" || fc->name == "floor" || fc->name == "sqrt") {
        if (fc->args.size() != 1) return GqlValue();
        auto d = numeric_arg(eval_arg(fc->args[0].get()));
        if (!d) return GqlValue();
        if (fc->name == "sqrt") {
            if (*d < 0.0) return GqlValue();   // no real root; NULL rather than NaN
            return GqlValue(std::sqrt(*d));
        }
        return GqlValue((fc->name == "floor") ? std::floor(*d) : std::ceil(*d));
    }
    if (fc->name == "power") {
        if (fc->args.size() != 2) return GqlValue();
        auto base = numeric_arg(eval_arg(fc->args[0].get()));
        auto exp = numeric_arg(eval_arg(fc->args[1].get()));
        if (!base || !exp) return GqlValue();
        return GqlValue(std::pow(*base, *exp));
    }
    if (fc->name == "mod") {
        if (fc->args.size() != 2) return GqlValue();
        GqlValue lhs = eval_arg(fc->args[0].get());
        GqlValue rhs = eval_arg(fc->args[1].get());
        // Integer operands stay integral; a zero divisor is NULL rather than a trap.
        if (is_integer_arg(lhs) && is_integer_arg(rhs)) {
            const int64_t divisor = std::get<int64_t>(rhs.property);
            if (divisor == 0) return GqlValue();
            return GqlValue(std::get<int64_t>(lhs.property) % divisor);
        }
        auto a = numeric_arg(lhs);
        auto b = numeric_arg(rhs);
        if (!a || !b || *b == 0.0) return GqlValue();
        return GqlValue(std::fmod(*a, *b));
    }

    // ---- null handling -----------------------------------------------------------------------------
    // coalesce(a, b, ...): the first argument that is not null.
    if (fc->name == "coalesce") {
        for (const auto& arg : fc->args) {
            GqlValue v = eval_arg(arg.get());
            const bool is_null = v.type == GqlValue::NIL ||
                                 (v.type == GqlValue::PROPERTY && std::holds_alternative<std::monostate>(v.property));
            if (!is_null) return v;
        }
        return GqlValue();
    }
    // nullif(a, b): null when the two are equal, otherwise a.
    if (fc->name == "nullif") {
        if (fc->args.size() != 2) return GqlValue();
        GqlValue a = eval_arg(fc->args[0].get());
        GqlValue b = eval_arg(fc->args[1].get());
        if (compare_gql_values(a, b) == 0) return GqlValue();
        return a;
    }

    // ---- graph element functions / predicates ------------------------------------------------------
    // element_id(element): the internal id of a node or relationship.
    if (fc->name == "element_id") {
        if (fc->args.size() != 1) return GqlValue();
        GqlValue v = eval_arg(fc->args[0].get());
        if (v.type == GqlValue::NODE) return GqlValue(static_cast<int64_t>(v.node->getId()));
        if (v.type == GqlValue::RELATIONSHIP) return GqlValue(static_cast<int64_t>(v.relationship->getId()));
        return GqlValue();
    }
    // property_exists(element, propertyName): whether the element carries the named property. The name
    // is a bare identifier (or a string), taken from the argument's syntax rather than evaluated.
    if (fc->name == "property_exists") {
        if (fc->args.size() != 2) return GqlValue();
        GqlValue v = eval_arg(fc->args[0].get());
        std::string prop;
        const Expression* pe = fc->args[1].get();
        if (pe->kind == ExpressionKind::VARIABLE) {
            prop = static_cast<const VariableExpr*>(pe)->name;
        } else {
            GqlValue pv = eval_arg(pe);
            if (pv.type == GqlValue::PROPERTY && std::holds_alternative<std::string>(pv.property)) {
                prop = std::get<std::string>(pv.property);
            }
        }
        if (prop.empty()) return GqlValue();
        if (v.type == GqlValue::NODE) {
            auto props = v.node->getProperties();
            return GqlValue(props.find(prop) != props.end());
        }
        if (v.type == GqlValue::RELATIONSHIP) {
            auto props = v.relationship->getProperties();
            return GqlValue(props.find(prop) != props.end());
        }
        return GqlValue();
    }
    // all_different(a, b, ...): true iff every element argument is a distinct graph element (by kind+id).
    // same(a, b, ...): true iff every element argument is the same graph element.
    if (fc->name == "all_different" || fc->name == "same") {
        if (fc->args.size() < 2) return GqlValue();
        std::vector<std::pair<int, uint64_t>> keys;  // kind: 0 = node, 1 = relationship
        keys.reserve(fc->args.size());
        for (const auto& a : fc->args) {
            GqlValue v = eval_arg(a.get());
            if (v.type == GqlValue::NODE) keys.emplace_back(0, v.node->getId());
            else if (v.type == GqlValue::RELATIONSHIP) keys.emplace_back(1, v.relationship->getId());
            else return GqlValue();  // a non-element argument leaves the predicate undefined
        }
        if (fc->name == "same") {
            for (size_t i = 1; i < keys.size(); ++i) {
                if (keys[i] != keys[0]) return GqlValue(false);
            }
            return GqlValue(true);
        }
        for (size_t i = 0; i < keys.size(); ++i) {
            for (size_t j = i + 1; j < keys.size(); ++j) {
                if (keys[i] == keys[j]) return GqlValue(false);
            }
        }
        return GqlValue(true);
    }

    return GqlValue(); // unreachable: the parser rejects names not in scalar_function_names()
}

}  // namespace ragedb::gql
