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

#ifndef RAGEDB_GQLPARSER_H
#define RAGEDB_GQLPARSER_H

#include "GqlLexer.h"
#include "GqlAst.h"
#include <vector>
#include <string>
#include <memory>

namespace ragedb::gql {

class GqlParser {
private:
    std::vector<Token> tokens;
    size_t pos = 0;

    const Token& peek(size_t offset = 0) const;
    const Token& advance();
    bool check(TokenType type) const;
    bool match(TokenType type);
    void consume(TokenType type, const std::string& error_message);
    std::string consume_identifier(const std::string& error_message);

    // Parsing routines
    GqlQuery parse_query();
    GqlQuery parse_union();
    GqlQuery parse_intersect();
    GqlQuery parse_single_query();
    void parse_return_items(GqlQuery& query, bool require_alias_for_expressions);
    void parse_group_by(GqlQuery& query);
    void parse_order_by(GqlQuery& query);
    void parse_limit(GqlQuery& query);
    MatchStatement parse_match();
    PathPattern parse_path_pattern();
    PatternNode parse_node_pattern();
    /// Parses a property map. Literal values are returned directly; when property_exprs is provided,
    /// a non-literal value (a bound variable, a LET binding, any computed expression) is parsed into it
    /// instead of being rejected.
    std::map<std::string, property_type_t> parse_properties(
        std::map<std::string, std::shared_ptr<Expression>>* property_exprs = nullptr);
    void parse_edge_details(PatternEdge& edge);
    /// Parses a label expression. `allow_keyword_ops` enables the `AND`/`OR`/`NOT` spellings alongside the
    /// symbolic `&`/`|`/`!`: they are unambiguous inside a pattern, where a delimiter ends the label, but
    /// not in an expression -- `n IS LABELED Person OR n.rank > 5` must read the OR as the boolean
    /// operator it is, so IS LABELED passes false and accepts only the symbolic forms.
    std::shared_ptr<LabelExpression> parse_label_expression(bool allow_keyword_ops = true);
    std::shared_ptr<LabelExpression> parse_label_or(bool allow_keyword_ops);
    std::shared_ptr<LabelExpression> parse_label_and(bool allow_keyword_ops);
    std::shared_ptr<LabelExpression> parse_label_factor(bool allow_keyword_ops);

    // Expression parsing (Precedence climbing)
    std::unique_ptr<Expression> parse_expression();
    std::unique_ptr<Expression> parse_or();
    std::unique_ptr<Expression> parse_and();
    std::unique_ptr<Expression> parse_not();
    std::unique_ptr<Expression> parse_comparison();
    std::unique_ptr<Expression> parse_add_sub();
    std::unique_ptr<Expression> parse_mul_div();
    std::unique_ptr<Expression> parse_unary();
    std::unique_ptr<Expression> parse_primary();
    /// Parse a braced subquery body `{ [MATCH ...]* [<bare pattern>] [WHERE <pred>] }` shared by
    /// EXISTS { ... } and COUNT { ... }. Assumes the next token is '{'; consumes through the '}'.
    void parse_braced_subquery(std::vector<MatchStatement>& matches, std::unique_ptr<Expression>& sub_where);

public:
    explicit GqlParser(std::vector<Token> tokens) : tokens(std::move(tokens)) {}
    static GqlQuery parse(const std::string& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_GQLPARSER_H
