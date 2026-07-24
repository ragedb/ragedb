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

// Primary expressions: the leaves of the expression grammar and every parenthesised or braced form
// that starts one -- literals, variables and property lookups, list literals and comprehensions,
// CAST, CASE, quantified list predicates, the EXISTS/COUNT braced subqueries, and function calls
// (aggregate, percentile, size, duration, and the general scalar dispatch).

#include "GqlParser.h"
#include "GqlValue.h"
#include <stdexcept>
#include <algorithm>
#include <cctype>
#include <memory>
#include <optional>

namespace ragedb::gql {

std::unique_ptr<Expression> GqlParser::parse_primary() {
    if (match(TokenType::EXISTS)) {
        std::vector<MatchStatement> matches;
        std::unique_ptr<Expression> sub_where;
        parse_braced_subquery(matches, sub_where);
        return std::make_unique<ExistsExpr>(std::move(matches), std::move(sub_where));
    }
    // Quantified list predicates: all|any|none|single(x IN list WHERE pred). `all`/`any` are keywords
    // (ALL_KW is also `ALL SHORTEST`, disambiguated here by the following '('); `none`/`single` are names.
    {
        bool is_quant = false;
        QuantifiedPredicateExpr::Quantifier q = QuantifiedPredicateExpr::ALL;
        if (check(TokenType::ALL_KW) && peek(1).type == TokenType::LPAREN) { q = QuantifiedPredicateExpr::ALL; is_quant = true; }
        else if (check(TokenType::ANY_KW) && peek(1).type == TokenType::LPAREN) { q = QuantifiedPredicateExpr::ANY; is_quant = true; }
        else if (check(TokenType::NAME) && peek(1).type == TokenType::LPAREN) {
            std::string ln = peek().text;
            std::transform(ln.begin(), ln.end(), ln.begin(), [](unsigned char c) { return std::tolower(c); });
            if (ln == "none") { q = QuantifiedPredicateExpr::NONE; is_quant = true; }
            else if (ln == "single") { q = QuantifiedPredicateExpr::SINGLE; is_quant = true; }
        }
        if (is_quant) {
            advance(); // consume all/any/none/single
            consume(TokenType::LPAREN, "Expected '(' after quantified predicate");
            std::string qvar = peek().text;
            consume(TokenType::NAME, "Expected variable name in quantified predicate");
            consume(TokenType::IN_KW, "Expected 'IN' in quantified predicate");
            auto qlist = parse_expression();
            consume(TokenType::WHERE, "Expected 'WHERE' in quantified predicate");
            auto qpred = parse_expression();
            consume(TokenType::RPAREN, "Expected ')' after quantified predicate");
            return std::make_unique<QuantifiedPredicateExpr>(q, std::move(qvar), std::move(qlist), std::move(qpred));
        }
    }
    // List literal: [a, b, c]. (The `x IN [a, b]` form is desugared to OR earlier, so it never reaches
    // here; this is the standalone list value -- what FOR expands and what a list-typed binding holds.)
    if (match(TokenType::LBRACKET)) {
        // List comprehension: [x IN list [WHERE pred] [| projection]]. Detected by the `<name> IN` head;
        // '|' is not a value-expression operator (only a label-alternation), so it delimits the projection.
        if (check(TokenType::NAME) && peek(1).type == TokenType::IN_KW) {
            std::string cvar = peek().text;
            advance(); // variable
            advance(); // IN
            auto clist = parse_expression();
            std::unique_ptr<Expression> cfilter = nullptr;
            if (match(TokenType::WHERE)) cfilter = parse_expression();
            std::unique_ptr<Expression> cproj = nullptr;
            if (match(TokenType::PIPE)) cproj = parse_expression();
            consume(TokenType::RBRACKET, "Expected ']' to close list comprehension");
            return std::make_unique<ListComprehensionExpr>(std::move(cvar), std::move(clist),
                                                           std::move(cfilter), std::move(cproj));
        }
        std::vector<std::unique_ptr<Expression>> elements;
        if (!check(TokenType::RBRACKET)) {
            do {
                elements.push_back(parse_expression());
            } while (match(TokenType::COMMA));
        }
        consume(TokenType::RBRACKET, "Expected ']' to close list literal");
        return std::make_unique<ListExpr>(std::move(elements));
    }
    // CAST(<expr> AS <type>). The primitive type names are lexed as their own keywords (they are also
    // schema-DDL types), so accept those tokens as well as the spellings that arrive as plain names.
    if (match(TokenType::CAST)) {
        consume(TokenType::LPAREN, "Expected '(' after CAST");
        auto value = parse_expression();
        consume(TokenType::AS, "Expected 'AS' in CAST");

        std::optional<CastType> target;
        if (match(TokenType::STRING_KW)) {
            target = CastType::STRING;
        } else if (match(TokenType::INTEGER_KW)) {
            target = CastType::INTEGER;
        } else if (match(TokenType::DOUBLE_KW)) {
            target = CastType::FLOAT;
        } else if (match(TokenType::BOOLEAN_KW)) {
            target = CastType::BOOLEAN;
        } else if (check(TokenType::NAME)) {
            std::string type_name = peek().text;
            advance();
            std::transform(type_name.begin(), type_name.end(), type_name.begin(),
                           [](unsigned char c) { return std::toupper(c); });
            if (type_name == "VARCHAR" || type_name == "TEXT") {
                target = CastType::STRING;
            } else if (type_name == "BIGINT") {
                target = CastType::INTEGER;
            } else if (type_name == "FLOAT" || type_name == "REAL") {
                target = CastType::FLOAT;
            } else {
                throw std::runtime_error("Unsupported CAST target type: " + type_name);
            }
        } else {
            throw std::runtime_error("Expected a type name after 'AS' in CAST");
        }

        consume(TokenType::RPAREN, "Expected ')' to close CAST");
        return std::make_unique<CastExpr>(std::move(value), *target);
    }
    // ISO GQL COUNT { <pattern subquery> } counts subquery matches (distinct from the count(x)
    // aggregate). It shares the braced-subquery body with EXISTS and reuses SizeExpr (a match count).
    if (check(TokenType::NAME) && peek(1).type == TokenType::LBRACE) {
        std::string upper_name = peek().text;
        std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        if (upper_name == "COUNT") {
            advance(); // consume COUNT
            std::vector<MatchStatement> matches;
            std::unique_ptr<Expression> sub_where;
            parse_braced_subquery(matches, sub_where);
            return std::make_unique<SizeExpr>(std::move(matches), std::move(sub_where));
        }
    }
    // Searched CASE expression: CASE WHEN cond THEN val [WHEN ...] [ELSE val] END.
    if (match(TokenType::CASE_KW)) {
        std::vector<std::pair<std::unique_ptr<Expression>, std::unique_ptr<Expression>>> branches;
        while (match(TokenType::WHEN_KW)) {
            auto cond = parse_expression();
            consume(TokenType::THEN_KW, "Expected 'THEN' after CASE WHEN condition");
            auto val = parse_expression();
            branches.emplace_back(std::move(cond), std::move(val));
        }
        if (branches.empty()) {
            throw std::runtime_error("CASE expression requires at least one WHEN branch");
        }
        std::unique_ptr<Expression> else_expr = nullptr;
        if (match(TokenType::ELSE_KW)) {
            else_expr = parse_expression();
        }
        consume(TokenType::END_KW, "Expected 'END' to close CASE expression");
        return std::make_unique<CaseExpr>(std::move(branches), std::move(else_expr));
    }
    if (match(TokenType::TRUE_KW)) {
        return std::make_unique<LiteralExpr>(true);
    }
    if (match(TokenType::FALSE_KW)) {
        return std::make_unique<LiteralExpr>(false);
    }
    if (match(TokenType::NULL_KW)) {
        return std::make_unique<LiteralExpr>(std::monostate{});
    }
    if (check(TokenType::NUMBER)) {
        int64_t val = peek().int_value;
        advance();
        return std::make_unique<LiteralExpr>(val);
    }
    if (check(TokenType::FLOAT_LIT)) {
        double val = peek().float_value;
        advance();
        return std::make_unique<LiteralExpr>(val);
    }
    if (check(TokenType::STRING_LIT)) {
        std::string val = peek().text;
        advance();
        return std::make_unique<LiteralExpr>(val);
    }
    if (match(TokenType::LPAREN)) {
        auto expr = parse_expression();
        consume(TokenType::RPAREN, "Expected ')' after expression");
        return expr;
    }
    if (check(TokenType::NAME)) {
        std::string var = peek().text;
        // Check for aggregate function call: NAME '('
        if (peek(1).type == TokenType::LPAREN) {
            std::string upper_name = var;
            std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), [](unsigned char c){ return std::toupper(c); });
            // `collect(x)` is the openCypher spelling; ISO GQL's general set function is `collect_list(x)`.
            // The dialect is pure GQL (WITH was removed for the same reason), so reject it rather than
            // quietly accepting a Cypher-ism.
            if (upper_name == "COLLECT") {
                throw std::runtime_error(
                    "collect is not GQL: use collect_list(...)");
            }
            if (upper_name == "COUNT" || upper_name == "SUM" || upper_name == "AVG" || upper_name == "MIN" || upper_name == "MAX" ||
                upper_name == "COLLECT_LIST" || upper_name == "STDDEV_POP" || upper_name == "STDDEV_SAMP") {
                advance(); // consume function name
                consume(TokenType::LPAREN, "Expected '(' after aggregate function");

                std::unique_ptr<Expression> arg = nullptr;
                bool distinct = false;
                // Special case for count(*)
                if (upper_name == "COUNT" && check(TokenType::STAR)) {
                    advance(); // consume '*'
                } else {
                    // Optional DISTINCT before the aggregated expression, e.g. count(DISTINCT x).
                    if (match(TokenType::DISTINCT)) {
                        distinct = true;
                    }
                    arg = parse_expression();
                }
                consume(TokenType::RPAREN, "Expected ')' after aggregate function argument");

                AggregateKind fn;
                if (upper_name == "COUNT") fn = AggregateKind::COUNT;
                else if (upper_name == "SUM") fn = AggregateKind::SUM;
                else if (upper_name == "AVG") fn = AggregateKind::AVG;
                else if (upper_name == "MIN") fn = AggregateKind::MIN;
                else if (upper_name == "COLLECT_LIST") fn = AggregateKind::COLLECT;
                else if (upper_name == "STDDEV_POP") fn = AggregateKind::STDDEV_POP;
                else if (upper_name == "STDDEV_SAMP") fn = AggregateKind::STDDEV_SAMP;
                else fn = AggregateKind::MAX;

                return std::make_unique<AggregateExpr>(fn, std::move(arg), distinct);
            } else if (upper_name == "PERCENTILE_CONT" || upper_name == "PERCENTILE_DISC") {
                // ISO GQL binary set function: PERCENTILE_CONT(value, fraction). The second argument
                // is the percentile in [0,1]; the first is the value expression to rank.
                advance(); // consume function name
                consume(TokenType::LPAREN, "Expected '(' after percentile function");
                bool distinct = match(TokenType::DISTINCT);
                std::unique_ptr<Expression> value = parse_expression();
                consume(TokenType::COMMA, "Expected ',' between percentile value and fraction");
                std::unique_ptr<Expression> fraction = parse_expression();
                consume(TokenType::RPAREN, "Expected ')' after percentile arguments");
                AggregateKind fn = upper_name == "PERCENTILE_CONT"
                    ? AggregateKind::PERCENTILE_CONT : AggregateKind::PERCENTILE_DISC;
                auto agg = std::make_unique<AggregateExpr>(fn, std::move(value), distinct);
                agg->arg2 = std::move(fraction);
                return agg;
            } else if (upper_name == "SIZE") {
                advance(); // consume "size"
                consume(TokenType::LPAREN, "Expected '(' after size");

                // size((pattern)) counts pattern matches (SizeExpr). size(<value expr>) -- a list or
                // string -- is the element/character count and aliases cardinality(). A path pattern
                // begins with a node '('; anything else at this position is a value expression.
                if (!check(TokenType::LPAREN)) {
                    std::vector<std::unique_ptr<Expression>> cargs;
                    cargs.push_back(parse_expression());
                    consume(TokenType::RPAREN, "Expected ')' after size argument");
                    return std::make_unique<FunctionCallExpr>("cardinality", std::move(cargs));
                }

                std::vector<MatchStatement> matches;
                MatchStatement stmt;
                stmt.pattern = parse_path_pattern();
                stmt.id = 0;
                matches.push_back(std::move(stmt));

                std::unique_ptr<Expression> sub_where = nullptr;
                if (match(TokenType::WHERE)) {
                    sub_where = parse_expression();
                }

                consume(TokenType::RPAREN, "Expected ')' after size expression");
                return std::make_unique<SizeExpr>(std::move(matches), std::move(sub_where));
            } else if (upper_name == "DURATION") {
                advance(); // consume "duration"
                consume(TokenType::LPAREN, "Expected '(' after duration");
                if (check(TokenType::LBRACE)) {
                    // Map form duration({hours: 4, minutes: 30, ...}): fixed unit keys and numeric-literal
                    // values, so fold to a millisecond count at parse time (the string form is evaluated at
                    // runtime; both yield an integer count of milliseconds).
                    advance(); // '{'
                    int64_t ms = 0;
                    if (!check(TokenType::RBRACE)) {
                        do {
                            std::string unit = peek().text;
                            std::transform(unit.begin(), unit.end(), unit.begin(),
                                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                            consume(TokenType::NAME, "Expected a duration unit key");
                            consume(TokenType::COLON, "Expected ':' after a duration unit");
                            bool neg = match(TokenType::MINUS);
                            double num;
                            if (check(TokenType::NUMBER)) { num = static_cast<double>(peek().int_value); advance(); }
                            else if (check(TokenType::FLOAT_LIT)) { num = peek().float_value; advance(); }
                            else throw std::runtime_error("Expected a numeric value for duration unit '" + unit + "'");
                            if (neg) num = -num;
                            double unit_ms;
                            if (unit == "years") unit_ms = 365.0 * 86400000.0;
                            else if (unit == "months") unit_ms = 30.0 * 86400000.0;
                            else if (unit == "weeks") unit_ms = 7.0 * 86400000.0;
                            else if (unit == "days") unit_ms = 86400000.0;
                            else if (unit == "hours") unit_ms = 3600000.0;
                            else if (unit == "minutes") unit_ms = 60000.0;
                            else if (unit == "seconds") unit_ms = 1000.0;
                            else if (unit == "milliseconds") unit_ms = 1.0;
                            else throw std::runtime_error("Unknown duration unit '" + unit + "'");
                            ms += static_cast<int64_t>(num * unit_ms);
                        } while (match(TokenType::COMMA));
                    }
                    consume(TokenType::RBRACE, "Expected '}' to close the duration map");
                    consume(TokenType::RPAREN, "Expected ')' after duration");
                    return std::make_unique<LiteralExpr>(ms);
                }
                // String form duration('P100D'): evaluated at runtime by the duration scalar function.
                auto darg = parse_expression();
                consume(TokenType::RPAREN, "Expected ')' after duration argument");
                std::vector<std::unique_ptr<Expression>> dargs;
                dargs.push_back(std::move(darg));
                return std::make_unique<FunctionCallExpr>("duration", std::move(dargs));
            } else {
                // Generic scalar function call: name ( arg, ... ) -- e.g. length(p),
                // zoned_datetime('2010-01-01'). Dispatched by name in the evaluator/typechecker.
                advance(); // consume function name
                consume(TokenType::LPAREN, "Expected '(' after function name");
                std::vector<std::unique_ptr<Expression>> args;
                if (!check(TokenType::RPAREN)) {
                    do {
                        args.push_back(parse_expression());
                    } while (match(TokenType::COMMA));
                }
                consume(TokenType::RPAREN, "Expected ')' after function arguments");
                std::string lname = var;
                std::transform(lname.begin(), lname.end(), lname.begin(),
                               [](unsigned char c) { return std::tolower(c); });

                // Only names the evaluator actually implements may parse. Anything else used to build a
                // FunctionCallExpr that typechecked as ANY and evaluated to NULL, so a query calling an
                // unimplemented function -- abs(), toInteger(), labels(), coalesce(), shortestPath() --
                // silently produced NULLs and plausible-looking wrong answers instead of failing. Reject
                // it here, where the name is known and the error can name it.
                if (!is_supported_scalar_function(lname)) {
                    throw std::runtime_error("Unknown function: " + var + "(). Supported scalar functions: " +
                                             supported_scalar_function_list());
                }
                return std::make_unique<FunctionCallExpr>(std::move(lname), std::move(args));
            }
        }

        advance();
        if (match(TokenType::DOT)) {
            std::string prop = peek().text;
            TokenType type = peek().type;
            if (type == TokenType::NAME || (type >= TokenType::TRUE_KW && type < TokenType::LPAREN)) {
                advance();
            } else {
                throw std::runtime_error("Expected property name after '.' (found: " + prop + ")");
            }
            return std::make_unique<PropertyLookupExpr>(var, prop);
        }
        return std::make_unique<VariableExpr>(var);
    }

    // A keyword that reaches expression position (all keyword-led forms -- literals, NOT, EXISTS --
    // were handled above) can only be an identifier shadowed by a reserved word: keep variables and
    // aliases named e.g. `with` referencable.
    {
        TokenType t = peek().type;
        if (t >= TokenType::TRUE_KW && t < TokenType::LPAREN) {
            std::string var = peek().text;
            advance();
            if (match(TokenType::DOT)) {
                std::string prop = peek().text;
                TokenType pt = peek().type;
                if (pt == TokenType::NAME || (pt >= TokenType::TRUE_KW && pt < TokenType::LPAREN)) {
                    advance();
                } else {
                    throw std::runtime_error("Expected property name after '.' (found: " + prop + ")");
                }
                return std::make_unique<PropertyLookupExpr>(var, prop);
            }
            return std::make_unique<VariableExpr>(var);
        }
    }

    throw std::runtime_error("Unexpected token in expression: " + peek().text);
}

} // namespace ragedb::gql
