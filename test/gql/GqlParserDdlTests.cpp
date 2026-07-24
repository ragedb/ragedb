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

// The DDL (schema) grammar: CREATE/DROP/ALTER NODE TYPE, CREATE/DROP/SHOW INDEX, and CREATE/DROP VIEW and
// CONSTRAINT -- all lowering to a schema_op on the parsed query. Split out of GqlParserTests.cpp to keep it
// under the file-size target.

#include <catch2/catch.hpp>
#include "../../src/gql/GqlParser.h"

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("GQL Parser parses schema statements (DDL)", "[gql_parser]") {
    SECTION("CREATE NODE TYPE") {
        std::string query = "CREATE NODE TYPE Person";
        auto q = GqlParser::parse(query);
        REQUIRE(q.schema_op.has_value());
        REQUIRE(q.schema_op->op == SchemaOperation::Op::CREATE_NODE_TYPE);
        REQUIRE(q.schema_op->name == "Person");
        REQUIRE(q.schema_op->properties.empty());
    }

    SECTION("CREATE NODE TYPE with properties") {
        std::string query = "CREATE NODE TYPE Customer (name STRING, age INT)";
        auto q = GqlParser::parse(query);
        REQUIRE(q.schema_op.has_value());
        REQUIRE(q.schema_op->op == SchemaOperation::Op::CREATE_NODE_TYPE);
        REQUIRE(q.schema_op->name == "Customer");
        REQUIRE(q.schema_op->properties.size() == 2);
        REQUIRE(q.schema_op->properties[0].first == "name");
        REQUIRE(q.schema_op->properties[0].second == "string");
        REQUIRE(q.schema_op->properties[1].first == "age");
        REQUIRE(q.schema_op->properties[1].second == "integer");
    }

    SECTION("DROP NODE TYPE") {
        std::string query = "DROP NODE TYPE Person";
        auto q = GqlParser::parse(query);
        REQUIRE(q.schema_op.has_value());
        REQUIRE(q.schema_op->op == SchemaOperation::Op::DROP_NODE_TYPE);
        REQUIRE(q.schema_op->name == "Person");
    }

    SECTION("ALTER NODE TYPE ADD") {
        std::string query = "ALTER NODE TYPE Person ADD weight DOUBLE";
        auto q = GqlParser::parse(query);
        REQUIRE(q.schema_op.has_value());
        REQUIRE(q.schema_op->op == SchemaOperation::Op::ALTER_NODE_TYPE);
        REQUIRE(q.schema_op->name == "Person");
        REQUIRE(q.schema_op->alter_op == SchemaOperation::AlterOp::ADD);
        REQUIRE(q.schema_op->alter_property_name == "weight");
        REQUIRE(q.schema_op->alter_property_type == "double");
    }

    SECTION("ALTER NODE TYPE DROP") {
        std::string query = "ALTER NODE TYPE Person DROP age";
        auto q = GqlParser::parse(query);
        REQUIRE(q.schema_op.has_value());
        REQUIRE(q.schema_op->op == SchemaOperation::Op::ALTER_NODE_TYPE);
        REQUIRE(q.schema_op->name == "Person");
        REQUIRE(q.schema_op->alter_op == SchemaOperation::AlterOp::DROP);
        REQUIRE(q.schema_op->alter_property_name == "age");
    }
}

TEST_CASE("GQL Parser parses index statements (DDL)", "[gql_parser]") {
    SECTION("CREATE INDEX on node") {
        std::string query = "CREATE INDEX Person.name";
        auto q = GqlParser::parse(query);
        REQUIRE(q.schema_op.has_value());
        REQUIRE(q.schema_op->op == SchemaOperation::Op::CREATE_INDEX);
        REQUIRE(q.schema_op->name == "Person");
        REQUIRE(q.schema_op->alter_property_name == "name");
    }

    SECTION("CREATE INDEX on relationship") {
        std::string query = "CREATE INDEX WORKS_AT.since";
        auto q = GqlParser::parse(query);
        REQUIRE(q.schema_op.has_value());
        REQUIRE(q.schema_op->op == SchemaOperation::Op::CREATE_INDEX);
        REQUIRE(q.schema_op->name == "WORKS_AT");
        REQUIRE(q.schema_op->alter_property_name == "since");
    }

    SECTION("DROP INDEX on node") {
        std::string query = "DROP INDEX Person.name";
        auto q = GqlParser::parse(query);
        REQUIRE(q.schema_op.has_value());
        REQUIRE(q.schema_op->op == SchemaOperation::Op::DROP_INDEX);
        REQUIRE(q.schema_op->name == "Person");
        REQUIRE(q.schema_op->alter_property_name == "name");
    }

    SECTION("SHOW INDEXES") {
        std::string query = "SHOW INDEXES";
        auto q = GqlParser::parse(query);
        REQUIRE(q.schema_op.has_value());
        REQUIRE(q.schema_op->op == SchemaOperation::Op::SHOW_INDEXES);
        REQUIRE(q.schema_op->name == "");
    }

    SECTION("SHOW INDEXES ON Person") {
        std::string query = "SHOW INDEXES ON Person";
        auto q = GqlParser::parse(query);
        REQUIRE(q.schema_op.has_value());
        REQUIRE(q.schema_op->op == SchemaOperation::Op::SHOW_INDEXES);
        REQUIRE(q.schema_op->name == "Person");
    }
}

TEST_CASE("GQL Parser parses VIEW and CONSTRAINT DDL", "[gql_parser]") {
    SECTION("CREATE VIEW") {
        std::string query = "CREATE VIEW Adult AS MATCH (p:Person) WHERE p.age >= 18 RETURN p";
        auto q = GqlParser::parse(query);
        REQUIRE(q.schema_op.has_value());
        REQUIRE(q.schema_op->op == SchemaOperation::Op::CREATE_VIEW);
        REQUIRE(q.schema_op->name == "Adult");
        REQUIRE(q.schema_op->query_string == "MATCH ( p : Person ) WHERE p . age >= 18 RETURN p");
    }

    SECTION("DROP VIEW") {
        std::string query = "DROP VIEW Adult";
        auto q = GqlParser::parse(query);
        REQUIRE(q.schema_op.has_value());
        REQUIRE(q.schema_op->op == SchemaOperation::Op::DROP_VIEW);
        REQUIRE(q.schema_op->name == "Adult");
    }

    SECTION("CREATE CONSTRAINT") {
        std::string query = "CREATE CONSTRAINT PositiveAge AS MATCH (p:Person) WHERE p.age < 0 RETURN p";
        auto q = GqlParser::parse(query);
        REQUIRE(q.schema_op.has_value());
        REQUIRE(q.schema_op->op == SchemaOperation::Op::CREATE_CONSTRAINT);
        REQUIRE(q.schema_op->name == "PositiveAge");
        REQUIRE(q.schema_op->query_string == "MATCH ( p : Person ) WHERE p . age < 0 RETURN p");
    }

    SECTION("DROP CONSTRAINT") {
        std::string query = "DROP CONSTRAINT PositiveAge";
        auto q = GqlParser::parse(query);
        REQUIRE(q.schema_op.has_value());
        REQUIRE(q.schema_op->op == SchemaOperation::Op::DROP_CONSTRAINT);
        REQUIRE(q.schema_op->name == "PositiveAge");
    }
}
