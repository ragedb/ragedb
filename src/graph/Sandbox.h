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

#ifndef RAGEDB_SANDBOX_H
#define RAGEDB_SANDBOX_H

#include <sol/sol.hpp>

namespace ragedb {

  class Sandbox {
    sol::state &lua;
    sol::environment env;

  public:
    explicit Sandbox(sol::state &lua) : lua(lua) { buildEnvironment(); }

    sol::environment &getEnvironment() { return env; }

    // Checks whether path is allowed
    bool checkPath(const std::string &path, bool write);

    // No path is valid
    inline static std::string basePath = "";

  private:
    void buildEnvironment();

    /// Secure loadstring. Prohibits bytecode, applies environment.
    ///
    /// @param str Source code
    /// @param chunkname Chunk name
    /// @return Either (func, nil) or (nil, error-str)
    std::tuple<sol::object, sol::object> loadstring(const std::string &str,
      const std::string &chunkname = sol::detail::default_chunk_name());

    /// Secure loadfile. Checks path, then calls secure loadstring.
    ///
    /// @param path Path to file
    /// @return Either (func, nil) or (nil, error-str)
    std::tuple<sol::object, sol::object> loadfile(const std::string &path);

    /// Secure dofile
    /// @param path Path to file
    /// @return Return value of function
    sol::object dofile(const std::string &path);

    inline static const std::vector<std::string> ALLOWED_LUA_LIBRARIES = {
      "string",
      "table",
      "math"
    };

    // http://lua-users.org/wiki/SandBoxes
    inline static const std::vector<std::string> ALLOWED_CUSTOM_FUNCTIONS = {
      "json",
      "date",
      "ftcsv"
    };


    inline static const std::vector<std::string> ALLOWED_GRAPH_OBJECTS = {
      "Direction",
      "Link",
      "Node",
      "Operation",
      "Relationship",
      "Roar"
    };

    inline static const std::vector<std::string> ALLOWED_GRAPH_FUNCTIONS = {
      "AllNodes",
      "AllNodeIds",
      "AllRelationships",
      "AllRelationshipIds",

      "FilterNodeCount",
      "FilterNodeIds",
      "FilterNodes",
      "FilterRelationshipCount",
      "FilterRelationshipIds",
      "FilterRelationships",

      "FindNodeCount",
      "FindNodeIds",
      "FindNodes",
      "FindRelationshipCount",
      "FindRelationshipIds",
      "FindRelationships",

      "LinksGetLinks",
      "LinksGetNeighbors",
      "LinksGetRelationships",

      "NodeAdd",
      "NodeDeleteProperties",
      "NodeDeleteProperty",
      "NodeGet",
      "NodeGetDegree",
      "NodeGetId",
      "NodeGetKey",
      "NodeGetLinks",
      "NodeGetNeighbors",
      "NodeGetProperties",
      "NodeGetProperty",
      "NodeGetRelationships",
      "NodeGetType",
      "NodePropertyTypeAdd",
      "NodePropertyTypeDelete",
      "NodeRemove",
      "NodeResetPropertiesFromJson",
      "NodeSetPropertiesFromJson",
      "NodeSetProperty",
      "NodeSetPropertyFromJson",
      "NodeTypeGet",
      "NodeTypeGetType",
      "NodeTypeInsert",
      "NodeTypesGet",
      "NodeTypesGetCount",

      "NodesGet",
      "NodesGetKey",

      "NodesGetProperties",
      "NodesGetProperty",
      "NodesGetType",

      "RelationshipAdd",
      "RelationshipDeleteProperties",
      "RelationshipDeleteProperty",
      "RelationshipGet",
      "RelationshipGetEndingNodeId",
      "RelationshipGetProperties",
      "RelationshipGetProperty",
      "RelationshipGetStartingNodeId",
      "RelationshipGetType",
      "RelationshipPropertyTypeAdd",
      "RelationshipPropertyTypeDelete",
      "RelationshipRemove",
      "RelationshipResetPropertiesFromJson",
      "RelationshipSetPropertiesFromJson",
      "RelationshipSetProperty",
      "RelationshipSetPropertyFromJson"
      "RelationshipTypeGet",
      "RelationshipTypeGetType",
      "RelationshipTypeInsert",
      "RelationshipTypesGet",
      "RelationshipTypesGetCount",

      "RelationshipsGet",
      "RelationshipsGetProperties",
      "RelationshipsGetProperty",
      "RelationshipsGetType",
    };

    inline static const std::vector<std::string> ALLOWED_LUA_FUNCTIONS = {
      "assert",
      "error",
      "ipairs",
      "next",
      "pairs",
      "pcall",
      "print",
      "select",
      "tonumber",
      "tostring",
      "type",
      "unpack",
      "_VERSION",
      "xpcall",

      // These functions are unsafe as they can bypass or change metatables,
      // but they are required to implement classes.
      "rawequal",
      "rawget",
      "rawset",
      "setmetatable",
    };

  };

}


#endif// RAGEDB_SANDBOX_H
