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

#ifndef RAGEDB_OPERATION_H
#define RAGEDB_OPERATION_H

namespace ragedb {
    enum class Operation {
            EQ,              // A == B
            NEQ,             // A != B
            GT,              // A  > B
            GTE,             // A >= B
            LT,              // A  < B 
            LTE,             // A <= B
            IS_NULL,         // A is Null
            STARTS_WITH,     // A starts with B
            CONTAINS,        // A contains B
            ENDS_WITH,       // A ends with B
            NOT_IS_NULL,     // A is not Null
            NOT_STARTS_WITH, // A does not start with B
            NOT_CONTAINS,    // A does not contain B
            NOT_ENDS_WITH,   // A does not end with B
            UNKNOWN
    };
}
#endif //RAGEDB_OPERATION_H
