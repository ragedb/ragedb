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

#ifndef RAGEDB_DOMAINCONSTRAINTREASONER_H
#define RAGEDB_DOMAINCONSTRAINTREASONER_H

#include "../GqlAst.h"

namespace ragedb::gql {

class DomainConstraintReasoner {
public:
    /**
     * @brief Performs composite domain constraint reasoning pass via DPLL(T) solver.
     * Compiles where filters and catalog constraints into a logical formula and solves
     * satisfiability to detect contradictory query patterns at compile-time.
     */
    static void domain_constraint_reasoning_pass(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_DOMAINCONSTRAINTREASONER_H
