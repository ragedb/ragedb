/*
 * Copyright Max De Marzi. All Rights Reserved.
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
#ifndef RAGEDB_DISAMBIGUATE_H
#define RAGEDB_DISAMBIGUATE_H

#include <sol/object.hpp>
#include "../Link.h"
#include "../Node.h"
#include "../Direction.h"

namespace ragedb {

    enum InnerType {
        direction,
        string,
        strings,
        id,
        ids,
        link,
        links,
        node,
        nodes,
        relationship,
        relationships,
        unknown
    };

}
#endif// RAGEDB_DISAMBIGUATE_H