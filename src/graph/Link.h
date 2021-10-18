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

#ifndef RAGEDB_LINK_H
#define RAGEDB_LINK_H

#include <cstdint>
#include <ostream>
#include <eve/product_type.hpp>

namespace ragedb {
  struct Link : eve::struct_support<Link, std::uint64_t, std::uint64_t> {

        static decltype(auto) node_id(eve::like<Link> auto&& self) {
          return get<0>(std::forward<decltype(self)>(self));
        }

        static decltype(auto) rel_id(eve::like<Link> auto&& self) {
          return get<1>(std::forward<decltype(self)>(self));
        }

        friend std::ostream& operator<<(std::ostream& os, const Link& ids);
    };
}


#endif //RAGEDB_LINK_H
