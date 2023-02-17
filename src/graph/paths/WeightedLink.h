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

#ifndef RAGEDB_WEIGHTEDLINK_H
#define RAGEDB_WEIGHTEDLINK_H

#include "../Link.h"
namespace ragedb {
    class WeightedLink {
      public:
        WeightedLink(Link link);
        WeightedLink(Link link, double weight);
        Link link;
        double weight;

        [[nodiscard]] Link getLink() const;

        [[nodiscard]] double getWeight() const;

        std::partial_ordering operator<=>(const WeightedLink& that) const {
            if (auto cmp = weight <=> that.weight; cmp != 0)
                return cmp;
            return link <=> that.link;
        }

        bool operator==(const WeightedLink& that)const{
            return (link == that.link) && (weight == that.weight);
        }
        friend std::ostream& operator<<(std::ostream& os, const WeightedLink& weightedLink);
    };

}

#endif// RAGEDB_WEIGHTEDLINK_H
