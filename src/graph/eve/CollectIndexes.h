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

#ifndef RAGEDB_COLLECTINDEXES_H
#define RAGEDB_COLLECTINDEXES_H

#include <eve/algo/concepts.hpp>
#include <eve/algo/for_each.hpp>
#include <eve/views/iota.hpp>
#include <eve/views/zip.hpp>

#include <eve/module/core.hpp>

#include <concepts>
#include <type_traits>
#include <vector>

// auto collect_indexes(std::vector<T>, Predicate predicate) ->
//   std::vector<std::uint64_t>
//
// Returns indexes of all elements for which Predicate returns true.
//
// I'd suggest to pass `auto` parameter in predicate, because otherwise
// You'd need to compute the cardinal.
//
//
// Unfortunately at the moment we don't expose all the things to make it nice,
// but it should work reasonably well on x86 from ssse3 to avx2
//
// For arm we do not yet have a good `compress_store` and for avx512 too.
// We can prioritize that if needed.

namespace ragedb {

template <
  eve::algo::relaxed_range R,// relaxed_range is our version of the std::range
                             // concept that supports things like aligned pointers, zip, etc.
                             // ---
  typename P,                // Predicate. Don't conceptify, since it's quite difficult (see below)
                             // and does not seem that useful.
                             // ---
  std::integral IdxType,     // Index type, should be big enough to store any element's position
                             // ---
  typename Alloc             // To not have to deal with allocators ourselves,
                             // We'll just allow you to pass a vector with any allocator.
  >
void collect_indexes(R&& r, P p, std::vector<IdxType, Alloc>& res)
{
    EVE_ASSERT( (static_cast<std::size_t>(r.end() - r.begin()) <= std::numeric_limits<IdxType>::max()),
      "The output type is not big enough to hold potential indexes");

    // Prepare the output in case it was not empty.
    res.clear();

    // Over allocating to always use `unsafe(compress_store)`.
    // eve won't go beyond eve::expected_cardinal_v<IdxType> per wide here.
    res.resize((r.end() - r.begin()) + eve::expected_cardinal_v<IdxType>);
    IdxType* out = res.data();

    // iota is going to be an iterator of 0, 1, 2, 3, ...
    auto idxes = eve::views::iota(IdxType{0});

    // This is a zip view of element, and it's index.
    auto r_with_idx = eve::views::zip(std::forward<R>(r), idxes);

    // Tweak for each for our purposes:
    //   aligning/unrolling won't help us here due to the operation being complicated.
    //   These tweaks won't affect correctness, you can align - it will work.
    auto for_each = eve::algo::for_each[eve::algo::no_aligning][eve::algo::unroll<1>];

    // Unlike `std::for_each` which calls the predicate with a reference,
    // eve::algo::for_each calls the operations with the iterator and ignore.
    // From iterator you can load/store values and ignore is needed to mask elements
    // outside the real range.
    //
    // The case where there is nothing to ignore is handled, there is a special `ignore_none`.
    for_each(
      r_with_idx,
      [&](eve::algo::iterator auto r_idx_it, eve::relative_conditional_expr auto ignore) mutable
      {
          // load an element and an index for each element.
          // The values in the `ignored` part are garbage.
          auto [elems, idxs] = eve::load[ignore](r_idx_it);

          // Apply the predicate
          auto test = p(elems);

          // We don't know what was the result of applying a predicate to garbage, we need to mask it.
          test = eve::replace_ignored(test, ignore, false);

          // unsafe(compress_store) - write elements marked as true to the output.
          // the elements are packed together to the left.
          // unsafe means we can write up to the register width of extra stuff.
          // returns pointer behind last written element
          //
          //  idxs    : [ 1 2 3 4 ]
          //  test    : [ f t f t ]
          //  written : [ 2 4 x x ]
          //  returns : out + 2
          out  = eve::unsafe(eve::compress_store)(idxs, test, out);
      });

    res.resize(out - res.data());
    res.shrink_to_fit();
}

}

#endif //RAGEDB_COLLECTINDEXES_H
