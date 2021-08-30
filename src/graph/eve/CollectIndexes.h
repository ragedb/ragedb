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

#include <eve/eve.hpp>

#include <eve/algo/for_each_iteration.hpp>
#include <eve/algo/preprocess_range.hpp>
#include <eve/algo/ptr_iterator.hpp>
#include <eve/algo/traits.hpp>

#include <eve/function/compress_store.hpp>
#include <eve/function/convert.hpp>
#include <eve/function/replace.hpp>

// auto collect_indexes(std::vector<T>, Predicate predicate) ->
//   std::vector<std::uint64_t>
//
// Returns indexes of all elements for which Predicate returns true.
//
// I'd suggest to pass `auto` parameter in predicate, because otherwise
// You'd need to compute the cardinal.
//
//
// Unfortunaty at the moment we don't expose all the things to make it nice,
// but it should work reasonably well on x86 from ssse3 to avx2
//
// For arm we do not yet have a good `compress_store` and for avx512 too.
// We can prioritize that if needed.

namespace ragedb {

    // Start reading from the algorithm.
    // This is the callback that will be invoked with `wide`
    template <typename N, typename P>
    struct collect_indexes_delegate
    {

        collect_indexes_delegate(P _p, std::uint64_t* _out) :
                p(_p),
                idx([](int i, int) { return i; }), // 0, 1, 2, 3, 4 ...
                out{_out}
        {}

        EVE_FORCEINLINE bool step(auto it, eve::relative_conditional_expr auto ignore, auto /*idx*/)
        {
            // Test the elements
            auto test = p(eve::load[ignore](it));

            // We need to convert it to the logical of index type.
            // This should be fixed after https://github.com/jfalcou/eve/issues/868
            auto idx_test = eve::convert(test, eve::as<eve::logical<std::uint64_t>>{});

            // Because we don't want to deal with ignored elements in compress store,
            // We'll do it here.
            // .mask returns `ignore` as logical
            idx_test = idx_test && ignore.mask(eve::as(idx_test));

            // Now we store the selected indexes
            out = eve::unsafe(eve::compress_store)(idx, idx_test, out);

            // Add to each index the step
            idx += N{}();

            return false;  // We don't want to break
        }

        P p;

        eve::wide<std::uint64_t, N> idx;  // This would be i in for (i = 0; i != size; ++i)

        eve::algo::unaligned_ptr_iterator<std::uint64_t, N> out;  // A wrapper around a pointer
    };

    template <typename I, typename P>
    std::vector<std::uint64_t> collect_indexes(I f, I l, P p)
    {
        if (f == l) return {};

        using T = typename std::iterator_traits<I>::value_type;

        // We need to decide how big of register we are going to use
        // eve::expected_cardinal_v - will tell you the native cardinal of a type
        // We need to take the bigger one of std::uint64_t and T
        using N = eve::fixed<sizeof(T) < sizeof(std::uint64_t) ?
                             eve::expected_cardinal_v<T> :
                             eve::expected_cardinal_v<std::uint64_t> >;
        // Construct iteration traits
        eve::algo::traits iteration_traits{
                eve::algo::no_aligning,          // This means that will not do use aligned reads.
                // Using aligned reads might be better, but it requires slightly
                // more work, since it means the element indexes won't start with 0
                // ----------------------------------------------------------------
                eve::algo::unroll<1>,            // This means, that we will not do any unrolling.
                // compress_store is a complex operation by itself and
                // unrolling is not benefitial here
                // ---------------------------------------------------------------
                eve::algo::force_cardinal<N{}()> // Use the step that we computed earlier
        };

        // Buffer to store indexes.
        // Added an extra step to size so that we don't have to do ignore at the end
        std::vector<std::uint64_t> res(unsigned((l - f) + N{}()), 0);

        // Create our callback
        collect_indexes_delegate<N, P> delegate{p, res.data()};

        // Convert from `std::vector` to smth that eve library can work with
        // And invoke the iteration
        auto processed = eve::algo::preprocess_range(iteration_traits, f, l);
        auto iteration = eve::algo::for_each_iteration(
                processed.traits(), processed.begin(), processed.end());
        iteration(delegate);

        // Now we need to remove the extra allocation
        std::uint64_t* new_end = delegate.out.ptr;
        res.resize(static_cast<std::size_t>(new_end - res.data()));

        res.shrink_to_fit();  // optional - remove extra allocation

        return res;
    }

}

#endif //RAGEDB_COLLECTINDEXES_H
