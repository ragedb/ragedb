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

#include "benchmark/benchmark.h"
#include <random>
#include <set>
#include <vector>
#include <algorithm>

#if defined(__GNUC__) || defined(__HP_aCC) || defined(__clang__)
#define INLINE inline __attribute__((always_inline))
#else
#define INLINE __forceinline
#endif

// Force clang to use conditional move instead of branches. For other types of T than 64-bit integers, please
// rewrite the assembly or use the ternary operator below instead.
template <class T> INLINE size_t choose(T a, T b, size_t src1, size_t src2)
{
#if defined(__clang__) && defined(__x86_64)
    size_t res = src1;
    asm("cmpq %1, %2; cmovaeq %4, %0"
        :
        "=q" (res)
        :
        "q" (a),
        "q" (b),
        "q" (src1),
        "q" (src2),
        "0" (res)
        :
        "cc");
    return res;
#else
    return b >= a ? src2 : src1;
#endif
}

template<class RandomAccessIterator, class T>
INLINE RandomAccessIterator fast_upper_bound3_max(RandomAccessIterator first, RandomAccessIterator last, T value)
{
    size_t size = std::distance(first, last);
    size_t low = 0;

    while (size > 0) {
        size_t half = size / 2;
        size_t other_half = size - half;
        size_t probe = low + half;
        size_t other_low = low + other_half;
        T v = first[probe];
        size = half;
        // clang won't use conditional move here, so we have made a choose() function instead
        // low = v <= value ? other_low : low;
        low = choose(v, value, low, other_low);
    }

    return first + low;
}

template<class RandomAccessIterator, class T>
RandomAccessIterator branch_less_upper_bound_iterators(RandomAccessIterator first, RandomAccessIterator last, T value) {
    auto len = std::distance(first, last);
    while (len > 1) {
        auto half = len / 2;
        std::ranges::advance(first, half);
        std::ranges::advance(first, (value < *first ? -1 * half : 0));
        len -= half;
    }
    //std::ranges::advance(first, *first <= value);
    std::ranges::advance(first, *first < value); // will return exact if found
    return first;
}

template<class RandomAccessIterator, class T>
inline RandomAccessIterator leapfrogSeek2( RandomAccessIterator first, RandomAccessIterator last, const T& val ) {
    RandomAccessIterator it;
    it = branch_less_upper_bound_iterators( first, last, val);
    int bound, halfBound;

    //1. We first look for an interval [bound/2, bound] such that contains our value of interest.
    bound = 1;
    it = first + bound;
    while( it < last && it[0] < val ) {
        bound *= 2;
        it = first + bound;
    }
    halfBound = bound / 2;

    //2. We now check if the found upper bound is less than the end of the array.
    if( (it + bound) < last )
        last = it + bound;

    //3. Now that we have found the interval of interest, let's run a binary search!
    it = branch_less_upper_bound_iterators( first + halfBound, last, val);

    if(it == first)
        return it;
//
//    it--;             //4. Sadly the upper_bound procedure returns the first element "greater than"
    if( *it == val )   //   and not "greater or equal than" so we correct this below.
        return it;
    else
        return ++it;
}

/**
 * This procedure seeks the first upper bound such that is greater
 * or equal than a supplied value.
 * In order to meet the above complexity of the leapfrog join, it is crucial
 * that this procedure has amortized cost O(1 + log(Nmax/Nmin)).
 *
 * This can be achieved by first running exponential search for restricting the search
 * range. In fact it is not needed that the whole array must be visited.
 * Exponential search has complexity O(log(i)) where i is the position of the found element.
 */
template<class RandomAccessIterator, class T>
inline RandomAccessIterator leapfrogSeek( RandomAccessIterator first, RandomAccessIterator last, const T& val ) {
    RandomAccessIterator it;
    int bound, halfBound;

    //1. We first look for an interval [bound/2, bound] such that contains our value of interest.
    bound = 1;
    it = first + bound;
    while( it < last && it[0] < val ) {
        bound *= 2;
        it = first + bound;
    }
    halfBound = bound / 2;

    //2. We now check if the found upper bound is less than the end of the array.
    if( (it + bound) < last )
        last = it + bound;

    //3. Now that we have found the interval of interest, let's run a binary search!
    it = std::upper_bound( first + halfBound, last, val );

    //4. Sadly the upper_bound procedure returns the first element "greater than"
    //   and not "greater or equal than" so we correct this below.
    if(it == first)
        return it;

    it--;
    if( *it == val )
        return it;
    else
        return ++it;
}

/**
 * In below all the magic of intersection happens!
 * The algorithm just calls the leapfrogSeek repeatedly and does
 * book-keeping of the current maximum value.
 */
template<template<class...> class C,
  class... A,
  class T = typename C<A...>::value_type,
  class RandomAccessIterator = typename C<A...>::iterator>
void leapfrogJoin2( std::vector<C<A...>>& indexes, std::vector<T>& resultSet ) {
    RandomAccessIterator its[indexes.size()];
    T value, max;
    int it;
    T ceiling = std::numeric_limits<T>::max();
    //1. Check if any index is empty => Intersection empty
    for( auto& index : indexes )
        if( index.size() == 0 )
            return;

    //2. Sort indexes by their first value and do some initial iterators bookkeeping!
    std::sort( indexes.begin(), indexes.end(),
      [] ( const C<A...>& a, const C<A...>& b ) { return *a.begin() < *b.begin(); }
    );

    for ( int i = 0; i < indexes.size(); i++ ) {
        its[i] = indexes[i].begin();
        if (indexes[i].back() < ceiling) {
            ceiling = indexes[i].back();
        }
    }
    max = *( its[indexes.size() - 1] );
    it = 0;

    //3. Let the fun begin!
    while( max <= ceiling ) {
        value = *( its[it] );
        //3.1. An intersecting value has been found!
        if( value == max ) {
            resultSet.push_back( value );
            its[it]++;
            //3.2. We shall find a value greater or equal than the current max
        } else {
            its[it] = leapfrogSeek2( its[it], indexes[it].end(), max );
        }
        if( its[it] == indexes[it].end() )
            break;
        //4. Store the maximum
        max = *( its[it] );
        it = ++it % indexes.size();
    }
}

/**
 * In below all the magic of intersection happens!
 * The algorithm just calls the leapfrogSeek repeatedly and does
 * book-keeping of the current maximum value.
 */
template<template<class...> class C,
  class... A,
  class T = typename C<A...>::value_type,
  class RandomAccessIterator = typename C<A...>::iterator>
void leapfrogJoin( std::vector<C<A...>>& indexes, std::vector<T>& resultSet ) {
    RandomAccessIterator its[indexes.size()];
    T value, max;
    int it;

    //1. Check if any index is empty => Intersection empty
    for( auto& index : indexes )
        if( index.size() == 0 )
            return;

    // 2. Sort indexes by their first value and do some initial iterators book-keeping!
    std::sort( indexes.begin(), indexes.end(),
      [] ( const C<A...>& a, const C<A...>& b ) { return *a.begin() < *b.begin(); }
    );

    for ( int i = 0; i < indexes.size(); i++ )
        its[i] = indexes[i].begin();

    max = *( its[indexes.size() - 1] );
    it = 0;

    //3. Let the fun begin!
    while( true ) {
        value = *( its[it] );
        //3.1. An intersecting value has been found!
        if( value == max ) {
            resultSet.push_back( value );
            its[it]++;
            //3.2. We shall find a value greater or equal than the current max
        } else {
            its[it] = leapfrogSeek( its[it], indexes[it].end(), max );
        }
        if( its[it] == indexes[it].end() )
            break;
        //4. Store the maximum
        max = *( its[it] );
        it = ++it % indexes.size();
    }
}

template<template<class...> class C,
  class... A,
  class T = typename C<A...>::value_type>
std::vector<T> leapfrogJoin2( std::vector<C<A...>>& indexes ) {
    std::vector<T> resultSet;

    leapfrogJoin2( indexes, resultSet );
    return resultSet;
}

template<template<class...> class C,
  class... A,
  class T = typename C<A...>::value_type>
std::vector<T> leapfrogJoin( std::vector<C<A...>>& indexes ) {
    std::vector<T> resultSet;

    leapfrogJoin( indexes, resultSet );
    return resultSet;
}




size_t BSintersection(const int * set1, const size_t length1,
  const int * set2, const size_t length2, int *out);

size_t onesidedgallopingintersection(const int * smallset,
  const size_t smalllength, const int * largeset,
  const size_t largelength, int * out);


// use in function below
#define BRANCHLESSMATCH() {                     \
        int m = (*B == *A) ? 1 : 0;             \
        int a = (*B >= *A) ? 1 : 0;             \
        int b = (*B <= *A) ? 1 : 0;             \
        *Match = *A;                            \
        Match += m;                             \
        A += a;                                 \
        B += b;                                 \
    }


/**
 * Unrolled branchless approach by N. Kurz.
 */
size_t scalar_branchless_unrolled(const int *A, size_t lenA,
  const int *B, size_t lenB,
  int *Match) {

    const size_t UNROLLED = 4;

    const int *initMatch = Match;
    const int *endA = A + lenA;
    const int *endB = B + lenB;

    if (lenA >= UNROLLED && lenB >= UNROLLED) {
        const int *stopA = endA - UNROLLED;
        const int *stopB = endB - UNROLLED;

        while (A < stopA && B < stopB) {
            BRANCHLESSMATCH();  // NOTE: number of calls must match UNROLLED
            BRANCHLESSMATCH();
            BRANCHLESSMATCH();
            BRANCHLESSMATCH();
        }
    }

    // Finish remainder without overstepping
    while (A < endA && B < endB) {
        BRANCHLESSMATCH();
    }

    size_t count = Match - initMatch;
    return count;
}

#undef BRANCHLESSMATCH





std::unordered_map<int, std::unordered_map<int, std::vector<std::vector<int>>>> sorted_maps;
std::vector<std::vector<int>> sorted_vectors;

std::vector<int> generate_sorted_data(int count, int size) {
    auto randomNumberBetween = [](int low, int high) {
        auto randomFunc = [distribution_ = std::uniform_int_distribution<int>(low, high),
                            random_engine_ = std::mt19937{ std::random_device{}() }]() mutable {
            return distribution_(random_engine_);
        };
        return randomFunc;
    };

    std::vector<int> data;
    std::generate_n(std::back_inserter(data), size, randomNumberBetween(1, size * 20 / count));

    std::ranges::sort(data);
    return data;
}

void DoSetupMultiSet(const benchmark::State& state) {
    auto count = state.range(0);
    auto size = state.range(1);

    std::vector<std::vector<int>> vectors;
    for (auto i = 0; i < count; i++) {
        vectors.emplace_back(generate_sorted_data(count, size));
    }
    sorted_maps[count][size] = vectors;

    // Setup/Teardown should never be called with any thread_idx != 0.
    assert(state.thread_index() == 0);
}

std::vector<int> multi_set_intersection_leapfrog(std::vector<std::vector<int>>& nums) {
    auto res = leapfrogJoin( nums );
    return res;
}

std::vector<int> multi_set_intersection_leapfrog2(std::vector<std::vector<int>>& nums) {
    auto res = leapfrogJoin2( nums );
    return res;
}

std::vector<int> multi_set_intersection_unordered_map(std::vector<std::vector<int>>& nums) {
    std::map<int, int> countMap;
    std::vector<int> res;
    int desiredCount = nums.size();
    for(auto num : nums) {
        for(int i : num) {
            countMap[i]++;
        }
    }
    for(auto num : countMap) {
        if(num.second == desiredCount) {
            res.push_back(num.first);
        }
    }
    return res;
}


std::vector<int> using_ranges_multi_set_intersection(std::vector<std::vector<int>>& nums) {
    // initialize by the first vector
    std::vector<int> result(nums[0].begin(), nums[0].end());
    sort(result.begin(), result.end());

    for(int i = 1; i < nums.size(); ++i) {
        sort(nums[i].begin(), nums[i].end());
        std::vector<int> intersection;

        std::ranges::set_intersection(nums[i], result, back_inserter(intersection));

        result = intersection;
    }

    return result;
}

std::vector<int> binary_search_multi_set_intersection(const std::vector<std::vector<int>>& nums) {
    auto it = nums.begin();
    std::vector<int> result(it->begin(), it->end());
    it++;
    for (; it != nums.end(); it++) {
        // here we can change the intersection function to any regular scalar
        // or vector pair-set intersection algorithms.
        size_t inter_length = BSintersection(result.data(),
          result.size(), it->data(), it->size(),
          result.data());
        result.resize(inter_length);
    }
    return result;
}

std::vector<int> galloping_search_multi_set_intersection(const std::vector<std::vector<int>>& nums) {
    auto it = nums.begin();
    std::vector<int> result(it->begin(), it->end());
    it++;
    for (; it != nums.end(); it++) {
        // here we can change the intersection function to any regular scalar
        // or vector pair-set intersection algorithms.
        size_t inter_length = onesidedgallopingintersection(result.data(),
          result.size(), it->data(), it->size(),
          result.data());
        result.resize(inter_length);
    }
    return result;
}

std::vector<int> galloping_search_multi_set_intersection_unsorted(std::vector<std::vector<int>>& nums) {
    auto it = nums.begin();
    std::vector<int> result(it->begin(), it->end());
    sort(result.begin(), result.end());
    it++;
    for (; it != nums.end(); it++) {
        sort(it->begin(), it->end());
        // here we can change the intersection function to any regular scalar
        // or vector pair-set intersection algorithms.
        size_t inter_length = onesidedgallopingintersection(result.data(),
          result.size(), it->data(), it->size(),
          result.data());
        result.resize(inter_length);
    }
    return result;
}







/**
 * Branchless approach by N. Kurz.
 */
size_t scalar_branchless(const int *A, size_t lenA,
  const int *B, size_t lenB,
  int *Match) {

    const int *initMatch = Match;
    const int *endA = A + lenA;
    const int *endB = B + lenB;

    while (A < endA && B < endB) {
        int m = (*B == *A) ? 1 : 0;  // advance Match only if equal
        int a = (*B >= *A) ? 1 : 0;  // advance A if match or B ahead
        int b = (*B <= *A) ? 1 : 0;  // advance B if match or B behind

        *Match = *A;   // write the result regardless of match
        Match += m;    // but will be rewritten unless advanced
        A += a;
        B += b;
    }

    size_t count = Match - initMatch;
    return count;
}

std::vector<int> branchless_search_multi_set_intersection(const std::vector<std::vector<int>>& nums) {
    auto it = nums.begin();
    std::vector<int> result(it->begin(), it->end());
    it++;
    for (; it != nums.end(); it++) {
        // here we can change the intersection function to any regular scalar
        // or vector pair-set intersection algorithms.
        size_t inter_length = scalar_branchless(result.data(),
          result.size(), it->data(), it->size(),
          result.data());
        result.resize(inter_length);
    }
    return result;
}

std::vector<int> branchless_unrolled_search_multi_set_intersection(const std::vector<std::vector<int>>& nums) {
    auto it = nums.begin();
    std::vector<int> result(it->begin(), it->end());
    it++;
    for (; it != nums.end(); it++) {
        // here we can change the intersection function to any regular scalar
        // or vector pair-set intersection algorithms.
        size_t inter_length = scalar_branchless_unrolled(result.data(),
          result.size(), it->data(), it->size(),
          result.data());
        result.resize(inter_length);
    }
    return result;
}


static void BM_Intersection_multi_set_intersection_unordered_map(benchmark::State &state) {
    sorted_vectors = sorted_maps[state.range(0)][state.range(1)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(multi_set_intersection_unordered_map(sorted_vectors));
    }
}

static void BM_Intersection_multi_set_intersection_set_intersection(benchmark::State &state) {
    sorted_vectors = sorted_maps[state.range(0)][state.range(1)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(using_ranges_multi_set_intersection(sorted_vectors));
    }
}

static void BM_Intersection_multi_set_intersection_binary_search(benchmark::State &state) {
    sorted_vectors = sorted_maps[state.range(0)][state.range(1)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(binary_search_multi_set_intersection(sorted_vectors));
    }
}

static void BM_Intersection_multi_set_intersection_galloping_search(benchmark::State &state) {
    sorted_vectors = sorted_maps[state.range(0)][state.range(1)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(galloping_search_multi_set_intersection(sorted_vectors));
    }
}
static void BM_Intersection_multi_set_intersection_unsorted(benchmark::State &state) {
    sorted_vectors = sorted_maps[state.range(0)][state.range(1)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(galloping_search_multi_set_intersection_unsorted(sorted_vectors));
    }
}



static void BM_Intersection_multi_set_intersection_branchless(benchmark::State &state) {
    sorted_vectors = sorted_maps[state.range(0)][state.range(1)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(branchless_search_multi_set_intersection(sorted_vectors));
    }
}

static void BM_Intersection_multi_set_intersection_branchless_unrolled(benchmark::State &state) {
    sorted_vectors = sorted_maps[state.range(0)][state.range(1)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(branchless_unrolled_search_multi_set_intersection(sorted_vectors));
    }
}

static void BM_Intersection_multi_set_intersection_leapfrog(benchmark::State &state) {
    sorted_vectors = sorted_maps[state.range(0)][state.range(1)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(multi_set_intersection_leapfrog(sorted_vectors));
    }
}

static void BM_Intersection_multi_set_intersection_leapfrog2(benchmark::State &state) {
    sorted_vectors = sorted_maps[state.range(0)][state.range(1)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(multi_set_intersection_leapfrog2(sorted_vectors));
    }
}


//BENCHMARK(BM_Intersection_multi_set_intersection_unordered_map)
//  ->ArgsProduct({
//    benchmark::CreateDenseRange(2, 7, /*step=*/ 1),
//    benchmark::CreateRange(8, 262144, /*multi=*/ 8) })->Setup(DoSetupMultiSet);

BENCHMARK(BM_Intersection_multi_set_intersection_set_intersection)
  ->ArgsProduct({
    benchmark::CreateDenseRange(2, 7, /*step=*/ 1),
    benchmark::CreateRange(8, 262144, /*multi=*/ 8) })->Setup(DoSetupMultiSet);

BENCHMARK(BM_Intersection_multi_set_intersection_binary_search)
  ->ArgsProduct({
    benchmark::CreateDenseRange(2, 7, /*step=*/ 1),
    benchmark::CreateRange(8, 262144, /*multi=*/ 8) })->Setup(DoSetupMultiSet);
BENCHMARK(BM_Intersection_multi_set_intersection_galloping_search)
  ->ComputeStatistics("max", [](const std::vector<double>& v) -> double {
      return *(std::max_element(std::begin(v), std::end(v)));
  })
  ->ArgsProduct({
    benchmark::CreateDenseRange(2, 7, /*step=*/ 1),
    benchmark::CreateRange(8, 262144, /*multi=*/ 8) })->Setup(DoSetupMultiSet);
BENCHMARK(BM_Intersection_multi_set_intersection_branchless)
  ->ArgsProduct({
    benchmark::CreateDenseRange(2, 7, /*step=*/ 1),
    benchmark::CreateRange(8, 262144, /*multi=*/ 8) })->Setup(DoSetupMultiSet);
BENCHMARK(BM_Intersection_multi_set_intersection_branchless_unrolled)
  ->ArgsProduct({
    benchmark::CreateDenseRange(2, 7, /*step=*/ 1),
    benchmark::CreateRange(8, 262144, /*multi=*/ 8) })->Setup(DoSetupMultiSet);
//
BENCHMARK(BM_Intersection_multi_set_intersection_leapfrog2)
  ->ArgsProduct({
    benchmark::CreateDenseRange(2, 7, /*step=*/ 1),
    benchmark::CreateRange(8, 262144, /*multi=*/ 8) })->Setup(DoSetupMultiSet);

BENCHMARK(BM_Intersection_multi_set_intersection_leapfrog)
  ->ArgsProduct({
    benchmark::CreateDenseRange(2, 7, /*step=*/ 1),
    benchmark::CreateRange(8, 262144, /*multi=*/ 8) })->Setup(DoSetupMultiSet);





//BENCHMARK(BM_Intersection_multi_set_intersection_unsorted)
//  ->ArgsProduct({
//    benchmark::CreateDenseRange(2, 7, /*step=*/ 1),
//    benchmark::CreateRange(8, 262144, /*multi=*/ 8) })->Setup(DoSetupMultiSet);

BENCHMARK_MAIN();

/**
 * This is pure binary search
 * Used by BSintersectioncardinality below
 * @param array
 * @param pos
 * @param min
 * @return
 */
static size_t __BSadvanceUntil(const int * array, const size_t pos,
  const size_t length, const size_t min) {
    size_t lower = pos + 1;
    if (lower == length || array[lower] >= min) {
        return lower;
    }
    // can safely assume that length>0
    size_t upper = length - 1;
    if (array[upper] < min) {
        return length;
    }
    size_t mid;
    while (lower < upper) {
        mid = (lower + upper) / 2;
        if (array[mid] == min) {
            return mid;
        }

        if (array[mid] < min) {
            lower = mid + 1;
        } else {
            upper = mid;
        }
    }
    return upper;
}

/**
 * Based on binary search.
 */
size_t BSintersection(const int * set1, const size_t length1,
  const int * set2, const size_t length2, int *out) {
    if ((0 == length1) or (0 == length2))
        return 0;
    size_t answer = 0;
    size_t k1 = 0, k2 = 0;
    while (true) {
        if (set1[k1] < set2[k2]) {
            k1 = __BSadvanceUntil(set1, k1, length1, set2[k2]);
            if (k1 == length1)
                return answer;
        }
        if (set2[k2] < set1[k1]) {
            k2 = __BSadvanceUntil(set2, k2, length2, set1[k1]);
            if (k2 == length2)
                return answer;
        } else {
            out[answer++] = set1[k1];
            ++k1;
            if (k1 == length1)
                break;
            ++k2;
            if (k2 == length2)
                break;
        }
    }
    return answer;
}

/**
 * This is often called galloping or exponential search.
 *
 * Used by frogintersectioncardinality below
 *
 * Based on binary search...
 * Find the smallest integer larger than pos such
 * that array[pos]>= min.
 * If none can be found, return array.length.
 * From code by O. Kaser.
 */
static size_t __frogadvanceUntil(const int * array, const size_t pos,
  const size_t length, const size_t min) {
    size_t lower = pos + 1;

    // special handling for a possibly common sequential case
    if ((lower >= length) or (array[lower] >= min)) {
        return lower;
    }

    size_t spansize = 1; // could set larger
    // bootstrap an upper limit

    while ((lower + spansize < length) and (array[lower + spansize] < min))
        spansize *= 2;
    size_t upper = (lower + spansize < length) ? lower + spansize : length - 1;

    // maybe we are lucky (could be common case when the seek ahead expected to be small and sequential will otherwise make us look bad)
    //if (array[upper] == min) {
    //    return upper;
    //}

    if (array[upper] < min) {// means array has no item >= min
        return length;
    }

    // we know that the next-smallest span was too small
    lower += (spansize / 2);

    // else begin binary search
    size_t mid = 0;
    while (lower + 1 != upper) {
        mid = (lower + upper) / 2;
        if (array[mid] == min) {
            return mid;
        } else if (array[mid] < min)
            lower = mid;
        else
            upper = mid;
    }
    return upper;

}

size_t onesidedgallopingintersection(const int * smallset,
  const size_t smalllength, const int * largeset,
  const size_t largelength, int * out) {
    if(largelength < smalllength) return onesidedgallopingintersection(largeset,largelength,smallset,smalllength,out);
    if (0 == smalllength)
        return 0;
    const int * const initout(out);
    size_t k1 = 0, k2 = 0;
    while (true) {
        if (largeset[k1] < smallset[k2]) {
            k1 = __frogadvanceUntil(largeset, k1, largelength, smallset[k2]);
            if (k1 == largelength)
                break;
        }
    midpoint: if (smallset[k2] < largeset[k1]) {
            ++k2;
            if (k2 == smalllength)
                break;
        } else {
            *out++ = smallset[k2];
            ++k2;
            if (k2 == smalllength)
                break;
            k1 = __frogadvanceUntil(largeset, k1, largelength, smallset[k2]);
            if (k1 == largelength)
                break;
            goto midpoint;
        }
    }
    return out - initout;

}