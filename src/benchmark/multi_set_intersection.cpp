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

std::vector<int> generate_sorted_data(size_t size) {
    auto randomNumberBetween = [](int low, int high) {
        auto randomFunc = [distribution_ = std::uniform_int_distribution<int>(low, high),
                            random_engine_ = std::mt19937{ std::random_device{}() }]() mutable {
            return distribution_(random_engine_);
        };
        return randomFunc;
    };

    std::vector<int> data;
    std::generate_n(std::back_inserter(data), size, randomNumberBetween(1, size * 4));

    std::ranges::sort(data);
    return data;
}

void DoSetupMultiSet(const benchmark::State& state) {
    std::vector<int> counts = { 2, 3, 4, 5, 6, 7 };
    std::vector<int> sizes = { 8, 64, 512, 4096, 32768, 262144 };
    for (auto count : counts) {
        for (auto size : sizes) {
            std::vector<std::vector<int>> vectors;
            for (int i = 0; i < count; i++) {
                vectors.emplace_back(generate_sorted_data(size));
            }
            sorted_maps[count][size] = vectors;
        }
    }

    // Setup/Teardown should never be called with any thread_idx != 0.
    assert(state.thread_index() == 0);
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


BENCHMARK(BM_Intersection_multi_set_intersection_unordered_map)
  ->ArgsProduct({
    benchmark::CreateDenseRange(2, 7, /*step=*/ 1),
    benchmark::CreateRange(8, 262144, /*multi=*/ 8) })->Setup(DoSetupMultiSet);

BENCHMARK(BM_Intersection_multi_set_intersection_set_intersection)
  ->ArgsProduct({
    benchmark::CreateDenseRange(2, 7, /*step=*/ 1),
    benchmark::CreateRange(8, 262144, /*multi=*/ 8) })->Setup(DoSetupMultiSet);

BENCHMARK(BM_Intersection_multi_set_intersection_binary_search)
  ->ArgsProduct({
    benchmark::CreateDenseRange(2, 7, /*step=*/ 1),
    benchmark::CreateRange(8, 262144, /*multi=*/ 8) })->Setup(DoSetupMultiSet);
BENCHMARK(BM_Intersection_multi_set_intersection_galloping_search)
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