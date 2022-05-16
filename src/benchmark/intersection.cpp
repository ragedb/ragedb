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
#include <unordered_set>
#include <roaring/roaring64map.hh>

static int MAX = 1000;

std::vector<int> generate_data(size_t size) {
  using value_type = int;
  static std::uniform_int_distribution<value_type> distribution(
    0,
    MAX);
  static std::default_random_engine generator;

  std::vector<value_type> data(size);
  std::generate(data.begin(), data.end(), []() { return distribution(generator); });
  return data;
}

std::vector<int> first = generate_data(32768);
std::vector<int> second = generate_data(32768);
std::unordered_map<int, std::vector<int>> map1;
std::unordered_map<int, std::vector<int>> map2;
std::unordered_map<int, std::vector<int>> sorted_map1;
std::unordered_map<int, std::vector<int>> sorted_map2;

void DoSetup(const benchmark::State& state) {
    std::vector<int> sizes = { 8, 64, 512, 4096, 32768 };
    for(auto size : sizes) {
        map1[size] = generate_data(size);
        map2[size] = generate_data(size);
    }

    sorted_map1.insert(map1.begin(), map1.end());
    sorted_map2.insert(map2.begin(), map2.end());

    for(auto size : sizes) {
        std::ranges::sort(sorted_map1[size]);
        std::ranges::sort(sorted_map2[size]);
    }

    // Setup/Teardown should never be called with any thread_idx != 0.
    assert(state.thread_index() == 0);
}

std::vector<int> intersection_unordered_set(std::vector<int>& nums1, std::vector<int>& nums2) {
  std::unordered_set<int> st(nums1.begin(), nums1.end());
  std::vector<int> ans;

  for(auto num : nums2){
    if(st.find(num)!=st.end()){
      ans.push_back(num);
      st.erase(num);
    }
  }
  return ans;
}

std::vector<int> intersection_unordered_map(std::vector<int>& nums1, std::vector<int>& nums2) {
  std::unordered_map<int, int> mp;
  for (int i = 0; i < nums1.size(); i++) {
    if (mp[nums1[i]] < 1) {
      mp[nums1[i]]++;
    }
  }
  std::vector<int> res;
  for (int i = 0; i < nums2.size(); i++) {
    if (mp[nums2[i]] == 1) {
      res.push_back(nums2[i]);
      mp[nums2[i]]--;
    }
  }
  return res;
}

std::vector<int> intersection_two_sets(std::vector<int>& nums1, std::vector<int>& nums2) {
  std::set<int>s1,s2;

  for(auto it:nums1){
    s1.insert(it);
  }
  for(auto it:nums2){
    s2.insert(it);
  }
  std::vector<int>ans;
  for(auto it:s2){
    if(s1.find(it)!=s1.end()){
      ans.push_back(it);
    }
  }
  return ans;
}

std::vector<int> intersection_both(std::vector<int>& nums1, std::vector<int>& nums2) {
  sort (nums1.begin(), nums1.end());
  sort (nums2.begin(), nums2.end());
  std::map <int, int> map;
  std::vector<int> ret;
  int i = 0;
  int j = 0;

  while (i < nums1.size() && j < nums2.size()) {
    if (nums1[i] == nums2[j]){
      map.emplace(nums1[i], i);
      i++;
      j++;
      continue;
    }
    else if (nums1[i] > nums2[j])
      j++;
    else if (nums1[i] < nums2[j])
      i++;
  }

  for (auto it : map) {
    ret.emplace_back(it.first);
  }

  return ret;
}

std::vector<int> intersection_roaring_in_place(std::vector<int>& num1, std::vector<int>& num2) {
  roaring::Roaring roaring1;
  roaring::Roaring roaring2;
  std::vector<int> result;

  for(int i = 0 ; i<num1.size() ; i++){
      roaring1.add(num1[i]);
  }
  for(int i = 0 ; i<num2.size() ; i++){
      roaring1.add(num2[i]);
  }

  roaring1 &= roaring2;

  result.reserve(roaring1.cardinality());
  uint32_t* array = new uint32_t[roaring1.cardinality()];
  roaring1.toUint32Array(array);
  result.assign(array, array + roaring1.cardinality());
  delete[] array;
  return result;
}

std::vector<int> intersection_roaring(std::vector<int>& num1, std::vector<int>& num2) {
  roaring::Roaring roaring1;
  roaring::Roaring roaring2;
  roaring::Roaring roaring3;
  std::vector<int> result;

  for(int i = 0 ; i<num1.size() ; i++){
      roaring1.add(num1[i]);
  }
  for(int i = 0 ; i<num2.size() ; i++){
      roaring1.add(num2[i]);
  }

  roaring3 = roaring1 & roaring2;

  result.reserve(roaring3.cardinality());
  uint32_t* array = new uint32_t[roaring3.cardinality()];
  roaring3.toUint32Array(array);
  result.assign(array, array + roaring3.cardinality());
  delete[] array;
  return result;
}

std::vector<int> binarySearch(std::vector<int> a1, std::vector<int> a2){
  std::vector<int> res;
  for(int i=0;i<a1.size();i++){
    int s = 0;
    int e = a2.size()-1;
    while(s<=e){
      int mid = s+(e-s)/2;
      if(a2[mid] == a1[i]){
        if(find(res.begin(),res.end(), a2[mid])!=res.end())
          break;
        res.push_back(a2[mid]);
        break;
      }
      else if(a2[mid]>a1[i])
        e = mid-1;
      else
        s = mid+1;
    }
  }
  return res;
}

std::vector<int> intersection_binary_search(std::vector<int>& nums1, std::vector<int>& nums2) {
  sort(nums1.begin(),nums1.end());
  sort(nums2.begin(),nums2.end());
  int n1 = nums1.size();
  int n2 = nums2.size();

  if(n1<n2)
    return binarySearch(nums1,nums2);
  else
    return binarySearch(nums2,nums1);
}

bool isInArray(std::vector<int> &nums2, int &start_idx, int last_number){
  int end_idx = nums2.size()- 1;

  while(start_idx <= end_idx){
    int middle_idx = (start_idx + end_idx)/2;

    if(nums2[middle_idx] < last_number){
      start_idx = middle_idx + 1;
    }
    else if (nums2[middle_idx] > last_number){
      end_idx = middle_idx - 1;
    }
    else{
      start_idx = middle_idx;
      return true;
    }
  }
  return false;
}

std::vector<int> intersection_sorted_early_termination(std::vector<int>& nums1, std::vector<int>& nums2) {
  std::vector<int> intersection;
  if(nums1.empty() || nums2.empty()){
    return intersection;
  }
  std::sort(nums1.begin(), nums1.end());
  std::sort(nums2.begin(), nums2.end());

  int last_number = -1;
  int start_idx = 0;
  int end_idx = nums2.size()-1;
  for(size_t i = 0; i < nums1.size(); ++i){
    if(nums1[i]!=last_number){
      last_number = nums1[i];

      if(nums2[end_idx] < last_number){
        // max number in nums2 is smaller than current nr in nums1
        break;
      }

      if(isInArray(nums2, start_idx, last_number)){
        intersection.push_back(last_number);
      }
    }
  }
  return intersection;
}

std::vector<int> using_ranges_set_intersection(std::vector<int>& nums1, std::vector<int>& nums2) {
    sort(nums1.begin(),nums1.end());
    sort(nums2.begin(),nums2.end());
    std::vector<int>ans;
    std::ranges::set_intersection(nums1,nums2,back_inserter(ans));
    return ans;
}

// Already In Order
std::vector<int> intersection_in_order_binary_search(std::vector<int>& nums1, std::vector<int>& nums2) {
    int n1 = nums1.size();
    int n2 = nums2.size();

    if(n1<n2)
        return binarySearch(nums1,nums2);
    else
        return binarySearch(nums2,nums1);
}

std::vector<int> intersection_in_order_both(std::vector<int>& nums1, std::vector<int>& nums2) {
    std::map <int, int> map;
    std::vector<int> ret;
    int i = 0;
    int j = 0;

    while (i < nums1.size() && j < nums2.size()) {
        if (nums1[i] == nums2[j]){
            map.emplace(nums1[i], i);
            i++;
            j++;
            continue;
        }
        else if (nums1[i] > nums2[j])
            j++;
        else if (nums1[i] < nums2[j])
            i++;
    }

    for (auto it : map) {
        ret.emplace_back(it.first);
    }

    return ret;
}

std::vector<int> intersection_in_order_early_termination(std::vector<int>& nums1, std::vector<int>& nums2) {
    std::vector<int> intersection;
    if(nums1.empty() || nums2.empty()){
        return intersection;
    }

    int last_number = -1;
    int start_idx = 0;
    int end_idx = nums2.size()-1;
    for(size_t i = 0; i < nums1.size(); ++i){
        if(nums1[i]!=last_number){
            last_number = nums1[i];

            if(nums2[end_idx] < last_number){
                // max number in nums2 is smaller than current nr in nums1
                break;
            }

            if(isInArray(nums2, start_idx, last_number)){
                intersection.push_back(last_number);
            }
        }
    }
    return intersection;
}

std::vector<int> using_ranges_in_order_set_intersection(std::vector<int>& nums1, std::vector<int>& nums2) {
    std::vector<int>ans;
    std::ranges::set_intersection(nums1,nums2,back_inserter(ans));
    return ans;
}

static void BM_Intersection_unordered_set(benchmark::State &state) {
  first = map1[state.range(0)];
  second = map2[state.range(0)];
  for (auto _ : state) {
    benchmark::DoNotOptimize(intersection_unordered_set(first, second));
  }
}

static void BM_Intersection_unordered_map(benchmark::State &state) {
    first = map1[state.range(0)];
    second = map2[state.range(0)];
    for (auto _ : state) {
      benchmark::DoNotOptimize(intersection_unordered_map(first, second));
    }
}

static void BM_Intersection_two_sets(benchmark::State &state) {
    first = map1[state.range(0)];
    second = map2[state.range(0)];
    for (auto _ : state) {
      benchmark::DoNotOptimize(intersection_two_sets(first, second));
    }
}

static void BM_Intersection_sort_both(benchmark::State &state) {
  first = map1[state.range(0)];
  second = map2[state.range(0)];

  for (auto _ : state) {
    benchmark::DoNotOptimize(intersection_both(first, second));
  }
}

static void BM_Intersection_roaring_in_place(benchmark::State &state) {
    first = map1[state.range(0)];
    second = map2[state.range(0)];
    for (auto _ : state) {
      benchmark::DoNotOptimize(intersection_roaring_in_place(first, second));
    }
}

static void BM_Intersection_roaring(benchmark::State &state) {
  first = map1[state.range(0)];
  second = map2[state.range(0)];

  for (auto _ : state) {
    benchmark::DoNotOptimize(intersection_roaring(first, second));
  }
}

static void BM_Intersection_binary_search(benchmark::State &state) {
    first = map1[state.range(0)];
    second = map2[state.range(0)];
    for (auto _ : state) {
      benchmark::DoNotOptimize(intersection_binary_search(first, second));
    }
}

static void BM_Intersection_sorted_early_term(benchmark::State &state) {
  first = map1[state.range(0)];
  second = map2[state.range(0)];

  for (auto _ : state) {
    benchmark::DoNotOptimize(intersection_sorted_early_termination(first, second));
  }
}

static void BM_Intersection_set_intersection(benchmark::State &state) {
    first = map1[state.range(0)];
    second = map2[state.range(0)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(using_ranges_in_order_set_intersection(first, second));
    }
}

// Already In Order

static void BM_Intersection_in_order_binary_search(benchmark::State &state) {
    first = sorted_map1[state.range(0)];
    second = sorted_map2[state.range(0)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(intersection_in_order_binary_search(first, second));
    }
}

static void BM_Intersection_in_order_both(benchmark::State &state) {
    first = sorted_map1[state.range(0)];
    second = sorted_map2[state.range(0)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(intersection_in_order_both(first, second));
    }
}

static void BM_Intersection_in_order_early_term(benchmark::State &state) {
    first = sorted_map1[state.range(0)];
    second = sorted_map2[state.range(0)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(intersection_in_order_early_termination(first, second));
    }
}

static void BM_Intersection_in_order_set_intersection(benchmark::State &state) {
    first = sorted_map1[state.range(0)];
    second = sorted_map2[state.range(0)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(using_ranges_in_order_set_intersection(first, second));
    }
}
static size_t __BSadvanceUntil2(const int * array, const size_t pos,
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

size_t BSintersection2(const int * set1, const size_t length1,
  const int * set2, const size_t length2, int *out) {
    if ((0 == length1) or (0 == length2))
        return 0;
    size_t answer = 0;
    size_t k1 = 0, k2 = 0;
    while (true) {
        if (set1[k1] < set2[k2]) {
            k1 = __BSadvanceUntil2(set1, k1, length1, set2[k2]);
            if (k1 == length1)
                return answer;
        }
        if (set2[k2] < set1[k1]) {
            k2 = __BSadvanceUntil2(set2, k2, length2, set1[k1]);
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

std::vector<int> binary_search_intersection(const std::vector<int>& nums1, std::vector<int>& nums2s) {

    std::vector<int> result(nums1.begin(), nums1.end());

        // here we can change the intersection function to any regular scalar
        // or vector pair-set intersection algorithms.
        size_t inter_length = BSintersection2(result.data(),
          result.size(), nums2s.data(), nums2s.size(),
          result.data());
        result.resize(inter_length);

    return result;
}

static void BM_Intersection_in_order_binary_search_intersection(benchmark::State &state) {
    first = sorted_map1[state.range(0)];
    second = sorted_map2[state.range(0)];

    for (auto _ : state) {
        benchmark::DoNotOptimize(binary_search_intersection(first, second));
    }
}







// Register the function as a benchmark

// Not Sorted
//BENCHMARK(BM_Intersection_two_sets)->Range(8, 8 << 12)->Setup(DoSetup);
//BENCHMARK(BM_Intersection_binary_search)->Range(8, 8 << 12)->Setup(DoSetup);
//BENCHMARK(BM_Intersection_unordered_map)->Range(8, 8 << 12)->Setup(DoSetup);
//BENCHMARK(BM_Intersection_unordered_set)->Range(8, 8 << 12)->Setup(DoSetup);
//BENCHMARK(BM_Intersection_roaring_in_place)->Range(8, 8 << 12)->Setup(DoSetup);
//BENCHMARK(BM_Intersection_roaring)->Range(8, 8 << 12)->Setup(DoSetup);
//BENCHMARK(BM_Intersection_sort_both)->Range(8, 8 << 12)->Setup(DoSetup);
//BENCHMARK(BM_Intersection_sorted_early_term)->Range(8, 8 << 12)->Setup(DoSetup);
//BENCHMARK(BM_Intersection_set_intersection)->Range(8, 8 << 12)->Setup(DoSetup);

// Already Sorted

//BENCHMARK(BM_Intersection_in_order_both)->Range(8, 8 << 12)->Setup(DoSetup);
//BENCHMARK(BM_Intersection_in_order_binary_search)->Range(8, 8 << 12)->Setup(DoSetup);
//BENCHMARK(BM_Intersection_in_order_early_term)->Range(8, 8 << 12)->Setup(DoSetup);
//BENCHMARK(BM_Intersection_in_order_set_intersection)->Range(8, 8 << 12)->Setup(DoSetup);
//BENCHMARK(BM_Intersection_in_order_binary_search_intersection)->Range(8, 8 << 12)->Setup(DoSetup);
