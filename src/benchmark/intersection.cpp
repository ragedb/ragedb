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

static int MAX = 1000000;

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

std::vector<int> intersection_sort_both(std::vector<int>& nums1, std::vector<int>& nums2) {

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
  roaring::Roaring map1;
  roaring::Roaring map2;
  std::vector<int> result;

  for(int i = 0 ; i<num1.size() ; i++){
    map1.add(num1[i]);
  }
  for(int i = 0 ; i<num2.size() ; i++){
    map1.add(num2[i]);
  }

  map1 &= map2;

  result.reserve(map1.cardinality());
  uint32_t* array = new uint32_t[map1.cardinality()];
  map1.toUint32Array(array);
  result.assign(array, array + map1.cardinality());
  delete[] array;
  return result;
}

std::vector<int> intersection_roaring(std::vector<int>& num1, std::vector<int>& num2) {
  roaring::Roaring map1;
  roaring::Roaring map2;
  roaring::Roaring map3;
  std::vector<int> result;

  for(int i = 0 ; i<num1.size() ; i++){
    map1.add(num1[i]);
  }
  for(int i = 0 ; i<num2.size() ; i++){
    map1.add(num2[i]);
  }

  map3 = map1 & map2;

  result.reserve(map3.cardinality());
  uint32_t* array = new uint32_t[map3.cardinality()];
  map3.toUint32Array(array);
  result.assign(array, array + map3.cardinality());
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
std::vector<int> first = generate_data(32768);
std::vector<int> second = generate_data(32768);
std::unordered_map<int, std::vector<int>> map1;
std::unordered_map<int, std::vector<int>> map2;

void DoSetup(const benchmark::State& state) {
  map1[8] = generate_data(8);
  map2[8] = generate_data(8);
  map1[64] = generate_data(64);
  map2[64] = generate_data(64);
  map1[512] = generate_data(512);
  map2[512] = generate_data(512);
  map1[4096] = generate_data(4096);
  map2[4096] = generate_data(4096);
  map1[32768] = generate_data(32768);
  map2[32768] = generate_data(32768);


  // Setup/Teardown should never be called with any thread_idx != 0.
  assert(state.thread_index() == 0);
}

static void BM_Intersection_unordered_set(benchmark::State &state) {
  first = map1[state.range(0)];
  second = map2[state.range(0)];
  for (auto _ : state) {
    intersection_unordered_set(first, second);
  }
}

static void BM_Intersection_unordered_map(benchmark::State &state) {
//  std::vector<int> first = generate_data(state.range(0));
//  std::vector<int> second = generate_data(state.range(0));
  for (auto _ : state) {
    intersection_unordered_map(first, second);
  }
}

static void BM_Intersection_two_sets(benchmark::State &state) {
//  std::vector<int> first = generate_data(state.range(0));
//  std::vector<int> second = generate_data(state.range(0));
  for (auto _ : state) {
    intersection_two_sets(first, second);
  }
}

static void BM_Intersection_sort_both(benchmark::State &state) {
  first = map1[state.range(0)];
  second = map2[state.range(0)];

  for (auto _ : state) {
    benchmark::DoNotOptimize(intersection_sort_both(first, second));
  }
}

static void BM_Intersection_roaring_in_place(benchmark::State &state) {
//  std::vector<int> first = generate_data(state.range(0));
//  std::vector<int> second = generate_data(state.range(0));
  for (auto _ : state) {
    intersection_roaring_in_place(first, second);
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
//  std::vector<int> first = generate_data(state.range(0));
//  std::vector<int> second = generate_data(state.range(0));
  for (auto _ : state) {
    intersection_binary_search(first, second);
  }
}

static void BM_Intersection_sorted_early_term(benchmark::State &state) {
  first = map1[state.range(0)];
  second = map2[state.range(0)];

  for (auto _ : state) {
    benchmark::DoNotOptimize(intersection_sorted_early_termination(first, second));
  }
}

// Register the function as a benchmark
//BENCHMARK(BM_Intersection_two_sets)->Range(8, 8 << 10);
//BENCHMARK(BM_Intersection_binary_search)->Range(8, 8 << 10);
//BENCHMARK(BM_Intersection_unordered_map)->Range(8, 8 << 10);
//BENCHMARK(BM_Intersection_unordered_set)->Range(8, 8 << 10);
//BENCHMARK(BM_Intersection_roaring_in_place)->Range(8, 8 << 10);
BENCHMARK(BM_Intersection_roaring)->Range(8, 8 << 12)->Setup(DoSetup);
BENCHMARK(BM_Intersection_sort_both)->Range(8, 8 << 12)->Setup(DoSetup);
BENCHMARK(BM_Intersection_sorted_early_term)->Range(8, 8 << 12)->Setup(DoSetup);

