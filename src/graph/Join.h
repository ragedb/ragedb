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

#ifndef RAGEDB_JOIN_H
#define RAGEDB_JOIN_H

#include <algorithm>
#include <vector>

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
void leapfrogJoin( std::vector<C<A...>>& indexes, std::vector<T>& resultSet ) {
    RandomAccessIterator its[indexes.size()];
    T value, max;
    int it;

    //1. Check if any index is empty => Intersection empty
    for( auto& index : indexes )
        if( index.size() == 0 )
            return;

    //2. Sort indexes by their first value and do some initial iterators book-keeping!
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
std::vector<T> leapfrogJoin( std::vector<C<A...>>& indexes ) {
    std::vector<T> resultSet;

    leapfrogJoin( indexes, resultSet );
    return resultSet;
}
#endif// RAGEDB_JOIN_H
