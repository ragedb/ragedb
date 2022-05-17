#include <catch2/catch.hpp>

#if defined(__GNUC__) || defined(__HP_aCC) || defined(__clang__)
#define INLINE inline __attribute__((always_inline))
#else
#define INLINE __forceinline
#endif

unsigned int Factorial(unsigned int number)
{
  return number <= 1 ? number : Factorial(number - 1) * number;
}

TEST_CASE("Factorials are computed", "[factorial]")
{
    REQUIRE(Factorial(1) == 1);
    REQUIRE(Factorial(2) == 2);
    REQUIRE(Factorial(3) == 6);
}

template< class T>
T branchless_lower_bound(std::vector<T>& vec, T value) {
    auto len = vec.size();
    int *base = vec.data();
      while (len > 1) {
        auto half = len / 2;
        base = (base[half] < value ? &base[half] : base);
        len -= half;
    }
    return *(base + (*base < value));
}

template< class T>
T branch_less_upper_bound(std::vector<T>& vec, T value) {
    auto len = vec.size();
    int *base = vec.data();
    while (len > 1) {
        auto half = len / 2;
        base = (value < base[half] ? base : &base[half]);
        len -= half;
    }

    return *(base + (*base <= value)); // passes
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
    std::ranges::advance(first, *first < value);
    return first;
}

TEST_CASE("Lower Bound is correct", "[lower_bound]") {
    std::vector<int> test = {1,3,5,7,9};
    REQUIRE(branch_less_upper_bound<int>(test, 4) == std::upper_bound(test.begin(), test.end(), 4).operator*());
    REQUIRE(branch_less_upper_bound<int>(test, 5) == std::upper_bound(test.begin(), test.end(), 5).operator*());
    REQUIRE(branch_less_upper_bound<int>(test, 11) == std::upper_bound(test.begin(), test.end(), 11).operator*());

    REQUIRE(*branch_less_upper_bound_iterators(test.begin(), test.end(), 5) == 5);
    REQUIRE(*branch_less_upper_bound_iterators(test.begin(), test.end(), 4) == 5);
    REQUIRE(branch_less_upper_bound_iterators(test.begin(), test.end(), 11) == test.end());

//    REQUIRE(branch_less_upper_bound_iterators(test.begin(), test.end(), 4) == std::upper_bound(test.begin(), test.end(), 4));
//    REQUIRE(branch_less_upper_bound_iterators(test.begin(), test.end(), 5) == std::upper_bound(test.begin(), test.end(), 5));
//    REQUIRE(branch_less_upper_bound_iterators(test.begin(), test.end(), 11) == std::upper_bound(test.begin(), test.end(), 11));



    REQUIRE(branchless_lower_bound<int>(test, 4) == std::lower_bound(test.begin(), test.end(), 4).operator*());
    REQUIRE(branchless_lower_bound<int>(test, 5) == std::lower_bound(test.begin(), test.end(), 5).operator*());
    REQUIRE(branchless_lower_bound<int>(test, 11) == std::lower_bound(test.begin(), test.end(), 11).operator*());

}
