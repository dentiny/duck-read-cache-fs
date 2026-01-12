// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Adapted from abseil optional unit test implementation.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "optional.hpp"

#include <functional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace duckdb {

struct Tracker {
	Tracker() : value(0) {
		++ctor;
	}
	explicit Tracker(int v) : value(v) {
		++ctor;
	}
	Tracker(const Tracker &other) : value(other.value) {
		++copy;
	}
	Tracker(Tracker &&other) noexcept : value(other.value) {
		++move;
	}
	Tracker &operator=(const Tracker &other) {
		value = other.value;
		++copy_assign;
		return *this;
	}
	Tracker &operator=(Tracker &&other) noexcept {
		value = other.value;
		++move_assign;
		return *this;
	}
	~Tracker() {
		++dtor;
	}

	static void Reset() {
		ctor = copy = move = copy_assign = move_assign = dtor = 0;
	}

	int value;

	static int ctor;
	static int copy;
	static int move;
	static int copy_assign;
	static int move_assign;
	static int dtor;
};

int Tracker::ctor = 0;
int Tracker::copy = 0;
int Tracker::move = 0;
int Tracker::copy_assign = 0;
int Tracker::move_assign = 0;
int Tracker::dtor = 0;

struct Comparable {
	explicit Comparable(int v = 0) : v(v) {
	}
	int v;
};

inline bool operator==(const Comparable &a, const Comparable &b) {
	return a.v == b.v;
}
inline bool operator<(const Comparable &a, const Comparable &b) {
	return a.v < b.v;
}

} // namespace duckdb

using namespace duckdb; // NOLINT

TEST_CASE("Default and nullopt", "[optional]") {
	optional<int> empty;
	REQUIRE(!empty);

	optional<int> also_empty(nullopt);
	REQUIRE(!also_empty);

	optional<int> with_value(5);
	REQUIRE(with_value);
	REQUIRE(*with_value == 5);
}

TEST_CASE("In-place and initializer-list construction", "[optional]") {
	optional<string> name(in_place, "duckdb");
	REQUIRE(name);
	REQUIRE(name->size() == 6u);

	optional<vector<int>> numbers(in_place, {1, 2, 3});
	REQUIRE(numbers);
	REQUIRE(numbers->size() == 3u);
	REQUIRE(numbers->at(1) == 2);
}

TEST_CASE("Copy and move semantics", "[optional]") {
	Tracker::Reset();
	optional<Tracker> src(in_place, 10);
	optional<Tracker> copy(src);
	optional<Tracker> moved(std::move(src));

	REQUIRE(copy);
	REQUIRE(moved);
	REQUIRE(copy->value == 10);
	REQUIRE(moved->value == 10);

	REQUIRE(Tracker::ctor == 1);
	REQUIRE(Tracker::copy == 1);
	REQUIRE(Tracker::move == 1);
}

TEST_CASE("Assignment and reset", "[optional]") {
	Tracker::Reset();
	optional<Tracker> opt;
	opt = Tracker(3);
	REQUIRE(opt);
	REQUIRE(opt->value == 3);

	opt = nullopt;
	REQUIRE(!opt);

	optional<Tracker> lhs(in_place, 7);
	optional<Tracker> rhs(in_place, 9);
	lhs = rhs;
	REQUIRE(lhs);
	REQUIRE(lhs->value == 9);

	lhs = std::move(rhs);
	REQUIRE(lhs);
	REQUIRE(lhs->value == 9);
}

TEST_CASE("Emplace", "[optional]") {
	optional<string> text;
	string &ref1 = text.emplace("abc");
	REQUIRE(&ref1 == &*text);
	REQUIRE(text);
	REQUIRE(text->size() == 3u);

	string &ref2 = text.emplace(5, 'z');
	REQUIRE(&ref2 == &*text);
	REQUIRE(text->size() == 5u);
}

TEST_CASE("Accessors and exceptions", "[optional]") {
	optional<string> text("hello");
	REQUIRE(text->size() == 5u);
	REQUIRE((*text)[1] == 'e');

	const optional<string> ctext(text);
	REQUIRE(ctext->size() == 5u);
	REQUIRE((*ctext)[4] == 'o');

	optional<int> empty;
	REQUIRE_THROWS_AS(empty.value(), bad_optional_access);
}

TEST_CASE("value_or", "[optional]") {
	optional<int> empty;
	optional<int> filled(8);
	REQUIRE(empty.value_or(42) == 42);
	REQUIRE(filled.value_or(42) == 8);
	REQUIRE(optional<string>().value_or("fallback") == "fallback");
}

TEST_CASE("swap", "[optional]") {
	optional<int> a;
	optional<int> b(1);
	swap(a, b);
	REQUIRE(a);
	REQUIRE(*a == 1);
	REQUIRE(!b);

	optional<int> c(2);
	optional<int> d(3);
	c.swap(d);
	REQUIRE(*c == 3);
	REQUIRE(*d == 2);
}

TEST_CASE("Comparisons", "[optional]") {
	optional<int> e1;
	optional<int> e2;
	optional<int> one(1);
	optional<int> two(2);

	REQUIRE(e1 == e2);
	REQUIRE(e1 == nullopt);
	REQUIRE(one != e1);
	REQUIRE(one < two);
	REQUIRE(two > one);
	REQUIRE(one == 1);
	REQUIRE(1 == one);
	REQUIRE(two > 1);

	optional<Comparable> ca(Comparable(5));
	optional<Comparable> cb(Comparable(6));
	REQUIRE(ca < cb);
	REQUIRE(cb > ca);
}

TEST_CASE("make_optional helpers", "[optional]") {
	auto opt_int = make_optional(7);
	REQUIRE(static_cast<bool>(opt_int));
	REQUIRE(*opt_int == 7);

	auto opt_str = make_optional<string>(3, 'a');
	REQUIRE(opt_str);
	REQUIRE(*opt_str == "aaa");

	auto opt_vec = make_optional<vector<int>>({1, 2, 3});
	REQUIRE(opt_vec);
	REQUIRE(opt_vec->size() == 3u);
}

TEST_CASE("hash", "[optional]") {
	std::hash<optional<int>> h;
	size_t a = h(nullopt);
	size_t b = h(optional<int>(5));
	size_t c = h(optional<int>(6));
	REQUIRE(a != b);
	REQUIRE(b != c);
}

int main(int argc, char **argv) {
	int result = Catch::Session().run(argc, argv);
	return result;
}
