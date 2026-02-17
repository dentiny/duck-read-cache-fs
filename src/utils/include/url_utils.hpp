#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

struct ParsedURL {
	string scheme;
	string host;
	string path;
	string query;
	string fragment;
	string url_without_query;
};

class URLUtils {
public:
	static string StripQueryAndFragment(const string &url);
	static ParsedURL ParseURL(const string &url);
};

} // namespace duckdb
