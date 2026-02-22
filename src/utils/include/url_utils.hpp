#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

//! Parsed URL components
struct ParsedURL {
	string scheme;            // e.g., "http", "https", "s3"
	string host;              // e.g., "example.com", "bucket.s3.amazonaws.com"
	string path;              // e.g., "/path/to/file.parquet"
	string query;             // e.g., "param1=value1&param2=value2"
	string fragment;          // e.g., "section1"
	string url_without_query; // Full URL without query parameters and fragment
};

//! URL parsing and manipulation utilities
class URLUtils {
public:
	//! Strip query parameters and fragment from a URL
	//! Example: "https://example.com/file.parquet?version=1#section" -> "https://example.com/file.parquet"
	static string StripQueryAndFragment(const string &url);

	//! Parse URL into components
	//! Parses scheme, host, path, query, and fragment from a URL
	//! Example: "https://example.com:8080/path/file?param=value#fragment"
	static ParsedURL ParseURL(const string &url);
};

struct SanitizedCachePath {
	explicit SanitizedCachePath(const string &url);
	const string &Path() const {
		return path;
	}
	operator const string &() const {
		return path;
	}

private:
	string path;
};

} // namespace duckdb
