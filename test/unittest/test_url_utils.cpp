#include "catch/catch.hpp"

#include "duckdb/common/string.hpp"
#include "url_utils.hpp"

using namespace duckdb;

TEST_CASE("URLUtils::StripQueryAndFragment - Basic cases", "[url_utils]") {
	// URL without query or fragment
	REQUIRE(URLUtils::StripQueryAndFragment("https://example.com/file.parquet") == "https://example.com/file.parquet");

	// URL with query parameters
	REQUIRE(URLUtils::StripQueryAndFragment("https://example.com/file.parquet?version=1") ==
	        "https://example.com/file.parquet");

	// URL with multiple query parameters
	REQUIRE(URLUtils::StripQueryAndFragment("https://example.com/file.parquet?version=1&format=snappy") ==
	        "https://example.com/file.parquet");

	// URL with fragment
	REQUIRE(URLUtils::StripQueryAndFragment("https://example.com/file.parquet#section1") ==
	        "https://example.com/file.parquet");

	// URL with both query and fragment
	REQUIRE(URLUtils::StripQueryAndFragment("https://example.com/file.parquet?version=1#section1") ==
	        "https://example.com/file.parquet");
}

TEST_CASE("URLUtils::StripQueryAndFragment - S3 URLs", "[url_utils]") {
	// S3 URL with query parameters
	REQUIRE(URLUtils::StripQueryAndFragment("s3://bucket/path/file.parquet?versionId=xyz") ==
	        "s3://bucket/path/file.parquet");

	// S3 URL without query
	REQUIRE(URLUtils::StripQueryAndFragment("s3://bucket/path/file.parquet") == "s3://bucket/path/file.parquet");
}

TEST_CASE("URLUtils::StripQueryAndFragment - HTTP URLs", "[url_utils]") {
	// HTTP URL with port and query
	REQUIRE(URLUtils::StripQueryAndFragment("http://example.com:8080/api/data?format=json") ==
	        "http://example.com:8080/api/data");

	// HTTPS URL with nested path
	REQUIRE(URLUtils::StripQueryAndFragment("https://cdn.example.com/data/2024/01/file.parquet?token=abc123") ==
	        "https://cdn.example.com/data/2024/01/file.parquet");
}

TEST_CASE("URLUtils::StripQueryAndFragment - Edge cases", "[url_utils]") {
	// Empty string
	REQUIRE(URLUtils::StripQueryAndFragment("") == "");

	// Just a question mark
	REQUIRE(URLUtils::StripQueryAndFragment("https://example.com/file?") == "https://example.com/file");

	// Just a hash
	REQUIRE(URLUtils::StripQueryAndFragment("https://example.com/file#") == "https://example.com/file");

	// Query with equals but no value
	REQUIRE(URLUtils::StripQueryAndFragment("https://example.com/file?key=") == "https://example.com/file");
}

TEST_CASE("URLUtils::ParseURL - Complete URLs", "[url_utils]") {
	// Parse a complete HTTP URL
	auto parsed = URLUtils::ParseURL("https://example.com:8080/path/to/file.parquet?version=1&format=snappy#section1");
	REQUIRE(parsed.scheme == "https");
	REQUIRE(parsed.host == "example.com:8080");
	REQUIRE(parsed.path == "/path/to/file.parquet");
	REQUIRE(parsed.query == "version=1&format=snappy");
	REQUIRE(parsed.fragment == "section1");
	REQUIRE(parsed.url_without_query == "https://example.com:8080/path/to/file.parquet");
}

TEST_CASE("URLUtils::ParseURL - S3 URLs", "[url_utils]") {
	// Parse S3 URL
	auto parsed = URLUtils::ParseURL("s3://my-bucket/data/file.parquet?versionId=xyz");
	REQUIRE(parsed.scheme == "s3");
	REQUIRE(parsed.host == "my-bucket");
	REQUIRE(parsed.path == "/data/file.parquet");
	REQUIRE(parsed.query == "versionId=xyz");
	REQUIRE(parsed.fragment == "");
	REQUIRE(parsed.url_without_query == "s3://my-bucket/data/file.parquet");
}

TEST_CASE("URLUtils::ParseURL - URLs without query or fragment", "[url_utils]") {
	// Simple HTTP URL
	auto parsed = URLUtils::ParseURL("https://example.com/file.parquet");
	REQUIRE(parsed.scheme == "https");
	REQUIRE(parsed.host == "example.com");
	REQUIRE(parsed.path == "/file.parquet");
	REQUIRE(parsed.query == "");
	REQUIRE(parsed.fragment == "");
	REQUIRE(parsed.url_without_query == "https://example.com/file.parquet");
}

TEST_CASE("URLUtils::ParseURL - URLs without scheme", "[url_utils]") {
	// URL without scheme (relative or local path)
	auto parsed = URLUtils::ParseURL("/path/to/file.parquet?version=1");
	REQUIRE(parsed.scheme == "");
	REQUIRE(parsed.path == "/path/to/file.parquet");
	REQUIRE(parsed.query == "version=1");
	REQUIRE(parsed.url_without_query == "/path/to/file.parquet");
}

TEST_CASE("URLUtils::ParseURL - Host only URLs", "[url_utils]") {
	// URL with scheme and host only
	auto parsed = URLUtils::ParseURL("https://example.com");
	REQUIRE(parsed.scheme == "https");
	REQUIRE(parsed.host == "example.com");
	REQUIRE(parsed.path == "");
	REQUIRE(parsed.query == "");
	REQUIRE(parsed.fragment == "");
	REQUIRE(parsed.url_without_query == "https://example.com");
}

TEST_CASE("URLUtils::ParseURL - Edge cases", "[url_utils]") {
	// Empty string
	auto parsed = URLUtils::ParseURL("");
	REQUIRE(parsed.scheme == "");
	REQUIRE(parsed.host == "");
	REQUIRE(parsed.path == "");
	REQUIRE(parsed.query == "");
	REQUIRE(parsed.fragment == "");
	REQUIRE(parsed.url_without_query == "");
}

TEST_CASE("URLUtils - Real world examples", "[url_utils]") {
	// AWS S3 with version ID
	string s3_url = "s3://my-bucket/data/2024/file.parquet?versionId=abc123&partNumber=1";
	REQUIRE(URLUtils::StripQueryAndFragment(s3_url) == "s3://my-bucket/data/2024/file.parquet");
	auto parsed_s3 = URLUtils::ParseURL(s3_url);
	REQUIRE(parsed_s3.scheme == "s3");
	REQUIRE(parsed_s3.host == "my-bucket");
	REQUIRE(parsed_s3.path == "/data/2024/file.parquet");
	REQUIRE(parsed_s3.query == "versionId=abc123&partNumber=1");

	// HTTPS URL with authentication token
	string https_url = "https://storage.googleapis.com/bucket/file.parquet?token=xyz789&expires=1234567890";
	REQUIRE(URLUtils::StripQueryAndFragment(https_url) == "https://storage.googleapis.com/bucket/file.parquet");
	auto parsed_https = URLUtils::ParseURL(https_url);
	REQUIRE(parsed_https.scheme == "https");
	REQUIRE(parsed_https.host == "storage.googleapis.com");
	REQUIRE(parsed_https.path == "/bucket/file.parquet");
	REQUIRE(parsed_https.query == "token=xyz789&expires=1234567890");

	// HTTP URL with port and complex query
	string http_url = "http://localhost:9000/api/data/file.csv?format=csv&delimiter=%2C&encoding=utf8#results";
	REQUIRE(URLUtils::StripQueryAndFragment(http_url) == "http://localhost:9000/api/data/file.csv");
	auto parsed_http = URLUtils::ParseURL(http_url);
	REQUIRE(parsed_http.scheme == "http");
	REQUIRE(parsed_http.host == "localhost:9000");
	REQUIRE(parsed_http.path == "/api/data/file.csv");
	REQUIRE(parsed_http.query == "format=csv&delimiter=%2C&encoding=utf8");
	REQUIRE(parsed_http.fragment == "results");
}
