#include "url_utils.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

string URLUtils::StripQueryAndFragment(const string &url) {
	// Find the first occurrence of '?' or '#'
	// Query parameters start with '?', fragments start with '#'
	auto question_pos = url.find('?');
	auto hash_pos = url.find('#');
	
	// Find the earliest position of either '?' or '#'
	auto strip_pos = string::npos;
	if (question_pos != string::npos && hash_pos != string::npos) {
		strip_pos = (question_pos < hash_pos) ? question_pos : hash_pos;
	} else if (question_pos != string::npos) {
		strip_pos = question_pos;
	} else if (hash_pos != string::npos) {
		strip_pos = hash_pos;
	}
	
	if (strip_pos != string::npos) {
		return url.substr(0, strip_pos);
	}
	return url;
}

ParsedURL URLUtils::ParseURL(const string &url) {
	ParsedURL result;
	
	if (url.empty()) {
		return result;
	}
	
	// Start parsing from the beginning
	size_t pos = 0;
	size_t url_len = url.length();
	
	// 1. Parse scheme (e.g., "http://", "https://", "s3://")
	auto scheme_end = url.find("://", pos);
	if (scheme_end != string::npos) {
		result.scheme = url.substr(pos, scheme_end - pos);
		pos = scheme_end + 3; // Skip "://"
	}
	
	// 2. Find the end of host (marked by '/', '?', or '#')
	size_t host_end = url.find_first_of("/?#", pos);
	if (host_end == string::npos) {
		// No path, query, or fragment - entire rest is host
		result.host = url.substr(pos);
		result.url_without_query = url;
		return result;
	}
	
	result.host = url.substr(pos, host_end - pos);
	pos = host_end;
	
	// 3. Parse path (until '?' or '#')
	size_t path_end = url.find_first_of("?#", pos);
	if (path_end == string::npos) {
		// No query or fragment - rest is path
		result.path = url.substr(pos);
		result.url_without_query = url;
		return result;
	}
	
	result.path = url.substr(pos, path_end - pos);
	
	// Store URL without query/fragment
	result.url_without_query = url.substr(0, path_end);
	
	pos = path_end;
	
	// 4. Parse query (if starts with '?')
	if (pos < url_len && url[pos] == '?') {
		pos++; // Skip '?'
		size_t query_end = url.find('#', pos);
		if (query_end == string::npos) {
			// No fragment - rest is query
			result.query = url.substr(pos);
			return result;
		}
		result.query = url.substr(pos, query_end - pos);
		pos = query_end;
	}
	
	// 5. Parse fragment (if starts with '#')
	if (pos < url_len && url[pos] == '#') {
		pos++; // Skip '#'
		result.fragment = url.substr(pos);
	}
	
	return result;
}

} // namespace duckdb
