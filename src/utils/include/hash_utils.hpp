#pragma once

#include "crypto.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

// Convert SHA-256 to hex string format.
string Sha256ToHexString(const hash_bytes &sha256);

// Get SHA-256 string for the given string.
string GetSha256(const string &input);

} // namespace duckdb
