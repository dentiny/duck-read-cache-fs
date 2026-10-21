#include "hash_utils.hpp"

#include "crypto.hpp"

namespace duckdb {

string Sha256ToHexString(const hash_bytes &sha256) {
	static constexpr char kHexChars[] = "0123456789abcdef";
	string result;
	// SHA256 has 32 byte, we encode 2 chars for each byte of SHA256.
	result.reserve(64);

	for (unsigned char byte : sha256) {
		result += kHexChars[byte >> 4];  // Get high 4 bits
		result += kHexChars[byte & 0xF]; // Get low 4 bits
	}
	return result;
}

string GetSha256(const string &input) {
	duckdb::hash_bytes sha256_val;
	static_assert(sizeof(sha256_val) == 32);
	static AESStateSSLFactory encryption_util;
	duckdb::sha256(encryption_util, const_data_ptr_cast(input.data()), input.length(), sha256_val);
	return Sha256ToHexString(sha256_val);
}

} // namespace duckdb
