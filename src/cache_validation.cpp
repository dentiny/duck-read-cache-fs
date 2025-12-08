#include "cache_validation.hpp"

#include "cache_filesystem_config.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

bool IsCacheEntryValid(bool validate, const string &cached_version_tag, timestamp_t cached_last_modified,
                       const string &current_version_tag, timestamp_t current_last_modified) {
	if (!validate) {
		return true;
	}
	if (!current_version_tag.empty() || !cached_version_tag.empty()) {
		return cached_version_tag == current_version_tag;
	}
	if (cached_last_modified != current_last_modified) {
		return false;
	}
	// The last modified time matches. However, we cannot blindly trust this,
	// because some file systems use a low resolution clock to set the last modified time.
	// So, we will require that the last modified time is more than 10 seconds ago.
	static constexpr int64_t LAST_MODIFIED_THRESHOLD = 10LL * 1000LL * 1000LL;
	const auto access_time = Timestamp::GetCurrentTimestamp();
	if (access_time < current_last_modified) {
		return false;
	}
	return access_time - current_last_modified > LAST_MODIFIED_THRESHOLD;
}

} // namespace duckdb
