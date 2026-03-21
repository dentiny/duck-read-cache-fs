PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=cache_httpfs
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

format-all: format
	find benchmark/ -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	find test/unittest/ -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt
	cmake-format -i test/unittest/CMakeLists.txt

HTTPFS_TEST_TMPDIR := /tmp/cache-httpfs-duckdb-httpfs-test

# httpfs tests to skip, since caching layer changes behavior.
HTTPFS_TEST_BLACKLIST := \
	test/sql/httpfs/hffs.test_slow \
	test/sql/httpfs/globbing.test \
	test/sql/logging/file_system_logging.test \
	test/sql/logging/http_logging.test \
	test/sql/settings/test_disabled_file_system_httpfs.test \
	test/sql/storage/external_file_cache/external_file_cache_read_blob.test_slow \
	test/sql/copy/s3/glob_s3_recursive.test_slow

# Prepare httpfs tests: rewrite require directives, inject cache clear, and remove blacklisted tests.
define PREPARE_HTTPFS_TESTS
	@rm -rf $(HTTPFS_TEST_TMPDIR)
	@mkdir -p $(HTTPFS_TEST_TMPDIR)
	@cp -r duckdb-httpfs/test $(HTTPFS_TEST_TMPDIR)/test
	@find $(HTTPFS_TEST_TMPDIR)/test -type f \( -name "*.test" -o -name "*.test_slow" \) -exec \
		sed -i 's/^require httpfs$$/require cache_httpfs\n\nstatement ok\nSET cache_httpfs_type='"'"'in_mem'"'"';\n\nstatement ok\nSELECT cache_httpfs_clear_cache();/' {} +
	@for f in $(HTTPFS_TEST_BLACKLIST); do rm -f $(HTTPFS_TEST_TMPDIR)/$$f; done
endef

test_reldebug_httpfs:
	$(PREPARE_HTTPFS_TESTS)
	./build/reldebug/test/unittest --test-dir $(HTTPFS_TEST_TMPDIR) "test/sql/*"

test_release_httpfs:
	$(PREPARE_HTTPFS_TESTS)
	./build/release/test/unittest --test-dir $(HTTPFS_TEST_TMPDIR) "test/sql/*"

test_debug_httpfs:
	$(PREPARE_HTTPFS_TESTS)
	./build/debug/test/unittest --test-dir $(HTTPFS_TEST_TMPDIR) "test/sql/*"

.PHONY: format-all test_release_httpfs test_reldebug_httpfs test_debug_httpfs
