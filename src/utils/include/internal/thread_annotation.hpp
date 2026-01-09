#pragma once

// Enable thread safety attributes only with clang.
// The attributes can be safely erased when compiling with other compilers.
#if defined(__clang__) && (!defined(SWIG))
#define THREAD_ANNOTATION_ATTRIBUTE__(x) __attribute__((x))
#ifndef DUCKDB_THREAD_ANNOTATION_ENABLED
#define DUCKDB_THREAD_ANNOTATION_ENABLED 1
#endif
#else
#define THREAD_ANNOTATION_ATTRIBUTE__(x) // no-op
#ifndef DUCKDB_THREAD_ANNOTATION_ENABLED
#define DUCKDB_THREAD_ANNOTATION_ENABLED 0
#endif
#endif

// CAPABILITY is an attribute on classes, which specifies that objects of the
// class can be used as a capability. The string argument specifies the kind of
// capability in error messages, e.g. "mutex".
#define DUCKDB_CAPABILITY(x) THREAD_ANNOTATION_ATTRIBUTE__(capability(x))

// SCOPED_CAPABILITY is an attribute on classes that implement RAII-style
// locking, in which a capability is acquired in the constructor, and released
// in the destructor. Such classes require special handling because the
// constructor and destructor refer to the capability via different names.
#define DUCKDB_SCOPED_CAPABILITY THREAD_ANNOTATION_ATTRIBUTE__(scoped_lockable)
