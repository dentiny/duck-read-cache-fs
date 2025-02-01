// These user-defined literals makes sizes.

#pragma once

#include <cstdint>

inline unsigned long long operator""_PiB(unsigned long long sz) {
  return sz * 1024ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL;
}
inline unsigned long long operator""_PB(unsigned long long sz) {
  return sz * 1000ULL * 1000ULL * 1000ULL * 1000ULL * 1000ULL;
}

inline unsigned long long operator""_TiB(unsigned long long sz) {
  return sz * 1024ULL * 1024ULL * 1024ULL * 1024ULL;
}
inline unsigned long long operator""_TB(unsigned long long sz) {
  return sz * 1000ULL * 1000ULL * 1000ULL * 1000ULL;
}

inline unsigned long long operator""_GiB(unsigned long long sz) {
  return sz * 1024ULL * 1024ULL * 1024ULL;
}
inline unsigned long long operator""_GB(unsigned long long sz) {
  return sz * 1000ULL * 1000ULL * 1000ULL;
}

inline unsigned long long operator""_MiB(unsigned long long sz) {
  return sz * 1024ULL * 1024ULL;
}
inline unsigned long long operator""_MB(unsigned long long sz) {
  return sz * 1000ULL * 1000ULL;
}

inline unsigned long long operator""_KiB(unsigned long long sz) {
  return sz * 1024ULL;
}
inline unsigned long long operator""_KB(unsigned long long sz) {
  return sz * 1000ULL;
}

inline unsigned long long operator""_PiB(long double sz) {
  const long double res = sz * 1024ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL;
  return static_cast<unsigned long long>(res);
}
inline unsigned long long operator""_PB(long double sz) {
  const long double res = sz * 1000ULL * 1000ULL * 1000ULL * 1000ULL * 1000ULL;
  return static_cast<unsigned long long>(res);
}

inline unsigned long long operator""_TiB(long double sz) {
  const long double res = sz * 1024ULL * 1024ULL * 1024ULL * 1024ULL;
  return static_cast<unsigned long long>(res);
}
inline unsigned long long operator""_TB(long double sz) {
  const long double res = sz * 1000ULL * 1000ULL * 1000ULL * 1000ULL;
  return static_cast<unsigned long long>(res);
}

inline unsigned long long operator""_GiB(long double sz) {
  const long double res = sz * 1024ULL * 1024ULL * 1024ULL;
  return static_cast<unsigned long long>(res);
}
inline unsigned long long operator""_GB(long double sz) {
  const long double res = sz * 1000ULL * 1000ULL * 1000ULL;
  return static_cast<unsigned long long>(res);
}

inline unsigned long long operator""_MiB(long double sz) {
  const long double res = sz * 1024ULL * 1024ULL;
  return static_cast<unsigned long long>(res);
}
inline unsigned long long operator""_MB(long double sz) {
  const long double res = sz * 1000ULL * 1000ULL;
  return static_cast<unsigned long long>(res);
}

inline unsigned long long operator""_KiB(long double sz) {
  const long double res = sz * 1024ULL;
  return static_cast<unsigned long long>(res);
}
inline unsigned long long operator""_KB(long double sz) {
  const long double res = sz * 1000ULL;
  return static_cast<unsigned long long>(res);
}

inline unsigned long long operator""_B(unsigned long long sz) { return sz; }
