$ErrorActionPreference = "Stop"

if (-not $env:VSCMD_VER -or ([version]$env:VSCMD_VER).Major -lt 17) {
    throw "Run from a Visual Studio 2022+ developer shell."
}

$PROJ_ROOT = $PSScriptRoot
Set-Location $PROJ_ROOT

$VCPKG_BASELINE = "84bab45d415d22042bd0b9081aea57f362da3f35"
$VCPKG_TOOLCHAIN_PATH = Join-Path $env:VCPKG_ROOT scripts/buildsystems/vcpkg.cmake
$VCPKG_MANIFEST_DIR = Join-Path $PROJ_ROOT build/vcpkg-manifest

New-Item -ItemType Directory -Force -Path build/release | Out-Null
New-Item -ItemType Directory -Force -Path $VCPKG_MANIFEST_DIR | Out-Null
Copy-Item vcpkg.json (Join-Path $VCPKG_MANIFEST_DIR vcpkg.json) -Force
@"
{
  "default-registry": {
    "kind": "builtin",
    "baseline": "$VCPKG_BASELINE"
  }
}
"@ | Set-Content (Join-Path $VCPKG_MANIFEST_DIR vcpkg-configuration.json) -Encoding utf8

cmake `
    -DEXTENSION_STATIC_BUILD=1 `
    -DDUCKDB_EXTENSION_CONFIGS="$PROJ_ROOT/extension_config.cmake" `
    -DVCPKG_BUILD=1 `
    -DCMAKE_TOOLCHAIN_FILE="$VCPKG_TOOLCHAIN_PATH" `
    -DVCPKG_MANIFEST_DIR="$VCPKG_MANIFEST_DIR" `
    -DVCPKG_TARGET_TRIPLET=x64-windows-static `
    -DENABLE_UNITTEST_CPP_TESTS=FALSE `
    -DCMAKE_BUILD_TYPE=Release `
    -S ./duckdb/ `
    -B build/release
if ($LASTEXITCODE) { exit $LASTEXITCODE }

cmake --build build/release --config Release
if ($LASTEXITCODE) { exit $LASTEXITCODE }
