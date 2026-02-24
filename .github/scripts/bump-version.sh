#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <release-version> [properties-file]" >&2
  exit 1
fi

release_version="$1"
properties_file="${2:-gradle.properties}"

if [[ ! -f "${properties_file}" ]]; then
  echo "Properties file not found: ${properties_file}" >&2
  exit 1
fi

if [[ "${release_version}" == *"-SNAPSHOT" ]]; then
  echo "Release version must not include -SNAPSHOT: ${release_version}" >&2
  exit 1
fi

if ! [[ "${release_version}" =~ ^[0-9]+(\.[0-9]+){2}([.-][A-Za-z0-9]+)?$ ]]; then
  echo "Invalid release version format: ${release_version}" >&2
  exit 1
fi

current_version="$(sed -n 's/^version=//p' "${properties_file}" | head -n1)"
if [[ -z "${current_version}" ]]; then
  echo "Could not find 'version=' in ${properties_file}" >&2
  exit 1
fi

tmp_file="$(mktemp)"
trap 'rm -f "${tmp_file}"' EXIT

awk -v version="${release_version}" '
BEGIN { updated = 0 }
/^version=/ {
  print "version=" version
  updated = 1
  next
}
{ print }
END {
  if (updated == 0) {
    exit 2
  }
}
' "${properties_file}" > "${tmp_file}"

mv "${tmp_file}" "${properties_file}"

echo "Updated version: ${current_version} -> ${release_version}"
