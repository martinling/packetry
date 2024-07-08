import subprocess
import json
import re
import os

vcpkg_result = subprocess.run(
    ['./vcpkg/vcpkg', 'depend-info', 'gtk'],
    capture_output=True)

vcpkg_result.check_returncode()

packages = set()

# Find all packages we depend on.
for line in vcpkg_result.stderr.decode().rstrip().split('\n'):
    header, remainder = line.split(': ')
    package = header.split('[')[0]
    packages.add(package)
    dependencies = remainder.split(', ')
    packages |= set(dependencies)

# Discard empty package names.
packages.discard('')

# Discard vcpkg build tools.
packages = set(p for p in packages if not p.startswith('vcpkg-'))

# gperf is needed to build fontconfig, but is not linked to.
packages.discard('gperf')

# gettext is needed to build many dependencies, but is not linked to.
packages.discard('gettext')

# getopt and pthread are virtual packages.
packages.discard('getopt')
packages.discard('pthread')

# sassc is used to build GTK, but is not linked to.
packages.discard('sassc')

versions = {}

# These packages are missing license information in vcpkg.
licenses = {
    'libiconv': ['LGPL-2.1-or-later'],
    'egl-registry': ['Apache-2.0', 'MIT'],
    'liblzma': ['BSD-0-Clause'],
    'libsass': ['MIT'],
}

# These packages are missing homepage information in vcpkg.
homepages = {
    'fribidi': 'https://github.com/fribidi/fribidi',
}

for package in packages:
    metadata = json.load(open(f'vcpkg/ports/{package}/vcpkg.json'))
    version = metadata.get('version-semver',
        metadata.get('version',
            metadata.get('version-date')))
    if version is None:
        raise KeyError(f"Couldn't find a version for package {package}")
    versions[package] = version
    if metadata.get('homepage') is not None:
        homepages[package] = metadata['homepage']
    if metadata.get('license') is not None:
        licenses[package] = metadata['license'].split(' OR ')

print("The following libraries are dynamically linked into Packetry:")

print(f"<Directory Id='full_licenses' Name='full-licenses'>")
for package in sorted(packages):
    pkgid = package.replace('-', '_')
    guid = subprocess.run('uuidgen', capture_output=True).stdout.decode().rstrip()
    print(f"    <Component Id='LICENSE_{pkgid}' Guid='{guid}'>")
    print(f"        <File Id='LICENSE_{pkgid}' Name='LICENSE-{package}.txt' DiskId='1' Source='full-licenses/LICENSE-{package}.txt'>")
    print(f"    </Component>")
print(f"</Directory>")
print()
for package in sorted(packages):
    pkgid = package.replace('-', '_')
    print(f"<ComponentRef Id='LICENSE_{pkgid}'/>")
