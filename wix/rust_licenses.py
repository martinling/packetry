import subprocess

cargo_result = subprocess.run(
    ['cargo', 'license', '--authors', '--do-not-bundle', '--color=never'],
    capture_output=True)

cargo_result.check_returncode()

print("The following Rust packages are statically linked into Packetry:")

for line in cargo_result.stdout.decode().rstrip().split("\n"):
    package, remainder = line.split(": ")
    if package == 'packetry':
        continue
    version, licenses, by, authors = remainder.split(", ")
    licenses = licenses[1:-1] \
        .replace(' OR ', ' or ') \
        .replace(' WITH ', ' with ') \
        .replace(' AND ', ' and ')
    authors = authors[1:-1].split("|")
    print()
    print(f"{package} version {version}")
    if len(authors) == 1:
        print(f"Author: {str.join(', ', authors)}")
    else:
        print("Authors:")
        for author in authors:
            print(f"    {author}")
    print(f"License: {licenses}")
    print(f"Link: https://crates.io/crates/{package}")
