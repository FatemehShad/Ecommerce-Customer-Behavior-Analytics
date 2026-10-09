"""Fetch pinned Delta jars over verified HTTPS using Python's standard proxy support.

Maven Central publishes SHA-1 sidecars for these artifacts. Verify each downloaded artifact
against that publisher checksum; never disable TLS or install an unverified download.
Spark's bundled Scala, Hadoop, and ANTLR runtimes supply the remaining dependencies.
"""

import argparse
import hashlib
from pathlib import Path
from urllib.request import urlopen

ARTIFACTS = ("delta-spark_2.13", "delta-storage")
VERSION = "4.0.1"


def fetch(directory: Path) -> list[Path]:
    directory.mkdir(parents=True, exist_ok=True)
    paths = []
    for artifact in ARTIFACTS:
        filename = f"{artifact}-{VERSION}.jar"
        url = f"https://repo.maven.apache.org/maven2/io/delta/{artifact}/{VERSION}/{filename}"
        with urlopen(url + ".sha1", timeout=60) as response:
            checksum = response.read().decode("ascii").strip().split()[0]
        if len(checksum) != 40 or any(c not in "0123456789abcdef" for c in checksum.lower()):
            raise ValueError(f"Invalid publisher checksum for {filename}")
        target = directory / filename
        if target.exists() and hashlib.sha1(target.read_bytes()).hexdigest() == checksum:
            paths.append(target.resolve())
            continue
        with urlopen(url, timeout=60) as response:
            payload = response.read()
        if hashlib.sha1(payload).hexdigest() != checksum:
            raise ValueError(f"Artifact checksum mismatch for {filename}")
        target.write_bytes(payload)
        paths.append(target.resolve())
    return paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    print(",".join(str(path) for path in fetch(args.directory)))
