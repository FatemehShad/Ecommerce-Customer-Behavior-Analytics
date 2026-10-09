"""Download only Retailrocket's events.csv from its official public Kaggle endpoint."""

import csv
import hashlib
import os
import shutil
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from urllib.request import Request, urlopen

DATASET_URL = "https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset"
DOWNLOAD_URL = (
    "https://www.kaggle.com/api/v1/datasets/download/retailrocket/ecommerce-dataset/events.csv"
)
MAX_BYTES = 512 * 1024 * 1024
HEADER = {"timestamp", "visitorid", "event", "itemid", "transactionid"}


def download_events(output: Path) -> dict[str, str | int]:
    """Handle a raw CSV or ZIP response, validate the header, and preserve existing files.

    TLS uses Python's default verification and proxy configuration. ZIP member CRCs are
    checked during extraction. The returned SHA-256 records the downloaded CSV identity;
    Kaggle does not provide an expected checksum here, so it is not independent attestation.
    """
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite existing input: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".retailrocket-", dir=output.parent) as scratch:
        archive = Path(scratch) / "response"
        request = Request(DOWNLOAD_URL, headers={"User-Agent": "ecommerce-analytics/0.1"})
        with urlopen(request, timeout=60) as response, archive.open("wb") as handle:
            size = 0
            while chunk := response.read(1024 * 1024):
                size += len(chunk)
                if size > MAX_BYTES:
                    raise ValueError("Dataset download exceeds the 512 MiB events-file limit")
                handle.write(chunk)
        csv_path = archive
        if zipfile.is_zipfile(archive):
            with zipfile.ZipFile(archive) as bundle:
                members = [
                    info
                    for info in bundle.infolist()
                    if not info.is_dir() and PurePosixPath(info.filename).name == "events.csv"
                ]
                if len(members) != 1:
                    raise ValueError("Expected exactly one events.csv member in the download")
                if members[0].file_size > MAX_BYTES:
                    raise ValueError("Expanded events.csv exceeds the 512 MiB limit")
                csv_path = Path(scratch) / "events.csv"
                # Stream only this member to a chosen path; never extract archive paths.
                with bundle.open(members[0]) as source, csv_path.open("wb") as target:
                    shutil.copyfileobj(source, target)
        with csv_path.open(encoding="utf-8-sig", newline="") as handle:
            header = next(csv.reader(handle), [])
        if set(header) != HEADER or len(header) != len(HEADER):
            raise ValueError("Download is not Retailrocket's five-column events.csv")
        digest = hashlib.sha256()
        with csv_path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
        result = {
            "dataset_url": DATASET_URL,
            "output": str(output),
            "bytes": csv_path.stat().st_size,
            "sha256": digest.hexdigest(),
        }
        # Same filesystem, atomic publication, and no replacement even if another process writes.
        os.link(csv_path, output)
    return result
