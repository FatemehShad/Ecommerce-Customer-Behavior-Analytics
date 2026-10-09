import hashlib
import io
import zipfile

import pytest

from ecommerce_analytics import retailrocket

# Representative schema fixtures, not rows asserted to come from the original dataset.
CSV = b"timestamp,visitorid,event,itemid,transactionid\n1433221332117,257597,view,355908,\n"


@pytest.mark.parametrize("zipped", [False, True])
def test_official_download_response_formats(monkeypatch, tmp_path, zipped):
    payload = CSV
    if zipped:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("dataset/events.csv", CSV)
            archive.writestr("category_tree.csv", "categoryid,parentid\n1,\n")
        payload = buffer.getvalue()
    monkeypatch.setattr(retailrocket, "urlopen", lambda *args, **kwargs: io.BytesIO(payload))
    output = tmp_path / "events.csv"
    result = retailrocket.download_events(output)
    assert output.read_bytes() == CSV
    assert result["sha256"] == hashlib.sha256(CSV).hexdigest()
    assert result["bytes"] == len(CSV)
    assert result["dataset_url"] == retailrocket.DATASET_URL
    assert not list(tmp_path.glob(".retailrocket-*"))


def test_rejects_login_page_and_preserves_existing_data(monkeypatch, tmp_path):
    monkeypatch.setattr(
        retailrocket, "urlopen", lambda *args, **kwargs: io.BytesIO(b"<html>Sign in</html>")
    )
    output = tmp_path / "events.csv"
    with pytest.raises(ValueError, match="five-column"):
        retailrocket.download_events(output)
    assert not output.exists()
    output.write_bytes(CSV)
    with pytest.raises(FileExistsError, match="overwrite"):
        retailrocket.download_events(output)
    assert output.read_bytes() == CSV
