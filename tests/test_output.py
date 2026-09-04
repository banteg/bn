from pathlib import Path
import json
import pytest
import bn.output as output


@pytest.mark.parametrize("fmt", ["json", "ndjson"])
def test_structured_output_never_auto_spills(fmt, monkeypatch):
    monkeypatch.setattr(output, "_token_encoding", lambda: pytest.fail("must stay offline"))
    value = [{"text": "x" * 50000}]
    result = output.write_output_result(value, fmt=fmt, out_path=None, stem="test")
    assert not result.spilled
    assert json.loads(result.rendered) == (value if fmt == "json" else value[0])


def test_explicit_structured_spill(tmp_path, monkeypatch):
    monkeypatch.setattr(output, "spill_root", lambda: tmp_path)
    result = output.write_output_result({"x": "y" * 100}, fmt="json", out_path=None,
                                      stem="test", allow_spill=True, spill_byte_limit=20)
    assert result.spilled
    assert json.loads(Path(result.artifact["artifact_path"]).read_text()) == {"x": "y" * 100}


def test_byte_limit_and_no_spill(tmp_path, monkeypatch):
    monkeypatch.setattr(output, "spill_root", lambda: tmp_path)
    monkeypatch.setattr(output, "_token_encoding", lambda: pytest.fail("must stay offline"))
    for allow in (True, False):
        result = output.write_output_result("é" * 10, fmt="text", out_path=None,
                                          stem="test", allow_spill=allow, spill_byte_limit=15)
        assert result.spilled == allow


def test_out_is_offline_by_default_and_token_failure_is_nonfatal(tmp_path, monkeypatch):
    def unavailable():
        raise ImportError("no tiktoken")
    monkeypatch.setattr(output, "_token_encoding", unavailable)
    for count in (False, True):
        result = output.write_output_result("hello", fmt="text", out_path=tmp_path / "out.txt",
                                          stem="test", count_tokens=count)
        assert "tokens" not in result.artifact
        assert ("token_count_warning" in result.artifact) == count
        assert (tmp_path / "out.txt").read_text() == "hello\n"


def test_optional_token_count_treats_special_tokens_as_text(tmp_path, monkeypatch):
    class Encoder:
        def encode(self, text, *, disallowed_special):
            assert disallowed_special == ()
            return [1, 2]
    monkeypatch.setattr(output, "_token_encoding", lambda: Encoder())
    result = output.write_output_result("<|endoftext|>", fmt="text", out_path=tmp_path / "out",
                                      stem="test", count_tokens=True)
    assert result.artifact["tokens"] == 2


def test_concurrent_spills_preserve_distinct_payloads(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import hashlib
    import bn.output as output

    monkeypatch.setattr(output, "spill_root", lambda: tmp_path)
    def write(index):
        payload = f"function {index}\n"
        result = output.write_output_result(payload, fmt="text", out_path=None,
                                            stem="decompile", spill_byte_limit=1)
        return payload, result.artifact
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(write, range(32)))
    assert len({artifact["artifact_path"] for _, artifact in results}) == 32
    for payload, artifact in results:
        content = Path(artifact["artifact_path"]).read_bytes()
        assert content.decode() == payload
        assert hashlib.sha256(content).hexdigest() == artifact["sha256"]
