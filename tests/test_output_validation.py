import bn.cli


def test_empty_match_regex_is_valid_with_context(monkeypatch, capsys):
    calls = []
    def send(op, **kwargs):
        calls.append(op)
        return {"result": {"text": "first\nsecond"}}
    monkeypatch.setattr(bn.cli, "send_request", send)
    assert bn.cli.main(["decompile", "example", "--match", "", "--before", "1"]) == 0
    assert calls == ["decompile"]
    assert capsys.readouterr().out == "first\nsecond\n"
