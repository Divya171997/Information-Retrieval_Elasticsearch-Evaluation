"""Tests for searchengine_es8.py. No Elasticsearch server is needed: the client is mocked.

Run with:  python -m pytest tests/
"""
import ast
import contextlib
import io
import json
import pathlib
import sys
from unittest import mock

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import searchengine_es8 as se  # noqa: E402


def _original_mapr():
    """Load the unmodified ``mapr`` function from the original searchengine.py."""
    tree = ast.parse((ROOT / "searchengine.py").read_text(encoding="utf-8"))
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "mapr")
    namespace = {}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), "searchengine.py", "exec"), namespace)
    return namespace


def _fake_result(ids):
    return {"hits": {"hits": [{"_id": str(i), "_score": 1.0,
                               "_source": {"media-type": "News", "title": f"t{i}", "source": "s",
                                           "published": "2015-09-01T00:00:00Z", "content": "c"}}
                              for i in ids]}}


def test_mapr_matches_original_implementation():
    result = _fake_result([500, 3, 7999, 42])
    ordered, vec = se.relevance_vector(result)

    out_new = io.StringIO()
    with contextlib.redirect_stdout(out_new):
        se.mapr(vec, ordered)

    ns = _original_mapr()
    ns["totallist1"], ns["orderedsearch"] = vec, ordered
    out_old = io.StringIO()
    with contextlib.redirect_stdout(out_old):
        ns["mapr"](0.0, 0.0)

    assert out_new.getvalue() == out_old.getvalue()


def test_all_ten_menu_choices_build_queries_and_invalid_returns_none():
    answers = iter(["word"] * 100)
    for ch in map(str, range(1, 11)):
        assert se.build_query(ch, ask=lambda _: next(answers)) is not None
    assert se.build_query("11", ask=lambda _: "x") is None


def test_exact_title_query_matches_original():
    q = se.build_query("5", ask=lambda _: "10 Great Game Soundtracks for Studying")
    assert q == {"match_phrase": {"title": "10 Great Game Soundtracks for Studying"}}


def test_read_articles_stops_at_limit(tmp_path):
    data = tmp_path / "sample.jsonl"
    data.write_text("\n".join(json.dumps({"n": i}) for i in range(1, 20)), encoding="utf-8")
    got = list(se.read_articles(data, limit=8))
    assert [i for i, _ in got] == list(range(1, 8))      # ids 1..limit-1, as in the original


def test_interactive_session_with_mocked_client(capsys):
    client = mock.Mock()
    client.search.return_value = _fake_result([500])
    answers = iter(["yes", "y", "99", "5", "Some title", "n"])   # includes one invalid choice
    se.main([], elastic=client, ask=lambda _: next(answers))
    out = capsys.readouterr().out
    assert "Invalid choice" in out
    assert "Document Index is: 500" in out
    assert "Mean Average Precision:" in out
    kwargs = client.search.call_args.kwargs
    assert kwargs["index"] == se.DEFAULT_INDEX and "doc_type" not in kwargs and kwargs["size"] == 8000
