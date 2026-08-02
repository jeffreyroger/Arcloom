import os
import sys

import pipeline.run as run_module


def test_guard_replaces_none_streams_with_working_writers(monkeypatch):
    # pythonw.exe has no console: sys.stdout/sys.stderr are None, not merely
    # redirected. Third-party libraries that print unconditionally (e.g.
    # tqdm progress bars, pulled in by sentence-transformers starting week 2)
    # raise AttributeError on None -- a failure indistinguishable from the
    # week 1 kill-signature defect this guards against.
    monkeypatch.setattr(sys, "stdout", None)
    monkeypatch.setattr(sys, "stderr", None)
    monkeypatch.delenv("HF_HUB_DISABLE_PROGRESS_BARS", raising=False)
    monkeypatch.delenv("TQDM_DISABLE", raising=False)

    run_module._guard_pythonw_streams()

    assert sys.stdout is not None
    assert sys.stderr is not None
    print("must not raise")  # would raise AttributeError if stdout were still None
    assert os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] == "1"
    assert os.environ["TQDM_DISABLE"] == "1"
