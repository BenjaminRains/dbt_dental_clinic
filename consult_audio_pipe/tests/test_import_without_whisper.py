"""Phase 0: package imports must not load Whisper / torch."""

import importlib
import sys


def test_cleaning_import_does_not_load_whisper():
    sys.modules.pop("whisper", None)
    import consult_audio_pipe.cleaning as cleaning

    importlib.reload(cleaning)
    assert "whisper" not in sys.modules


def test_transcription_import_does_not_load_whisper():
    sys.modules.pop("whisper", None)
    import consult_audio_pipe.transcription as transcription

    importlib.reload(transcription)
    assert "whisper" not in sys.modules
    assert transcription.whisper is None
