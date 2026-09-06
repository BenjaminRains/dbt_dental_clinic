"""Make `dental_consultation_pipeline` resolve to `consult_audio_pipe` in tests."""

import sys

import consult_audio_pipe

sys.modules.setdefault("dental_consultation_pipeline", consult_audio_pipe)
