# Consult audio

Dev notes for `consult_audio_pipe` and how its outputs reach the clinic product.

The pipeline itself (Whisper → clean → Claude/ChatGPT → HTML/PDF) lives in
[`consult_audio_pipe/`](../../consult_audio_pipe/). Run it with `mdc consult-audio`.
Outputs are local, gitignored PHI.

| Document | Purpose |
| --- | --- |
| [CLINIC_FRONTEND_DELIVERY_PLAN.md](./CLINIC_FRONTEND_DELIVERY_PLAN.md) | Early rough plan: publish summaries into the clinic frontend |
| [TRANSCRIPTION_AND_CLEANING_PLAN.md](./TRANSCRIPTION_AND_CLEANING_PLAN.md) | Phased plan: Whisper options, safer dictionary, CI, PR/test plan |
| [../../consult_audio_pipe/README.md](../../consult_audio_pipe/README.md) | Pipeline stages, local run, output dirs |

Tracked in [TODO.md](../../TODO.md) under Tier 8.
