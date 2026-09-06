# Transcription and cleaning quality

> Status: **Phase 0 in progress** on `feature/consult-audio-phase-0-ci` — later phases not started
> Written: **2026-09-05**
> Related: [`consult_audio_pipe/`](../../consult_audio_pipe/), [README.md](./README.md), [TODO.md](../../TODO.md)

Improve Whisper output and stop the cleaner from rewriting already-correct
terms. Do **not** grow `dental_corrections.json` until the unsafe replaces and
money regexes are fixed.

This is separate from [CLINIC_FRONTEND_DELIVERY_PLAN.md](./CLINIC_FRONTEND_DELIVERY_PLAN.md)
(how summaries reach clinic) and from silent-audio QC as a product feature,
though Phase 3 adds a fail-fast volume gate so Wyatt-class files never hit
Whisper.

---

## Current state

### Transcription

`consult_audio_pipe/consult_audio_pipe/transcription.py` loads Whisper **`base`**
and calls `model.transcribe(path)` with no options.

| Gap | Effect |
| --- | --- |
| No `language="en"` | Extra language-guess / hallucination risk |
| No `initial_prompt` | No bias toward implant, all-on-four, Kamp, snap-in, zirconia |
| No `condition_on_previous_text=False` | Silence loops into pages of `...` (Wyatt) |
| Model reloaded per file | Slow, pointless |
| Pipeline transcribes **all** of `raw_audio/` | Re-Whispers already-done files |
| No volume / no-speech gate | Dead recordings still run Whisper |

`base` is the smallest useful model. Fine for a laptop smoke test; weak on
overlapping speech, money talk, and dental jargon.

### Dictionary and money

`dental_corrections.json` is a flat `{wrong: right}` map (~60 keys). Metadata
still says 29 rules harvested from **Christina King** only.
`context_issues.non_dental_words` is empty and unused.

Cleaning (`cleaning.py`):

1. Longest-key-first `\b` replace, case-insensitive.
2. Then aggressive money regexes.

On the Christina King clean file this already **regresses** good Whisper text:

| Raw | After clean |
| --- | --- |
| `snap-in dentures` / `snaps in` | `snap-in-in dentures` / `snap-in-ins in` |
| `12 grand` | `$12` (thousands dropped) |
| `35 bucks a piece` | `$$35 a piece` |

`\bsnap\b` matches the `snap` in `snap-in` because `-` is a word boundary.
Same class of landmine: `camp` → Kamp, `adventure` → a denture, `auction` →
option, `a pair of` → taking care of, `16 left` → 6 teeth left.

Money tests lock in some of the bugs (`$500,00` for “500 hundred”,
`$12,$13,000` for “12 13,000 give or take”). `15 to 24` always becomes
`$15,000 to $24,000`.

Later consults (e.g. Chris) still have errors the list never saw (`Peek don't
touch`, `call order`, `overbiting`). One-transcript harvesting does not
generalize.

**Rule:** Whisper prompt and a safer cleaner beat a bigger replace list.

---

## CI/CD rules for this work

`consult_audio_pipe` has a local pytest suite and **no GitHub Actions job
today**. `mdc_cli.yml` only covers `tools/mdc_cli`. Treat the missing CI as
part of the work, not an afterthought.

1. **One phase = one PR.** Each PR is reviewable, revertible, and leaves `main`
   green.
2. **Tests land with the behavior change** (same PR). Do not merge cleaner or
   Whisper-option changes on “manual listen” alone.
3. **No PHI in git or CI.** Do not commit raw audio, real transcripts, or
   `.env`. Fixtures are synthetic strings / tiny generated wavs.
4. **CI never downloads Whisper weights or torch CUDA.** Unit tests mock
   `whisper.load_model` / `transcribe`. Optional local quality runs stay off CI.
5. **Path-filtered workflow** on `consult_audio_pipe/**` and this doc’s
   workflow file only.
6. **Lazy-import Whisper** (or equivalent) so cleaning/conversion tests can run
   in CI without installing the full torch stack if we split extras. If the
   package still imports `whisper` at module load, CI must install CPU torch
   *or* we change the import first (Phase 0).
7. **Do not expand the dictionary** until Phase 1 regressions are gone.
8. **No `--no-verify`, no force-push to `main`.** Stack PRs on a feature
   branch (`feature/consult-audio-transcript-quality`).
9. **Default runtime stays `base`** until Phase 4. Model size is an env
   override, not a surprise download on every laptop.

---

## Phased approach

### Phase 0 — Make the suite CI-runnable

**Goal:** pytest can run on GitHub without pulling a Whisper checkpoint.

**Code**

- Lazy-import `whisper` inside `transcribe_audio_file` (and any other
  call sites). Module import of `cleaning` / `conversion` / `pipeline` status
  must not require torch.
- Add `.github/workflows/consult_audio_pipe.yml`:
  - `pull_request` + `push` to `main`, paths: `consult_audio_pipe/**`, the
    workflow file.
  - Python 3.11, `pip install -e consult_audio_pipe[dev]` **or** a slimmer
    `requirements-test.txt` if full `requirements.txt` pulls torch.
  - Prefer a test extra that omits `openai-whisper` / torch if Phase 0
    lazy-import is in place and transcription tests mock the import.
  - CI pytest **gate** (stale analysis/conversion/ChatGPT modules are not
    collected until they are updated):
    `test_import_without_whisper.py`, `test_transcription_comprehensive.py`,
    `test_package.py`, `test_cleaning_comprehensive.py`.
  - Skip or mark `network` tests that need live Anthropic/OpenAI
    (`test_claude_api.py`, `test_openai_api.py`) with `pytest.mark.integration`
    and default-deselect them (`addopts = -m "not integration"`).
  - Four money/`process_file` assertions that disagree with current cleaner
    behavior are `@pytest.mark.skip` until Phase 1.

**Tests in this PR**

- Existing suite still passes locally in `consult_audio_pipe/venv`.
- New or adjusted test: importing `consult_audio_pipe.cleaning` does not
  import `whisper`.
- CI job is green on the PR.

**Out of scope:** changing Whisper arguments or the dictionary.

---

### Phase 1 — Safer cleaner (no Whisper)

**Goal:** Christina-style text does not get worse after clean. Money rules only
fire when the utterance is actually money.

**Dictionary**

- Keep multi-word / high-precision phrases (`water trick`, `upper jump`,
  `great ball`, `Meribald`, `room healing`, `over-dead-tropations`).
- Quarantine or delete short / already-correct keys: `snap`, `snaps`, `camp`,
  `adventure`, `auction`, `console`/`council` if they collide, `a pair of`,
  `16 left`, `17 left`.
- Do not apply a replace that can match inside an already-correct term
  (`snap-in`, `snap-ins`). Hyphen-aware matching or “skip if already the
  target.”
- Fix target typos if we keep the row (`prosthedontist` → prosthodontist,
  `water-pick` → Waterpik if that is the intended brand).

**Money**

- `N grand` / `N k` → `$N,000` (or `$N000`), not `$N`.
- Do not double-prefix `$`.
- Drop or tightly constrain “any 4-digit number” and bare `15 to 24` →
  thousands. Keep “N dollars / bucks / thousand / grand.”
- Update tests that currently assert `$500,00` and `$12,$13,000` — those
  assertions are wrong, not a contract to preserve.

**Tests in this PR (required)**

Synthetic fixtures (not real consult files):

- `snap-in dentures` / `the denture snaps in` / `snap-ins` stay correct.
- `12 grand` → `$12,000` (or agreed equivalent), not `$12`.
- `35 bucks a piece` → one `$`, not `$$`.
- `15 to 24 months` does **not** become `$15,000 to $24,000`.
- Known-good phrases still apply (`water trick` → Waterpik / `water-pick`
  per the chosen target).
- Existing `test_cleaning_comprehensive.py` updated and green.

**Out of scope:** Whisper prompts, model size, ffmpeg QC.

---

### Phase 2 — Whisper call options (mocked)

**Goal:** Every transcribe uses English, a clinic glossary prompt, and
anti-loop settings. Load the model once per process. Only new audio files
are transcribed.

**Code**

- `language="en"`.
- `condition_on_previous_text=False`.
- Reasonable `no_speech_threshold` (document the value in the PR).
- `initial_prompt` built from a **glossary of target terms** (Kamp,
  Merrillville, implant, all-on-four, all-on-six, snap-in denture, temporary,
  zirconia, gingivectomy, overdenture, Waterpik) — the dictionary *targets*,
  not the wrong-side keys.
- Cache `load_model` on the module or a small helper.
- Pipeline / `transcribe_all_audio_files` only processes files that lack a
  `.txt` transcript (match `find_new_audio_files`).

**Tests in this PR**

- Mock `whisper.load_model`; assert `transcribe` kwargs include `language`,
  `initial_prompt` (contains `implant` / `snap-in`), and
  `condition_on_previous_text is False`.
- `load_model` called once when two files are processed in one run.
- Given an existing `{stem}.txt`, a second run does not call `transcribe`
  for that stem.
- No real audio, no network.

---

### Phase 3 — Fail-fast audio health (mocked ffmpeg)

**Goal:** Near-silent files (Wyatt: mean ≈ −90 dB, peak ≈ −81 dB) abort
before Whisper and write a clear log line.

**Code**

- Small `audio_qc` helper using `ffmpeg -af volumedetect` (already required
  on PATH for m4a).
- Fail if peak below a documented threshold (start around −50 dB peak; tune
  from Wyatt vs a known-good consult).
- Pipeline: QC each *new* file; skip / fail that file; do not write a fake
  `...` transcript.
- `mdc consult-audio validate` can keep warning if ffmpeg is missing; QC
  should fail closed if ffmpeg is missing at **run** time.

**Tests in this PR**

- Parse a fixture `volumedetect` stderr snippet (Wyatt-like vs speech-like).
- Pipeline/transcription unit test: QC fail → `transcribe` not called.
- Do not commit the Wyatt m4a.

---

### Phase 4 — Model size behind env (optional quality)

**Goal:** Default stays `base`. Operators can set `WHISPER_MODEL=small` or
`medium` locally for real consults.

**Code**

- Read model name from env (allowlist: `base`, `small`, `medium` — not
  `large` in v1).
- Document RAM/time tradeoff in `consult_audio_pipe/README.md`.
- Still load once per process.

**Tests in this PR**

- Mock `load_model`; assert it is called with `os.environ["WHISPER_MODEL"]`
  when set; default `base`.
- Reject unknown model names with a clear error (no download).

**Out of scope:** CI running `small`/`medium`. No checkpoint in git.

---

### Phase 5 — Harvest process (docs + optional helper)

**Goal:** A repeatable way to add **phrases**, not one-word landmines.

**Docs / helper**

- How to diff raw vs clean on a *local* consult (PHI stays on disk).
- Add a phrase only if it appears twice or is unambiguous in isolation.
- Optional script: scan local `transcripts/*.txt` for leftover tokens; print
  candidates; never write PHI into the repo.

**Tests in this PR**

- If a helper exists: unit tests on synthetic lines only.
- Dictionary schema / allowlist test: fail CI if a new key is a single
  token shorter than N characters unless it is on an explicit allowlist.

Only after Phases 1–2. Do not open this PR with a dump of new replacements.

---

## Branching and PR sequence

```
main
  └── feature/consult-audio-transcript-quality
        ├── pr/phase-0-ci          → merge to feature or straight to main
        ├── pr/phase-1-safer-clean
        ├── pr/phase-2-whisper-opts
        ├── pr/phase-3-audio-qc
        ├── pr/phase-4-model-env
        └── pr/phase-5-harvest
```

Prefer **each phase PR into `main`** if the feature branch is not shared.
Do not squash Phase 1 into Phase 4.

PR title pattern: `fix(consult-audio): safer snap-in / money cleaning (phase 1)`.

Each PR body:

- What changed and why (one phase).
- What is explicitly out of scope.
- Test plan (copy the phase checklist + the shared close-out below).
- Confirm no PHI added (`git status` under `consult_audio_pipe/raw_audio`,
  `transcripts`, `transcripts_clean`).

---

## Close-out PR and test plan

Use this on the **last** phase that lands on `main` (or a wrap-up PR if
phases merged independently). Earlier PRs use the subset for that phase.

### PR description (template)

```markdown
## Summary
- Phase N of docs/consult_audio/TRANSCRIPTION_AND_CLEANING_PLAN.md
- <one sentence user-facing effect>

## Test plan
- [ ] CI `consult_audio_pipe` workflow green on this PR
- [ ] `pytest consult_audio_pipe/tests -q` in consult_audio_pipe/venv
- [ ] No new gitignored PHI staged
- [ ] Phase-specific boxes below
```

### Automated (CI and local)

- [ ] New workflow runs on the PR (`consult_audio_pipe/**` path filter).
- [ ] Live API tests not required for merge (`-m "not integration"`).
- [ ] Cleaning regressions: snap-in / snaps / grand / bucks / months range.
- [ ] Whisper `transcribe` kwargs mocked (Phase 2+).
- [ ] Model loaded once per run (Phase 2+).
- [ ] Existing `{stem}.txt` skipped (Phase 2+).
- [ ] QC fail skips Whisper (Phase 3+).
- [ ] `WHISPER_MODEL` allowlist (Phase 4+).
- [ ] Short dictionary keys rejected unless allowlisted (Phase 5+).

### Local only (not CI) — after Phase 2+

Use a **non-PHI or already-processed** consult already on the workstation.
Do not attach files to the PR.

- [ ] `mdc consult-audio validate`
- [ ] `PYTHONIOENCODING=utf-8 mdc consult-audio pipeline status`
- [ ] Re-clean **only** a copy of Christina King (or equivalent) and confirm
      `snap-in` / `12 grand` / `35 bucks` are not regressing.
- [ ] Optional: one good-audio re-transcribe with `base` + new prompt; spot
      implant / snap-in / Kamp. Compare to the previous `.txt` locally.
- [ ] Optional Phase 4: same file with `WHISPER_MODEL=small` if the machine
      can take it; do not commit outputs.

### Do not

- Run the full historical `raw_audio_archive/` through Whisper for the PR.
- Commit HTML/PDF/summaries.
- Hit Claude/ChatGPT as a merge gate.
- Treat a Wyatt-class silent file as a transcription-quality sample.

### Merge

- Reviewer: tests + no PHI + phase scope.
- Merge with the repo default (merge commit or squash — follow current
  `main` practice).
- After merge: confirm the path-filtered workflow ran on `main`.

---

## Out of scope (all phases)

- Clinic frontend delivery (`CLINIC_FRONTEND_DELIVERY_PLAN.md`)
- Browser upload, Whisper on EC2, speaker diarization
- Warehouse / dbt models for transcripts
- Growing the replace list before Phase 1
- Amplifying silent files to “recover” speech
