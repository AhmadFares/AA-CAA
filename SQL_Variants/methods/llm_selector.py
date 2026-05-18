"""
LLM-based source ranking for TVD experiments.

One API call per UR: given the UR + per-source statistics, the LLM returns
an ordered list of sources (most to least relevant).  The existing exploration
loop then runs in that order — same SQL execution, same stopping condition.

Supported backends (set via env vars):
  OpenAI     — OPENAI_API_KEY=sk-...           LLM_MODEL=gpt-4o-mini
  Groq       — LLM_BACKEND=groq               LLM_API_KEY=gsk_...    LLM_MODEL=llama-3.3-70b-versatile
  Ollama     — LLM_BACKEND=ollama             LLM_MODEL=llama3.1     (no key needed)
  Gemini     — LLM_BACKEND=gemini             LLM_API_KEY=AIza...    LLM_MODEL=gemini-1.5-flash
  llama_cpp  — LLM_BACKEND=llama_cpp         LLAMA_CPP_MODEL_PATH=/path/to/model.gguf

Uses the `requests` library (no openai SDK needed).
"""

import os
import json
import time
import logging
import requests

log = logging.getLogger(__name__)

# ── backend config from env vars ──────────────────────────────────────────────
_BACKEND  = os.environ.get("LLM_BACKEND", "openai").lower()   # openai | groq | ollama | gemini

# For ollama: host where ollama serve is running (default localhost)
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")

_BASE_URLS = {
    "openai": "https://api.openai.com/v1",
    "groq":   "https://api.groq.com/openai/v1",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/openai/",
}

_DEFAULT_MODELS = {
    "openai": "gpt-4o-mini",
    "groq":   "llama-3.3-70b-versatile",
    "ollama": "llama3.1",
    "gemini": "gemini-1.5-flash",
}

LLM_MODEL    = os.environ.get("LLM_MODEL", _DEFAULT_MODELS.get(_BACKEND, "gpt-4o-mini"))
LLM_API_KEY  = (os.environ.get("LLM_API_KEY") or os.environ.get("OPENAI_API_KEY") or "")
LLM_BASE_URL = _BASE_URLS.get(_BACKEND, _BASE_URLS["openai"])

MAX_RETRIES = 6
# Min seconds between calls — only applies to remote APIs (groq/openai/gemini)
_MIN_CALL_INTERVAL = float(os.environ.get("LLM_CALL_INTERVAL", "6.0"))
_last_call_time: float = 0.0

# ── ollama client (lazy-loaded so non-ollama runs don't need langchain) ────────
_ollama_client = None

def _get_ollama_client():
    global _ollama_client
    if _ollama_client is None:
        from langchain_ollama import ChatOllama
        _ollama_client = ChatOllama(
            model=LLM_MODEL,
            base_url=OLLAMA_HOST,
            temperature=0,
        )
    return _ollama_client


# ── llama-cpp-python client (lazy-loaded, GPU via GGUF file) ──────────────────
_llama_cpp_model = None

def _get_llama_cpp_model():
    global _llama_cpp_model
    if _llama_cpp_model is None:
        from llama_cpp import Llama
        model_path = os.environ.get("LLAMA_CPP_MODEL_PATH", "")
        if not model_path:
            raise ValueError("LLAMA_CPP_MODEL_PATH must be set for llama_cpp backend")
        n_gpu_layers = int(os.environ.get("LLAMA_CPP_N_GPU_LAYERS", "-1"))
        n_ctx = int(os.environ.get("LLAMA_CPP_N_CTX", "16384"))
        _llama_cpp_model = Llama(
            model_path=model_path,
            n_gpu_layers=n_gpu_layers,
            n_ctx=n_ctx,
            verbose=False,
        )
    return _llama_cpp_model


def _call_llm_llama_cpp(prompt: str) -> list[int] | None:
    """Call local GGUF model via llama-cpp-python (GPU-accelerated)."""
    llm = _get_llama_cpp_model()
    for attempt in range(MAX_RETRIES):
        raw = ""
        try:
            out = llm.create_chat_completion(
                messages=[
                    {"role": "system", "content": "You are a data discovery expert. Return ONLY a valid JSON array, no explanation."},
                    {"role": "user",   "content": prompt},
                ],
                max_tokens=512,
                temperature=0,
                response_format={"type": "json_object"},
            )
            raw = _parse_raw(out["choices"][0]["message"]["content"])
            ranked = json.loads(raw)
            if isinstance(ranked, list):
                return [int(x) for x in ranked]
            # model might wrap in a dict key
            if isinstance(ranked, dict):
                for v in ranked.values():
                    if isinstance(v, list):
                        return [int(x) for x in v]
        except json.JSONDecodeError as e:
            log.warning(f"llama_cpp JSON parse error (attempt {attempt+1}): {e}  raw={raw!r}")
        except Exception as e:
            log.warning(f"llama_cpp error (attempt {attempt+1}): {e}")
            time.sleep(2 * (attempt + 1))
    return None


def _call_llm_llama_cpp_single(prompt: str) -> int | None:
    """Single-source variant for llama-cpp-python."""
    llm = _get_llama_cpp_model()
    for attempt in range(MAX_RETRIES):
        raw = ""
        try:
            out = llm.create_chat_completion(
                messages=[
                    {"role": "system", "content": "You are a data discovery expert. Return ONLY a valid JSON object with key 'next', no explanation."},
                    {"role": "user",   "content": prompt},
                ],
                max_tokens=32,
                temperature=0,
                response_format={"type": "json_object"},
            )
            raw = _parse_raw(out["choices"][0]["message"]["content"])
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and "next" in parsed:
                return int(parsed["next"])
            if isinstance(parsed, int):
                return parsed
        except json.JSONDecodeError as e:
            log.warning(f"llama_cpp JSON parse error (attempt {attempt+1}): {e}  raw={raw!r}")
        except Exception as e:
            log.warning(f"llama_cpp error (attempt {attempt+1}): {e}")
            time.sleep(2 * (attempt + 1))
    return None


# ── prompt builders ────────────────────────────────────────────────────────────

def _format_ur_aa(UR: dict) -> str:
    lines = ["User Requirement:"]
    for attr, vals in UR.items():
        vals_str = ", ".join(str(v) for v in vals[:20])
        suffix   = f"  … (+{len(vals)-20} more)" if len(vals) > 20 else ""
        lines.append(f"  {attr}: [{vals_str}{suffix}]")
    return "\n".join(lines)


def _format_ur_caa(UR: dict) -> str:
    lines = ["User Requirement (we want rows that touch at least one of these values per attribute,",
             "but also contain as many OTHER non-listed values as possible):"]
    for attr, vals in UR.items():
        vals_str = ", ".join(str(v) for v in vals[:20])
        suffix   = f"  … (+{len(vals)-20} more)" if len(vals) > 20 else ""
        lines.append(f"  {attr}: [{vals_str}{suffix}]")
    return "\n".join(lines)


def _format_sources_aa(remaining_sources, stats, UR: dict) -> str:
    """AA: show only UR-value frequencies per source."""
    value_index  = stats["value_index"]
    source_sizes = stats.get("source_sizes", {})
    lines        = ["\nCandidate Sources:"]

    for src_idx, table_name in remaining_sources:
        vec  = stats["source_vectors"][src_idx]
        size = int(source_sizes.get(src_idx, 0)) if isinstance(source_sizes, dict) \
               else (int(source_sizes[src_idx]) if src_idx < len(source_sizes) else 0)
        lines.append(f"\n  Source {src_idx} ({table_name})  [{size} rows]")

        for attr, ur_vals in UR.items():
            present, absent = [], []
            for v in ur_vals:
                j = value_index.get(f"{attr}:{v}") or value_index.get(f"{attr}:{float(v)}" if isinstance(v, int) else None)
                if j is not None:
                    p = float(vec[j]) if hasattr(vec, '__getitem__') else 0.0
                    if p > 0:
                        present.append(f"{v}({p:.2%})")
                    else:
                        absent.append(str(v))
                else:
                    absent.append(str(v))
            if present:
                line = f"    {attr}: {', '.join(present[:10])}"
                if len(present) > 10:
                    line += f"  +{len(present)-10} more"
                if absent:
                    line += f"  |  absent: {', '.join(absent[:5])}"
                    if len(absent) > 5:
                        line += f" +{len(absent)-5} more"
            else:
                line = f"    {attr}: (none of the requested values found)"
            lines.append(line)

    return "\n".join(lines)


def _format_sources_caa(remaining_sources, stats, UR: dict, top_n: int = 5) -> str:
    """CAA: show UR-value frequencies + top non-UR values per attribute per source."""
    value_index  = stats["value_index"]
    source_sizes = stats.get("source_sizes", {})

    # build reverse index: attr → {val → j}
    attr_all_vals: dict[str, dict] = {}
    for key, j in value_index.items():
        if ":" not in key:
            continue
        attr, val = key.split(":", 1)
        attr_all_vals.setdefault(attr, {})[val] = j

    lines = ["\nCandidate Sources:"]

    for src_idx, table_name in remaining_sources:
        vec  = stats["source_vectors"][src_idx]
        size = int(source_sizes.get(src_idx, 0)) if isinstance(source_sizes, dict) \
               else (int(source_sizes[src_idx]) if src_idx < len(source_sizes) else 0)
        lines.append(f"\n  Source {src_idx} ({table_name})  [{size} rows]")

        for attr, ur_vals in UR.items():
            ur_val_set = {str(v) for v in ur_vals} | {str(float(v)) for v in ur_vals if isinstance(v, int)}

            # UR values present/absent
            present, absent = [], []
            for v in ur_vals:
                j = value_index.get(f"{attr}:{v}") or value_index.get(f"{attr}:{float(v)}" if isinstance(v, int) else None)
                if j is not None:
                    p = float(vec[j]) if hasattr(vec, '__getitem__') else 0.0
                    if p > 0:
                        present.append(f"{v}({p:.2%})")
                    else:
                        absent.append(str(v))
                else:
                    absent.append(str(v))

            if present:
                ur_line = f"    {attr} [requested]: {', '.join(present[:10])}"
                if absent:
                    ur_line += f"  |  absent: {', '.join(absent[:5])}"
            else:
                ur_line = f"    {attr} [requested]: (none found)"
            lines.append(ur_line)

            # non-UR values in this source
            non_ur = []
            for val, j in attr_all_vals.get(attr, {}).items():
                if val in ur_val_set:
                    continue
                p = float(vec[j]) if hasattr(vec, '__getitem__') else 0.0
                if p > 0:
                    non_ur.append((val, p))
            non_ur.sort(key=lambda x: -x[1])
            total_non_ur = len(non_ur)
            top = non_ur[:top_n]
            if top:
                top_str = ", ".join(f"{v}({p:.2%})" for v, p in top)
                lines.append(f"    {attr} [non-requested]: {total_non_ur} distinct values  top: {top_str}")
            else:
                lines.append(f"    {attr} [non-requested]: none found")

    return "\n".join(lines)


def _build_prompt(UR: dict, remaining_sources, stats, mode: str) -> str:
    source_ids = [str(src_idx) for src_idx, _ in remaining_sources]
    ids_str    = ", ".join(source_ids)

    if mode == "tvd-aa":
        ur_text  = _format_ur_aa(UR)
        src_text = _format_sources_aa(remaining_sources, stats, UR)
        goal = (
            "Goal: find rows where ALL requested attribute values appear together in the SAME row "
            "(co-occurrence).\n"
            "A row is useful only if it simultaneously satisfies one value from EACH attribute.\n"
            "For example, if the request is {illness: [flu, cold], symptom: [fever]}, a good row "
            "has illness=flu AND symptom=fever in the same row — not just one of them."
        )
        instruction = (
            "Rank sources from MOST to LEAST useful for finding co-occurring rows.\n"
            "Prefer sources where many requested values appear together with high frequency."
        )
    else:  # tvd-caa
        ur_text  = _format_ur_caa(UR)
        src_text = _format_sources_caa(remaining_sources, stats, UR)
        goal = (
            "Goal: find rows that touch AT LEAST ONE requested value per attribute, "
            "while also containing as many values NOT in the request as possible \n"

            "Rows that partially match (some attributes match, others do not) are preferred "
            "over rows that fully match all attributes."
        )
        instruction = (
            "Rank sources from MOST to LEAST useful.\n"
            "Prefer sources with many distinct non-requested values (high diversity) "
            "that still touch at least some requested values."
        )

    prompt = f"""You are a data discovery expert.

{goal}

{ur_text}
{src_text}

{instruction}
Return ONLY a valid JSON array of source indices in ranked order, most useful first.
Use exactly these indices: [{ids_str}]
Example format: [3, 0, 5, 1, 2, 4]
"""
    return prompt


# ── LLM call via requests (no openai SDK needed) ───────────────────────────────

def _throttle():
    global _last_call_time
    elapsed = time.time() - _last_call_time
    if elapsed < _MIN_CALL_INTERVAL:
        time.sleep(_MIN_CALL_INTERVAL - elapsed)
    _last_call_time = time.time()


def _parse_raw(raw: str) -> str:
    """Strip markdown code fences and extract first JSON object/array."""
    import re
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()
    # Extract first complete JSON array or object (ignores trailing special tokens)
    m = re.search(r'(\[[\s\S]*?\]|\{[\s\S]*?\})', raw)
    if m:
        return m.group(1)
    return raw


def _call_llm_ollama(prompt: str) -> list[int] | None:
    """Call local Ollama via langchain_ollama. No throttle, no API key needed."""
    from langchain_core.messages import HumanMessage, SystemMessage
    client = _get_ollama_client()
    messages = [
        SystemMessage(content="You are a data discovery expert. Return only valid JSON."),
        HumanMessage(content=prompt),
    ]
    for attempt in range(MAX_RETRIES):
        try:
            response = client.invoke(messages)
            raw = _parse_raw(response.content)
            ranked = json.loads(raw)
            if isinstance(ranked, list):
                return [int(x) for x in ranked]
        except json.JSONDecodeError as e:
            log.warning(f"Ollama JSON parse error (attempt {attempt+1}): {e}  raw={raw!r}")
        except Exception as e:
            log.warning(f"Ollama error (attempt {attempt+1}): {e}")
            time.sleep(2 * (attempt + 1))
    return None


def _call_llm_ollama_single(prompt: str) -> int | None:
    """Single-source variant for local Ollama."""
    from langchain_core.messages import HumanMessage, SystemMessage
    client = _get_ollama_client()
    messages = [
        SystemMessage(content="You are a data discovery expert. Return only valid JSON."),
        HumanMessage(content=prompt),
    ]
    for attempt in range(MAX_RETRIES):
        try:
            response = client.invoke(messages)
            raw = _parse_raw(response.content)
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and "next" in parsed:
                return int(parsed["next"])
            if isinstance(parsed, int):
                return parsed
        except json.JSONDecodeError as e:
            log.warning(f"Ollama JSON parse error (attempt {attempt+1}): {e}  raw={raw!r}")
        except Exception as e:
            log.warning(f"Ollama error (attempt {attempt+1}): {e}")
            time.sleep(2 * (attempt + 1))
    return None


def _call_llm(prompt: str) -> list[int] | None:
    if _BACKEND == "ollama":
        return _call_llm_ollama(prompt)
    if _BACKEND == "llama_cpp":
        return _call_llm_llama_cpp(prompt)

    _throttle()
    if not LLM_API_KEY:
        log.warning(f"No API key set for backend '{_BACKEND}'. "
                    "Set LLM_API_KEY (or OPENAI_API_KEY for openai backend).")
        return None

    url     = f"{LLM_BASE_URL.rstrip('/')}/chat/completions"
    headers = {"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": "You are a data discovery expert. Return only valid JSON."},
            {"role": "user",   "content": prompt},
        ],
        "temperature": 0,
        "max_tokens":  512,
    }

    raw = ""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code == 429:
                wait = 65
                log.warning(f"LLM rate-limited (attempt {attempt+1}), waiting {wait}s…")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            raw = _parse_raw(resp.json()["choices"][0]["message"]["content"])
            ranked = json.loads(raw)
            if isinstance(ranked, list):
                return [int(x) for x in ranked]
        except json.JSONDecodeError as e:
            log.warning(f"LLM JSON parse error (attempt {attempt+1}): {e}  raw={raw!r}")
        except Exception as e:
            log.warning(f"LLM API error (attempt {attempt+1}): {e}")
            time.sleep(2 * (attempt + 1))

    return None


# ── T summary formatters ─────────────────────────────────────────────────────

def _format_T_summary_aa(T, UR: dict, max_missing: int = 15) -> str:
    """Summarise what's already in T for AA: per-attr found/missing + covered combinations."""
    import itertools
    lines = ["Current result table T summary:"]

    if T is None or T.empty:
        lines.append("  (no rows collected yet)")
        return "\n".join(lines)

    attrs = list(UR.keys())

    # per-attribute found / missing
    for attr in attrs:
        if attr not in T.columns:
            lines.append(f"  {attr}: found=[]  missing={[str(v) for v in UR[attr]]}")
            continue
        t_vals = set(T[attr].dropna().astype(str))
        found   = [str(v) for v in UR[attr] if str(v) in t_vals]
        missing = [str(v) for v in UR[attr] if str(v) not in t_vals]
        lines.append(f"  {attr}: found={found}  missing={missing}")

    # co-occurrences: which UR combinations are already covered
    total_combs = 1
    for attr in attrs:
        total_combs *= len(UR[attr])

    covered, still_missing = [], []
    for comb in itertools.product(*[UR[a] for a in attrs]):
        mask = True
        for a, v in zip(attrs, comb):
            if a not in T.columns:
                mask = False
                break
            mask = mask & (T[a].astype(str) == str(v))
        found_comb = bool(mask.any()) if hasattr(mask, 'any') else False
        label = "(" + ", ".join(f"{a}={v}" for a, v in zip(attrs, comb)) + ")"
        if found_comb:
            covered.append(label)
        else:
            still_missing.append(label)

    lines.append(f"  Co-occurrences already covered ({len(covered)} of {total_combs} combinations):")
    if covered:
        lines.append("    " + ", ".join(covered[:15]))
    else:
        lines.append("    (none yet)")
    lines.append(f"  Still missing (up to {max_missing} shown):")
    if still_missing:
        lines.append("    " + ", ".join(still_missing[:max_missing]))
    else:
        lines.append("    (all covered)")

    return "\n".join(lines)


def _format_T_summary_caa(T, UR: dict, top_n: int = 5) -> str:
    """Summarise what's already in T for CAA: per-attr requested found/missing + non-requested collected."""
    lines = ["Current result table T summary:"]

    if T is None or T.empty:
        lines.append("  (no rows collected yet)")
        return "\n".join(lines)

    for attr, ur_vals in UR.items():
        if attr not in T.columns:
            lines.append(f"  {attr} requested:     found=[]  missing={[str(v) for v in ur_vals]}")
            lines.append(f"  {attr} non-requested: 0 distinct values collected")
            continue

        ur_val_set = {str(v) for v in ur_vals}
        t_vals     = set(T[attr].dropna().astype(str))

        found   = [str(v) for v in ur_vals if str(v) in t_vals]
        missing = [str(v) for v in ur_vals if str(v) not in t_vals]
        non_req = sorted(t_vals - ur_val_set)

        lines.append(f"  {attr} requested:     found={found}  missing={missing}")
        if non_req:
            sample = ", ".join(non_req[:top_n])
            lines.append(f"  {attr} non-requested: {len(non_req)} distinct values collected"
                         f"  e.g. {sample}")
        else:
            lines.append(f"  {attr} non-requested: 0 distinct values collected")

    return "\n".join(lines)


# ── adaptive prompt builder ───────────────────────────────────────────────────

def _build_adaptive_prompt(UR: dict, remaining_sources, stats, mode: str,
                           current_cov: float, current_pen: float,
                           threshold: float, step: int, T=None) -> str:
    source_ids = [str(src_idx) for src_idx, _ in remaining_sources]
    ids_str    = ", ".join(source_ids)

    if mode == "tvd-aa":
        ur_text    = _format_ur_aa(UR)
        src_text   = _format_sources_aa(remaining_sources, stats, UR)
        t_summary  = _format_T_summary_aa(T, UR)
        goal = (
            "Goal: find rows where ALL requested attribute values appear together in the SAME row "
            "(co-occurrence).\n"
            "A row is useful only if it simultaneously satisfies one value from EACH attribute.\n"
            "For example, if the request is {illness: [flu, cold], symptom: [fever]}, a good row "
            "has illness=flu AND symptom=fever in the same row — not just one of them."
        )
        state_text = (
            f"Current state after {step} sources explored:\n"
            f"  U-Coverage = {current_cov:.3f}  (goal θ = {threshold})\n"
        )
        task_note = (
            "Pick the single BEST source to explore next to increase U-Coverage toward θ.\n"
            "If you believe no remaining source will contribute new co-occurring rows "
            "(e.g. coverage already at θ, or remaining sources look irrelevant), return -1 to stop."
        )
    else:  # tvd-caa
        ur_text    = _format_ur_caa(UR)
        src_text   = _format_sources_caa(remaining_sources, stats, UR)
        t_summary  = _format_T_summary_caa(T, UR)
        goal = (
            "Goal: find rows that touch AT LEAST ONE requested value per attribute, "
            "while also containing as many values NOT in the request as possible "
            "(diverse, complementary data).\n"
            "Rows that partially match (some attributes match, others do not) are preferred "
            "over rows that fully match all attributes."
        )
        state_text = (
            f"Current state after {step} sources explored:\n"
            f"  E-Coverage = {current_cov:.3f}  (fraction of requested values found)\n"
            f"  Penalty    = {current_pen:.3f}  (fraction of found values that are NOT in the request — higher is better)\n"
            f"  ε = {threshold}  (stop when both stop improving by more than ε)\n"
        )
        task_note = (
            "Pick the single BEST source to explore next — one that adds new requested values "
            "(improves E-Coverage) and/or brings in many non-requested values (improves Penalty).\n"
            "If you believe no remaining source will meaningfully improve either metric, return -1 to stop."
        )

    prompt = f"""You are a data discovery expert guiding an adaptive exploration of data sources.

{goal}

{ur_text}

{state_text}
{t_summary}

{src_text}

{task_note}

Return ONLY a valid JSON object with a single key "next":
  - Set "next" to the integer index of the best source to explore next.
  - Set "next" to -1 if you choose to stop early.
Use exactly one of these indices (or -1): [{ids_str}]
Example: {{"next": 3}}
"""
    return prompt


def _call_llm_single(prompt: str) -> int | None:
    """Like _call_llm but expects {\"next\": <int>}. Returns src_idx or -1 (stop), None on failure."""
    if _BACKEND == "ollama":
        return _call_llm_ollama_single(prompt)
    if _BACKEND == "llama_cpp":
        return _call_llm_llama_cpp_single(prompt)

    _throttle()
    if not LLM_API_KEY:
        log.warning(f"No API key set for backend '{_BACKEND}'.")
        return None

    url     = f"{LLM_BASE_URL.rstrip('/')}/chat/completions"
    headers = {"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system",
             "content": "You are a data discovery expert. Return only valid JSON."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
        "max_tokens":  64,
    }

    raw = ""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code == 429:
                wait = 65
                log.warning(f"LLM rate-limited (attempt {attempt+1}), waiting {wait}s…")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            raw = _parse_raw(resp.json()["choices"][0]["message"]["content"])
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and "next" in parsed:
                return int(parsed["next"])
            # fallback: maybe LLM returned a bare integer
            if isinstance(parsed, int):
                return parsed
        except json.JSONDecodeError as e:
            log.warning(f"LLM JSON parse error (attempt {attempt+1}): {e}  raw={raw!r}")
        except Exception as e:
            wait = 2 * (attempt + 1)
            log.warning(f"LLM API error (attempt {attempt+1}): {e}")
            time.sleep(wait)

    return None


# ── public API ────────────────────────────────────────────────────────────────

def llm_rank_sources(UR: dict, remaining_sources: list, stats: dict,
                     mode: str = "tvd-aa") -> list:
    """
    Ask the LLM to rank remaining_sources for the given UR.

    Returns the sources list reordered by LLM preference.
    Falls back to original order if the LLM call fails.

    Parameters
    ----------
    UR               : {attr: [val, ...]}
    remaining_sources: [(src_idx, table_name), ...]
    stats            : stats dict (value_index, source_vectors, source_sizes)
    mode             : "tvd-aa" or "tvd-caa"

    Returns
    -------
    reordered list of (src_idx, table_name)
    """
    if not remaining_sources:
        return remaining_sources

    prompt = _build_prompt(UR, remaining_sources, stats, mode)
    ranked_ids = _call_llm(prompt)

    if ranked_ids is None:
        log.warning("LLM ranking failed — falling back to original order")
        return remaining_sources

    # build lookup: src_idx → (src_idx, table_name)
    src_map   = {src_idx: (src_idx, tname) for src_idx, tname in remaining_sources}
    all_ids   = set(src_map.keys())

    reordered = []
    seen      = set()

    for sid in ranked_ids:
        if sid in src_map and sid not in seen:
            reordered.append(src_map[sid])
            seen.add(sid)

    # append any sources the LLM missed (shouldn't happen but be safe)
    for sid in all_ids:
        if sid not in seen:
            reordered.append(src_map[sid])

    return reordered


def llm_select_next_source(UR: dict, remaining_sources: list, stats: dict,
                           mode: str, current_cov: float, current_pen: float,
                           threshold: float, step: int, T=None):
    """
    Adaptive variant: ask the LLM to pick the single best source at each step.

    Returns
    -------
    (src_idx, table_name)  — source to explore next
    None                   — LLM chose to stop (or call failed; caller falls back)
    """
    if not remaining_sources:
        return None

    prompt   = _build_adaptive_prompt(UR, remaining_sources, stats, mode,
                                      current_cov, current_pen, threshold, step, T=T)
    src_idx  = _call_llm_single(prompt)

    if src_idx is None:
        log.warning("LLM adaptive call failed — falling back to first remaining source")
        return remaining_sources[0]

    if src_idx == -1:
        log.info(f"LLM chose to stop at step {step} (cov={current_cov:.3f})")
        return None

    src_map = {i: (i, t) for i, t in remaining_sources}
    if src_idx in src_map:
        return src_map[src_idx]

    log.warning(f"LLM returned unknown src_idx={src_idx} — using first remaining")
    return remaining_sources[0]
