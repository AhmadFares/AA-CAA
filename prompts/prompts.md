# LLM Prompts

This file documents the prompts used by the two LLM variants (**LLM-Guided** and **LLM-Adaptive**) for both modes (TVD-AA and TVD-CAA). The full implementation is in `SQL_Variants/methods/llm_selector.py`.

---

## LLM-Guided

A **single prompt** is issued at the start of exploration. The model receives the query (UR) and per-source statistics, and returns a ranked list of all sources. Sources are then explored in that fixed order.

**System message:**
```
You are a data discovery expert. Return only valid JSON.
```

### TVD-AA prompt template

```
You are a data discovery expert.

Goal: find rows where ALL requested attribute values appear together in the SAME row
(co-occurrence).
A row is useful only if it simultaneously satisfies one value from EACH attribute.
For example, if the query is {illness: [flu, cold], symptom: [fever]}, a good row
has illness=flu AND symptom=fever in the same row — not just one of them.

query:
  <attribute>: [<value>, ...]
  ...

Sources and their statistics:
  Source <id>: size=<n> rows
    <attribute>: <value> → <frequency>%
    ...
  ...

Rank sources from MOST to LEAST useful for finding co-occurring rows.
Prefer sources where many requested values appear together with high frequency.
Return ONLY a valid JSON array of source indices in ranked order, most useful first.
Use exactly these indices: [<id>, ...]
Example format: [3, 0, 5, 1, 2, 4]
```

### TVD-CAA prompt template

```
You are a data discovery expert.

Goal: find rows that touch AT LEAST ONE requested value per attribute,
while also containing as many values NOT in the query as possible
(diverse, complementary data).
Rows that partially match (some attributes match, others do not) are preferred
over rows that fully match all attributes.

query:
  <attribute>: [<value>, ...]
  ...

Sources and their statistics:
  Source <id>: size=<n> rows
    <attribute>: requested values → coverage%; non-requested values → <count> distinct
    ...
  ...

Rank sources from MOST to LEAST useful.
Prefer sources with many distinct non-requested values (high diversity)
that still touch at least some requested values.
Return ONLY a valid JSON array of source indices in ranked order, most useful first.
Use exactly these indices: [<id>, ...]
Example format: [3, 0, 5, 1, 2, 4]
```

**Expected output:**
```json
[3, 0, 5, 1, 2, 4]
```

---

## LLM-Adaptive

**One prompt per exploration step.** The model receives the UR, remaining sources, current coverage/penalty state, and a summary of the result table built so far. It picks the single best next source, or returns `-1` to stop early.

**System message:**
```
You are a data discovery expert. Return ONLY a valid JSON object with key "next", no explanation.
```

### TVD-AA prompt template

```
You are a data discovery expert guiding an adaptive exploration of data sources.

Goal: find rows where ALL requested attribute values appear together in the SAME row
(co-occurrence).
A row is useful only if it simultaneously satisfies one value from EACH attribute.
For example, if the query is {illness: [flu, cold], symptom: [fever]}, a good row
has illness=flu AND symptom=fever in the same row — not just one of them.

query:
  <attribute>: [<value>, ...]
  ...

Current state after <k> sources explored:
  U-Coverage = <value>  (goal θ = <threshold>)

Current result table T summary:
  <attribute>: found=[...] missing=[...]
  Co-occurrences already covered (<n> of <total> combinations):
    (<attr>=<val>, ...)
  Still missing:
    (<attr>=<val>, ...)

Remaining sources and their statistics:
  Source <id>: size=<n> rows
    <attribute>: <value> → <frequency>%
    ...

Pick the single BEST source to explore next to increase U-Coverage toward θ.
If you believe no remaining source will contribute new co-occurring rows
(e.g. coverage already at θ, or remaining sources look irrelevant), return -1 to stop.

Return ONLY a valid JSON object with a single key "next":
  - Set "next" to the integer index of the best source to explore next.
  - Set "next" to -1 if you choose to stop early.
Use exactly one of these indices (or -1): [<id>, ...]
Example: {"next": 3}
```

### TVD-CAA prompt template

```
You are a data discovery expert guiding an adaptive exploration of data sources.

Goal: find rows that touch AT LEAST ONE requested value per attribute,
while also containing as many values NOT in the query as possible
(diverse, complementary data).
Rows that partially match (some attributes match, others do not) are preferred
over rows that fully match all attributes.

query:
  <attribute>: [<value>, ...]
  ...

Current state after <k> sources explored:
  E-Coverage = <value>  (fraction of requested values found)
  Penalty    = <value>  (fraction of found values that are NOT in the query — higher is better)
  ε = <threshold>  (stop when both stop improving by more than ε)

Current result table T summary:
  <attribute> requested:     found=[...]  missing=[...]
  <attribute> non-requested: <n> distinct values collected  e.g. <sample>

Remaining sources and their statistics:
  Source <id>: size=<n> rows
    <attribute>: requested values → coverage%; non-requested values → <count> distinct
    ...

Pick the single BEST source to explore next — one that adds new requested values
(improves E-Coverage) and/or brings in many non-requested values (improves Penalty).
If you believe no remaining source will meaningfully improve either metric, return -1 to stop.

Return ONLY a valid JSON object with a single key "next":
  - Set "next" to the integer index of the best source to explore next.
  - Set "next" to -1 if you choose to stop early.
Use exactly one of these indices (or -1): [<id>, ...]
Example: {"next": 3}
```

**Expected output:**
```json
{"next": 3}
```
or to stop:
```json
{"next": -1}
```
