---
name: claim-audit
description: "Use when evaluating conclusion quality by mapping each claim to evidence, alternatives, and confidence level."
argument-hint: "Provide the claims section and references to tables/plots or notebook outputs."
user-invocable: true
---

# Claim Audit

## Purpose
Evaluate whether conclusions are adequately supported by evidence.

## When to Use
- Before finalizing any notebook conclusion section.
- During peer-review style self-checks.
- When claims feel plausible but potentially under-supported.

## Required Procedure
1. List each claim as a single sentence.
2. Link each claim to concrete evidence (table, plot, statistic, model output).
3. Provide at least one plausible alternative explanation.
4. Assign confidence: high, medium, or low.
5. State what new evidence could increase or decrease confidence.

## Output Format
Return a markdown table with columns:
- Claim
- Evidence reference
- Alternative explanation
- Confidence
- What would change confidence

## Rules
- If evidence is missing, mark claim as unsupported.
- Keep interpretation separate from factual observation.
- Do not upgrade confidence without explicit evidence.
