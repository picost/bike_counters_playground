---
name: rationale-synthesis
description: "Use when turning analysis outputs into a consistent scientific rationale with explicit limits and decision logic."
argument-hint: "Provide the section draft, key outputs, and the decision that depends on this section."
user-invocable: true
---

# Rationale Synthesis

## Purpose
Produce coherent, decision-ready scientific narrative from notebook outputs.

## When to Use
- At the end of each major notebook section.
- Before sharing findings with collaborators.
- Before drafting recommendations.

## Required Structure
1. Question being addressed
2. Why this method was used
3. Key evidence observed
4. Interpretation boundaries and limits
5. Decision statement
6. Next question or next check

## Output Format
Return the section as six titled paragraphs matching the required structure.

## Rules
- Distinguish fact from inference in every paragraph.
- Keep uncertainty explicit and proportional to evidence.
- Avoid unsupported causal language.
- Ensure recommendations are conditional on confidence.
