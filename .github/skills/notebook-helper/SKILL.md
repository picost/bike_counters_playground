---
name: notebook-helper
description: "Universal notebook copilot skill. Use when helping in Jupyter notebooks for data analysis, exploration, modeling, or reporting, with step-by-step collaboration, strong narrative cells, and reproducible code execution."
argument-hint: "Describe notebook path, current step, goal, and whether you want discussion, code, or direct cell edits"
user-invocable: true
---

# Notebook Helper

## Purpose
Provide a reusable, agent-agnostic workflow for helping in notebooks while keeping outputs:
- clear for human readers,
- technically correct and reproducible,
- decision-oriented at each step.

## When to Use
- Working in any notebook (`.ipynb`) for analysis, data science, ML, reporting, or experimentation.
- Requesting collaborative reasoning before writing code.
- Refactoring notebook structure for readability and interview/peer review quality.
- Adding or editing cells while preserving a coherent narrative.

## Collaboration Contract
1. Clarify the immediate objective before coding.
2. If context is missing, ask targeted questions.
3. Propose 2-3 viable approaches with trade-offs when design choices matter.
4. Recommend one approach and explain why.
5. Implement only what is needed for the current step.
6. After each step, summarize findings and propose the next step.

## Narrative Rules (Mandatory)
- Never provide a code cell without context.
- For each code cell, include a markdown explanation before and after.
- Before-code markdown must state:
  - what the cell does,
  - why it is needed,
  - what output is expected.
- After-code markdown must state:
  - the key result,
  - how to interpret it,
  - the next action or decision.
- Notebook wording must be in English unless the user explicitly asks otherwise.
- Avoid first-person wording in notebook content.

## Code and Reproducibility Rules
- Prefer readable, explicit code over compact but opaque code.
- Keep imports organized near the top unless a late import is justified.
- Include validation checks after data-impacting transformations.
- Make plotting output self-explanatory:
  - add clear titles,
  - label axes,
  - include units when relevant.
- Ensure notebook flow can run top-to-bottom without hidden state assumptions.

## Execution Safety Rules
- Confirm goal and constraints before destructive or large-scale edits.
- Do not silently ignore warnings/errors that affect conclusions.
- When a result depends on assumptions, state them explicitly.
- Flag leakage risks, data quality risks, and reproducibility risks only
  when they are relevant to the current step and not already mitigated.
  Do not flag them as a blanket statement for every step.

## Output Template (Default)
For each meaningful step, return:
1. Objective and assumptions.
2. Options and trade-offs (if applicable).
3. Recommended approach.
4. Notebook-ready markdown before code.
5. Notebook-ready code.
6. Notebook-ready markdown after code.
7. Risks/checks and next step.

## Notebook Edit Format
When asked to generate notebook document content, output valid JSON with a `cells` array.
- Each cell must include `metadata.language`.
- Existing cells must preserve `metadata.id`.
- New cells do not need `metadata.id`.
- Keep the structure valid and logically ordered.

## Non-Goals
- Do not introduce unnecessary architecture changes for a simple notebook step.
- Do not jump to modeling before validating data preparation assumptions.
- Do not prioritize code volume over clarity of reasoning.