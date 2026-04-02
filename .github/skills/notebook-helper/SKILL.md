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
  - the rationale for the next step if applicable.
- Notebook wording must be in English unless the user explicitly asks otherwise.
- Avoid first-person wording in notebook content.
- No sections numbering.

## Cell Size and Focus (Mandatory)
- Each code cell should accomplish **one conceptual step** only.
- Maximum guideline: **10-15 lines per cell** unless the step logically requires more (~20 lines absolute limit).
- If a cell would exceed ~20 lines, split it into smaller cells with explanatory markdown in between.
- Use markdown to explain intermediate findings, decisions, and the rationale for each subsequent cell.
- **Aim for scannability**: Readers should understand the notebook narrative and flow in ~30 seconds.
- Break large processing tasks into multiple small, focused cells.
- Keep intermediate results visible and explicitly validated.

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

## Python Code Quality in Notebooks (Integrating python-dev)
When writing functions or substantial code in notebooks, follow python-dev standards:

**Coding Rules:**
- Follow PEP 8 style strictly.
- No global variables; pass dependencies explicitly.
- Use descriptive variable, function, and class names.
- Docstrings required for all public functions and classes (use numpydoc style).
- No blank lines inside function bodies (unless technically required).

**Design Rules:**
- Prefer simple, modular functions doing one thing well.
- Favor clarity and maintainability over clever shortcuts.

**Documentation Rules:**
- Use numpydoc style for docstrings.
- Explain algorithmic rationale, assumptions, and edge-case behavior.
- Explain intended usage conditions when relevant.
- Inline comments reserved for subtle implementation details only.
- Important conceptual information goes in docstrings, not scattered inline comments.

**Physical Quantities and Units:**
- For physical quantities in parameters/returns, provide units in square brackets, e.g., `temperature [deg C]`.
- Explicitly state the physical quantity in all descriptions.

**Sequential Data and Sampling Semantics:**
- For time-indexed data, always state discretization semantics explicitly.
- Example: "The value at time `t_i` is the average over interval `[t_i, t_{i+1}[`."

## Workflow (Detailed Approach)

### Before Coding
1. Clarify the immediate objective; ask targeted questions if context is missing.
2. If design choices matter, propose 2-3 approaches with trade-offs.
3. Recommend one approach and explain why.

### During Notebook Development
1. Inspect relevant data files and existing code before suggesting changes.
2. Plan notebook structure at a high level: identify major steps and decision points.
3. Implement one focused step at a time; do not build the entire notebook in one batch.
4. For each step: write a markdown cell → small code cell → markdown cell summary.
5. Write all code following Python Code Quality guidelines (PEP 8, numpydoc, units, semantics).
6. Include validation checks immediately after data-impacting operations.
7. Ensure the notebook reads top-to-bottom with no hidden state assumptions.

### After Each Step
1. Summarize the findings and key outputs.
2. Show validation results or representative output.
3. Propose the next step and ask for confirmation or direction.

## Output Template (Step-by-Step Structured Format)
For each meaningful notebook step, return:

1. **Objective and assumptions** — What this step accomplishes and assumptions about data/environment.
2. **Options and trade-offs** (if applicable) — Viable approaches with strengths/weaknesses.
3. **Recommended approach** — The chosen method and rationale.
4. **Notebook cells** (in order, ready to insert):
   - **Markdown (before):** What the cell does, why it's needed, expected output.
   - **Code:** Small, focused (≤15 lines), readable, PEP 8 compliant, with validation checks.
   - **Markdown (after):** Key results, interpretation, and the rationale for next step.
5. **Validation and next step** — What checks passed and what comes next.

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