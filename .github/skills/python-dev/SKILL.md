---
name: python-dev
description: "Python development style guide. Use when writing, reviewing, or refactoring Python code with PEP 8, modular design, and high-quality numpydoc documentation with explicit physical units and sampling assumptions."
argument-hint: "Describe the Python task, constraints, and whether you want code generation, refactor, or review"
user-invocable: true
---

# Python Development Helper

## Purpose
Define consistent Python coding, design, and documentation standards for this workspace.

## When to Use
- Writing new Python modules, functions, or classes.
- Refactoring existing Python code for readability and maintainability.
- Reviewing Python code quality and documentation quality.

## Coding Rules (Mandatory)
- Follow PEP 8 style.
- Do not use global variables; pass dependencies and values explicitly.
- Avoid type annotations unless explicitly requested by the user.
- Use descriptive variable, function, and class names.
- Include docstrings for all public functions and classes.
- Do not add blank lines inside function bodies unless required for correctness.
- Keep function bodies compact and visually continuous.

## Design Rules (Mandatory)
- Prefer simple, modular functions that do one thing well.
- Keep function responsibilities narrow and explicit.
- Favor clarity and maintainability over clever shortcuts.

## Documentation Rules (Mandatory)
- Use numpydoc style for docstrings.
- Do not paraphrase code line by line.
- Explain algorithmic rationale, assumptions, and edge-case behavior.
- Explain intended usage conditions when relevant.
- Use inline comments sparingly.
- Inline comments should be reserved for subtle implementation details only.
- Important conceptual information must be documented in the algorithm/rationale section of docstrings, not in scattered inline comments.

## Physical Quantities and Units
- Always provide units for physical quantities in parameters and returns.
- Write units in square brackets, for example: `temperature [deg C]`.
- In parameter/return descriptions, explicitly state the physical quantity.

## Sequential Data and Sampling Semantics
- For sequential/time-indexed data, always state discretization semantics explicitly.
- Example semantics:
  - The value at time `t_i` is the average over interval `[t_i, t_{i+1}[`.
  - The value at time `t_i` is the instantaneous value at `t_i`.
- If multiple parameters share the same sampling scheme, document it once in a docstring note.

## Complex-Code Traceability Pattern (Optional)
When implementation is complex, link documentation steps to code markers.
- In docstring explanations, use numbered references such as `[1.1.]`.
- In code, place matching inline markers such as `# [1.1.]`.
- Use this pattern sparingly and only when it improves comprehension.

## Review Checklist
- PEP 8 respected.
- No unnecessary globals.
- No unsolicited type hints.
- No blank lines inside function bodies (unless technically required).
- Docstrings present and meaningful.
- Inline comments used sparingly and only for implementation subtleties.
- Algorithm-level important information is documented in docstrings, not inline comments.
- Units and physical meaning documented.
- Sampling semantics explicit for sequential data.
- Design remains modular and readable.