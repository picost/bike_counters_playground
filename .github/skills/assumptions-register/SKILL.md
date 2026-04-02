---
name: assumptions-register
description: "Use when starting a notebook section to document structural, data, and modeling assumptions with validation plans."
argument-hint: "Provide the question, dataset scope, and intended method."
user-invocable: true
---

# Assumptions Register

## Purpose
Make assumptions explicit before interpretation begins.

## When to Use
- At the start of each major analysis block.
- Before model fitting or comparative interpretation.
- When moving from exploration to decision-oriented statements.

## Required Procedure
1. Record assumptions in three classes:
   - Structural assumptions
   - Data assumptions
   - Modeling assumptions
2. For each assumption, define:
   - Why it matters
   - How it will be checked
   - Impact if violated
3. Label status:
   - Untested
   - Tentatively supported
   - Supported
   - Violated

## Output Format
Return a markdown table with columns:
- Assumption
- Class
- Check method
- Status
- Impact if violated

## Rules
- Do not proceed to strong claims with critical untested assumptions.
- Mark causal assumptions explicitly.
- Keep checks feasible with available data and outputs.
