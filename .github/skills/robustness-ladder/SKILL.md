---
name: robustness-ladder
description: "Use when escalating from a promising result to stronger validation through a tiered robustness checklist."
argument-hint: "Provide the key result, current checks completed, and decision sensitivity."
user-invocable: true
---

# Robustness Ladder

## Purpose
Define staged checks that increase confidence in findings.

## When to Use
- After an initial result appears important.
- Before recommendations that may drive decisions.
- When comparing rival explanations.

## Required Procedure
Build a three-tier validation plan:
1. Minimal checks (sanity and implementation validity)
2. Strong checks (stability and sensitivity)
3. High-rigor checks (external validity and stress conditions)

For each check include:
- Objective
- Failure condition
- Interpretation impact
- Priority (high, medium, low)

## Output Format
Return grouped bullet lists under:
- Tier 1: Minimal checks
- Tier 2: Strong checks
- Tier 3: High-rigor checks

Each item must include objective, failure condition, and impact.

## Rules
- Prioritize checks that could materially alter conclusions.
- Avoid long checklists with low decision value.
- Explicitly state whether current evidence clears each tier.
