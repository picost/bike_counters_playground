# AI and Agent Methodological Cheat-Sheet

## Quick start with Research Orchestrator
Use the Research Orchestrator agent as a single entrypoint and keep the discussion in one thread.

Copy-paste prompts:

1. Weekly kickoff (framing + assumptions)
"Use 02_data_exploration.ipynb. Research question: how weather affects normalized bike counts across counters. Current stage: framing. Build an assumptions register and propose two analysis routes with trade-offs."

2. Midweek challenge (interpretation stress test)
"Use 02_data_exploration.ipynb. Current stage: intermediate interpretation. Challenge the current temperature and rain interpretation with competing explanations and provide falsification checks."

3. End-of-week synthesis (decision-ready rationale)
"Use 02_data_exploration.ipynb. Current stage: synthesis. Audit key claims against evidence, then produce a concise rationale with facts, inference, uncertainty, decision, and next question."

## Objective
Use AI to improve the quality, consistency, and auditability of scientific reasoning in notebooks.

## Core principle
Treat AI as a structured research collaborator, not an oracle.

Human responsibilities:
- Final judgment
- Domain interpretation
- Decision ownership

AI responsibilities:
- Structure reasoning
- Challenge assumptions
- Surface alternatives
- Improve clarity and consistency

## Recommended workflow per notebook section
1. Frame the question
- State one question and one decision context.
- Define what result would be useful.

2. Build assumptions register
- Run assumptions-register skill.
- Do not proceed to strong claims with critical untested assumptions.

3. Analyze and observe
- Produce outputs (tables, plots, model summaries).
- Keep interpretations separate from raw observations.

4. Stress-test interpretation
- Run Hypothesis Challenger agent.
- Run Methodology Reviewer agent.

5. Audit claims
- Run claim-audit skill.
- Downgrade or remove unsupported claims.

6. Synthesize rationale
- Run Scientific Writer agent or rationale-synthesis skill.
- End with uncertainty and next-question statements.

## Agent usage guide
### Data Science Notebook Agent
Use for normal step-by-step notebook progress.

### Hypothesis Challenger
Use when a conclusion feels plausible but fragile.
Expected output:
- Competing explanations
- Falsification tests
- Missing discriminating evidence

### Methodology Reviewer
Use before accepting model-based claims.
Expected output:
- Assumption status
- Validity risks
- Required robustness checks

### Scientific Writer
Use after technical sections to standardize rationale.
Expected output:
- Facts
- Inference
- Uncertainty
- Decision
- Next question

## Skill usage guide
### assumptions-register
Use at the start of every major section.
Deliverable: assumptions table with checks and impact.

### robustness-ladder
Use when a result could affect decisions.
Deliverable: tiered validation checklist.

### claim-audit
Use before final section conclusions.
Deliverable: claim-evidence-confidence table.

### rationale-synthesis
Use when converting notes to a coherent narrative.
Deliverable: six-part scientific rationale section.

## Prompt templates
### Template A: Assumption setup
Context: [question, data scope, method]
Task: Build an assumptions register with check methods, status, and impact if violated.
Constraint: Keep it specific to available data.

### Template B: Critical challenge
Context: [current claim + evidence]
Task: Provide 3 competing explanations and falsification checks.
Constraint: Do not use evidence that is not present.

### Template C: Method validity
Context: [method + assumptions + outputs]
Task: Evaluate validity risks and minimum robustness checks.
Constraint: Separate exploratory from confirmatory claims.

### Template D: Rationale polishing
Context: [draft section + key outputs]
Task: Rewrite as facts, inference, uncertainty, decision, and next question.
Constraint: No unsupported causal language.

## Quality gates before moving on
A section can be considered complete only if:
- Every key claim is linked to explicit evidence.
- Critical assumptions are tested or clearly flagged.
- At least one alternative explanation is considered.
- Uncertainty is explicitly stated.
- Next step is justified by current evidence.

## Anti-patterns to avoid
- Asking AI for final conclusions before checking assumptions.
- Accepting fluent text as evidence.
- Mixing exploratory and confirmatory language.
- Hiding uncertainty to sound decisive.
- Producing large code blocks without narrative checkpoints.

## Weekly operating rhythm
1. Start of week: frame questions and assumptions.
2. Midweek: run challenge and methodology reviews.
3. End of week: run claim audit and rationale synthesis.

## Minimal section template
Use this markdown skeleton in notebooks:

Question:

Decision context:

Assumptions:

Observed evidence:

Alternative explanations considered:

Conclusion with confidence:

What could change this conclusion:

Next question:
