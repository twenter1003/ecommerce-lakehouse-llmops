# Antigravity Rules for ETL_EC Project

## User Instructions & Persona
1. **Interactive Mentoring Mode**:
   - Do NOT dump massive blocks of code across multiple files without the user's step-by-step understanding.
   - For every architectural decision or new milestone, explain the "Why" (trade-offs, why this tech over alternatives).
   - Verify each step with terminal outputs and tests before proceeding to the next.

2. **Blog / TIL Summary Trigger**:
   - When the user says **"오늘 여기까지"** (or implies ending for the day), generate a comprehensive, copy-paste ready Velog (Markdown) draft covering:
     - Today's background and goals
     - Architectural & technical decisions made
     - Real errors encountered and how they were solved (troubleshooting)
     - Core DE interview questions & answers related to today's work.

3. **Domain & Tech Focus**:
   - Domain: E-commerce (Clickstream, Orders, Payments, Cancellations)
   - Architecture: Kafka (Redpanda) -> Spark Structured Streaming -> Apache Iceberg Lakehouse -> dbt -> LLMOps (Product Catalog Vector ETL & Serving API)

4. **Mandatory Active Skills Execution Protocol**:
   - Every task must strictly invoke and enforce the assigned skills chain defined in `.agents/rules/active-skills-execution.md`:
     - Planning & Architecture: `de-debate`, `ponytail` (YAGNI)
     - Coding & Edits: `karpathy-guidelines` (Surgical, Simplicity, Goal-driven)
     - Data Pipeline: `kafka-infra-ops`, `gcp-spark`, `schema-mapping`
     - Debugging: `superpowers:systematic-debugging`
     - Verification: `superpowers:verification-before-completion`

