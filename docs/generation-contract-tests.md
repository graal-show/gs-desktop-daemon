# Rust generation contract tests

`tests/generation_contract.rs` replaces the former inline Python assertion
script in the generation-contract workflow.

Run `cargo +1.90.0 test --locked --test generation_contract`.

The Rust validator covers the schema identity, pinned shared authority
revision shape, ordered lifecycle, pre-commit rollback, generation-pinned
request semantics, edge router ownership, isolate affinity and mandatory
conformance gates. It also mutates 20 security-critical fields and confirms
they are rejected; missing authority revision and stable edge lists are
separately denied.

This validates the **consumer manifest**, not the executable shared
authority or end-to-end runtime. Promotion stays `candidate` until separate
shared conformance and product E2E evidence is produced.
