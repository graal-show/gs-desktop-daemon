use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{collections::BTreeSet, fs, path::PathBuf};

fn load_contract() -> Result<Value> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("ores-generation-contract.json");
    let content = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&content).context("parse generation contract JSON")
}

/// This validates the consumer contract itself; it does not claim that the
/// shared authority implementation or runtime handoff has passed conformance.
fn verify_contract(d: &Value) -> Result<()> {
    ensure!(
        d["schema"] == "ores.desktop-generation-consumer/v1",
        "wrong schema"
    );
    ensure!(
        d["consumer"]["repository"] == "graal-show/gs-desktop-daemon",
        "wrong consumer identity"
    );
    ensure!(
        d["authority"]["repository"] == "ORESoftware/ores-common-desktop-infra",
        "wrong authority repository"
    );
    let revision = d["authority"]["revision"]
        .as_str()
        .context("missing authority revision")?;
    ensure!(
        revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "invalid authority revision"
    );
    ensure!(
        d["lifecycle"]
            == json!([
                "prepare",
                "validate",
                "compile_build_generation",
                "stage",
                "health_check",
                "atomic_activate",
                "bounded_drain",
                "commit"
            ]),
        "lifecycle must preserve the generation activation order"
    );
    ensure!(
        d["rollback"]
            == json!({
                "required_before_commit": true,
                "retain_previous_generation": true
            }),
        "rollback must be possible before commit"
    );
    ensure!(
        d["request_semantics"]["new_requests"] == "active_generation"
            && d["request_semantics"]["existing_requests"] == "pinned_generation"
            && d["request_semantics"]["generation_identity_required"] == true,
        "requests must remain generation-pinned"
    );
    ensure!(
        d["routing"]["edge_proxy_route_authority"] == false,
        "edge proxy cannot own dynamic route authority"
    );
    let edges = d["routing"]["stable_edges"]
        .as_array()
        .context("missing stable edge list")?;
    let stable_edges: BTreeSet<&str> = edges.iter().filter_map(Value::as_str).collect();
    for edge in ["nginx", "haproxy", "caddy"] {
        ensure!(stable_edges.contains(edge), "missing stable edge {edge}");
    }
    ensure!(
        d["isolation"]["tenant_boundary"] == "os_process_per_tenant_generation"
            && d["isolation"]["unbounded_affinity_cardinality_forbidden"] == true,
        "cross-tenant isolation and bounded affinity are mandatory"
    );
    ensure!(
        d["isolation"]["session_id_source"] == "trusted_auth_or_router"
            && d["isolation"]["route_session_opt_in"] == true,
        "session identity and opt-in must be trusted"
    );
    ensure!(
        d["middleware"]["beam_code_reload_requires_drain_or_otp_proof"] == true,
        "unverified BEAM code reload must not bypass drain"
    );
    ensure!(
        d["verification"]["shared_conformance_required"] == true
            && d["verification"]["product_e2e_required"] == true
            && d["verification"]["promotion_state"] == "candidate",
        "candidate status must remain non-promoted until conformance"
    );
    Ok(())
}

#[test]
fn generation_contract_is_well_formed_and_denies_implicit_promotion() -> Result<()> {
    verify_contract(&load_contract()?)
}

#[test]
fn critical_contract_mutations_fail_closed() -> Result<()> {
    let valid = load_contract()?;
    let mut cases = 0_u32;
    for (pointer, replacement) in [
        ("/schema", json!("ores.desktop-generation-consumer/v0")),
        ("/authority/revision", json!("not-a-sha")),
        ("/authority/repository", json!("attacker/fake-authority")),
        ("/consumer/repository", json!("attacker/copy")),
        ("/lifecycle/5", json!("commit")),
        ("/rollback/required_before_commit", json!(false)),
        ("/rollback/retain_previous_generation", json!(false)),
        ("/request_semantics/new_requests", json!("any_generation")),
        (
            "/request_semantics/existing_requests",
            json!("latest_generation"),
        ),
        (
            "/request_semantics/generation_identity_required",
            json!(false),
        ),
        ("/routing/edge_proxy_route_authority", json!(true)),
        ("/routing/stable_edges", json!(["nginx", "haproxy"])),
        ("/isolation/tenant_boundary", json!("shared_process")),
        (
            "/isolation/unbounded_affinity_cardinality_forbidden",
            json!(false),
        ),
        ("/isolation/session_id_source", json!("guest_provided")),
        ("/isolation/route_session_opt_in", json!(false)),
        (
            "/middleware/beam_code_reload_requires_drain_or_otp_proof",
            json!(false),
        ),
        ("/verification/shared_conformance_required", json!(false)),
        ("/verification/product_e2e_required", json!(false)),
        ("/verification/promotion_state", json!("approved")),
    ] {
        let mut mutated = valid.clone();
        let value = mutated.pointer_mut(pointer).context("test fixture path")?;
        *value = replacement;
        ensure!(
            verify_contract(&mutated).is_err(),
            "contract verifier accepted forbidden mutation at {pointer}"
        );
        cases += 1;
    }
    assert_eq!(cases, 20);
    Ok(())
}

#[test]
fn absent_revision_and_missing_stable_edge_also_fail_closed() -> Result<()> {
    let mut invalid = load_contract()?;
    invalid["authority"]
        .as_object_mut()
        .context("authority object")?
        .remove("revision");
    ensure!(
        verify_contract(&invalid).is_err(),
        "missing revision accepted"
    );
    let mut invalid = load_contract()?;
    invalid["routing"]
        .as_object_mut()
        .context("routing object")?
        .remove("stable_edges");
    ensure!(
        verify_contract(&invalid).is_err(),
        "missing edge list accepted"
    );
    Ok(())
}
