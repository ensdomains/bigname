const V2_SEARCH_REGISTRY_OWNER: &str = "0x0000000000000000000000000000000000000d01";
const V2_SEARCH_CONTROL_OWNER: &str = "0x0000000000000000000000000000000000000d02";
const V2_SEARCH_REGISTRATION_REGISTRANT: &str = "0x0000000000000000000000000000000000000d03";
const V2_SEARCH_CONTROL_REGISTRANT: &str = "0x0000000000000000000000000000000000000d04";

#[tokio::test]
async fn v2_search_prefix_returns_record_rows() -> Result<()> {
    let (database, payload) = v2_search_payload("/v2/search?q=al&namespace=ens").await?;

    assert_eq!(payload["page"]["page_size"], json!(50));
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["meta"], json!({}));

    let data = payload["data"]
        .as_array()
        .expect("search data must be an array");
    assert_eq!(v2_search_names(data), vec!["alpha.eth", "alpine.eth"]);
    assert_eq!(data[0]["display_name"], json!("alpha.eth"));
    assert_eq!(data[0]["namespace"], json!("ens"));
    assert_eq!(
        data[0]["namehash"],
        json!(bigname_lookup::ens_namehash_hex("alpha.eth")?)
    );
    assert_eq!(
        data[0]["owner"],
        json!("0x00000000000000000000000000000000000000a1")
    );
    assert_eq!(
        data[0]["registrant"],
        json!("0x00000000000000000000000000000000000000a2")
    );
    assert_eq!(data[0]["registration_status"], json!("active"));
    assert_eq!(data[0]["registered_at"], json!("2024-01-02T00:00:00Z"));
    assert_eq!(data[0]["created_at"], json!("2023-01-02T00:00:00Z"));
    assert_eq!(data[0]["expires_at"], json!("2027-01-02T00:00:00Z"));
    assert!(data[0].get("relations").is_none());
    assert!(data[0].get("is_primary").is_none());
    assert!(data[0].get("role_summary").is_none());
    assert!(data[0].get("labelhash").is_none());
    assert!(data[0].get("subname_count").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_uses_dictionary_owner_and_registrant_precedence() -> Result<()> {
    let (database, payload) = v2_search_payload("/v2/search?q=precedence&namespace=ens").await?;

    let data = payload["data"]
        .as_array()
        .expect("search data must be an array");
    assert_eq!(v2_search_names(data), vec!["precedence.eth"]);
    assert_eq!(data[0]["owner"], json!(V2_SEARCH_CONTROL_OWNER));
    assert_eq!(
        data[0]["registrant"],
        json!(V2_SEARCH_REGISTRATION_REGISTRANT)
    );
    assert_eq!(data[0]["registration_status"], json!("active"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_match_modes_and_q_validation() -> Result<()> {
    let (database, prefix) = v2_search_payload("/v2/search?q=amm&namespace=ens").await?;
    assert_eq!(prefix["data"], json!([]));

    let contains = v2_search_payload_for_database(
        &database,
        "/v2/search?q=amm&match=contains&namespace=ens",
    )
    .await?;
    assert_eq!(
        v2_search_names(contains["data"].as_array().expect("contains data")),
        vec!["gamma.eth"]
    );

    for uri in [
        "/v2/search?namespace=ens",
        "/v2/search?q=&namespace=ens",
        "/v2/search?q=al&match=suffix&namespace=ens",
    ] {
        let response = v2_search_response_for_database(&database, uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let error: Value = read_json(response).await?;
        assert_eq!(error["error"]["code"], json!("invalid_input"), "{uri}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_lowercases_q_and_filters_namespace() -> Result<()> {
    let (database, uppercase) = v2_search_payload("/v2/search?q=AL&namespace=ens").await?;
    assert_eq!(
        v2_search_names(uppercase["data"].as_array().expect("uppercase data")),
        vec!["alpha.eth", "alpine.eth"]
    );

    let public = v2_search_payload_for_database(&database, "/v2/search?q=alpha").await?;
    assert_eq!(
        v2_search_names(public["data"].as_array().expect("public data")),
        vec!["alpha.base.eth", "alpha.eth"]
    );
    assert_eq!(public["meta"], json!({}));

    let basenames =
        v2_search_payload_for_database(&database, "/v2/search?q=alpha&namespace=basenames").await?;
    assert_eq!(
        v2_search_names(basenames["data"].as_array().expect("basenames data")),
        vec!["alpha.base.eth"]
    );

    let unknown =
        v2_search_response_for_database(&database, "/v2/search?q=alpha&namespace=internal").await?;
    assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(unknown).await?["error"]["code"],
        json!("invalid_input")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_escapes_like_metacharacters() -> Result<()> {
    // `_under.eth` and `bunder.eth` both match the unescaped LIKE pattern `_und%`.
    let (database, underscore) = v2_search_payload("/v2/search?q=_und&namespace=ens").await?;
    assert_eq!(
        v2_search_names(underscore["data"].as_array().expect("underscore data")),
        vec!["_under.eth"]
    );

    // `al%` matches every `al`-prefixed name only if the percent stays a wildcard. The percent is
    // exercised query-side only: the phase fixtures recompute identity through ENSIP-15, which
    // rejects a literal `%` label, so a stored `percent%name.eth` cannot be seeded here.
    let percent =
        v2_search_payload_for_database(&database, "/v2/search?q=al%25&namespace=ens").await?;
    assert_eq!(percent["data"], json!([]));

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_paginates_without_overlap_or_gap() -> Result<()> {
    let (database, first) = v2_search_payload("/v2/search?q=a&page_size=1").await?;
    assert_eq!(
        v2_search_names(first["data"].as_array().expect("first page data")),
        vec!["alpha.base.eth"]
    );
    assert_eq!(first["page"]["has_more"], json!(true));

    let mut page = first;
    let mut seen = vec!["alpha.base.eth".to_owned()];
    // Bounded so a cursor that stops advancing fails the assertion instead of looping forever.
    for _ in 0..8 {
        let Some(cursor) = page["page"]["next_cursor"].as_str().map(str::to_owned) else {
            break;
        };
        page = v2_search_payload_for_database(
            &database,
            &format!("/v2/search?q=a&page_size=1&cursor={cursor}"),
        )
        .await?;
        assert_eq!(page["page"]["cursor"], json!(cursor));
        let names = v2_search_names(page["data"].as_array().expect("page data"));
        assert_eq!(names.len(), 1);
        seen.push(names[0].to_owned());
    }

    assert_eq!(
        seen,
        vec![
            "alpha.base.eth",
            "alpha.eth",
            "alpine.base.eth",
            "alpine.eth"
        ]
    );
    assert_eq!(page["page"]["has_more"], json!(false));
    assert_eq!(page["page"]["next_cursor"], Value::Null);

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_rejects_cursor_anchor_changes() -> Result<()> {
    let (database, first) = v2_search_payload("/v2/search?q=al&namespace=ens&page_size=1").await?;
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a cursor")
        .to_owned();

    for uri in [
        format!("/v2/search?q=ga&namespace=ens&page_size=1&cursor={cursor}"),
        format!("/v2/search?q=al&match=contains&namespace=ens&page_size=1&cursor={cursor}"),
        format!("/v2/search?q=al&namespace=basenames&page_size=1&cursor={cursor}"),
        format!("/v2/search?q=al&page_size=1&cursor={cursor}"),
    ] {
        let response = v2_search_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(
            read_json::<Value>(response).await?["error"]["code"],
            json!("invalid_input"),
            "{uri}"
        );
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_rejects_snapshot_selectors_and_accepts_explicit_latest() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;

    for (uri, message) in [
        (
            "/v2/search?q=al&namespace=ens&at=2026-04-17T00:01:48Z",
            "at is not supported because collection routes read latest state",
        ),
        (
            "/v2/search?q=al&namespace=ens&finality=safe",
            "finality must be latest because collection routes read latest state",
        ),
        (
            "/v2/search?q=al&namespace=ens&finality=finalized",
            "finality must be latest because collection routes read latest state",
        ),
    ] {
        let response = v2_search_response_for_database(&database, uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        let error: Value = read_json(response).await?;
        assert_eq!(error["error"]["code"], json!("invalid_input"), "{uri}");
        assert_eq!(error["error"]["message"], json!(message), "{uri}");
    }

    let latest =
        v2_search_payload_for_database(&database, "/v2/search?q=al&namespace=ens&finality=latest")
            .await?;
    assert_eq!(latest["meta"], json!({}));

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_rejects_unknown_params_and_returns_empty_matches() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;

    let unknown =
        v2_search_response_for_database(&database, "/v2/search?q=al&namespace=ens&sort=name")
            .await?;
    assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json::<Value>(unknown).await?["error"]["code"],
        json!("invalid_input")
    );

    let empty = v2_search_payload_for_database(&database, "/v2/search?q=nomatch").await?;
    assert_eq!(empty["data"], json!([]));
    assert_eq!(empty["page"]["has_more"], json!(false));
    assert_eq!(empty["page"]["next_cursor"], Value::Null);

    database.cleanup().await
}

#[tokio::test]
async fn v2_search_omits_snapshot_metadata_across_namespace_scopes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;

    for uri in [
        "/v2/search?q=alpha",
        "/v2/search?q=alpha&namespace=ens",
        "/v2/search?q=alpha&namespace=basenames",
    ] {
        let payload = v2_search_payload_for_database(&database, uri).await?;
        assert!(payload["meta"].get("as_of").is_none(), "{uri}");
        assert!(payload["meta"].get("as_of_token").is_none(), "{uri}");
    }

    database.cleanup().await
}

async fn v2_search_payload(uri: &str) -> Result<(TestDatabase, Value)> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_search_fixture(&database).await?;
    let payload = v2_search_payload_for_database(&database, uri).await?;
    Ok((database, payload))
}

async fn v2_search_payload_for_database(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = v2_search_response_for_database(database, uri).await?;
    assert_eq!(response.status(), StatusCode::OK, "{uri}");
    read_json(response).await
}

async fn v2_search_response_for_database(database: &TestDatabase, uri: &str) -> Result<Response> {
    app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("search request must build"),
        )
        .await
        .context("v2 search request failed")
}

fn v2_search_names(rows: &[Value]) -> Vec<&str> {
    rows.iter()
        .map(|row| row["name"].as_str().expect("search row must include name"))
        .collect()
}

async fn seed_v2_search_fixture(database: &TestDatabase) -> Result<()> {
    let specs = v2_search_specs();

    let raw_blocks = specs
        .iter()
        .map(|spec| {
            raw_block(
                spec.chain_id(),
                &spec.block_hash(),
                None,
                spec.block_number,
                1_717_190_000 + spec.block_number,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &raw_blocks).await?;

    let surfaces = specs
        .iter()
        .map(|spec| {
            collection_name_surface(
                &spec.logical_name_id(),
                spec.name,
                &spec.namehash(),
                spec.block_number,
            )
        })
        .collect::<Vec<_>>();
    upsert_test_name_surfaces(&database.pool, &surfaces).await?;

    let token_lineages = specs
        .iter()
        .map(|spec| TokenLineage {
            token_lineage_id: spec.token_lineage_id(),
            chain_id: spec.chain_id().to_owned(),
            block_hash: spec.block_hash(),
            block_number: spec.block_number,
            provenance: json!({"seed": "v2_search"}),
            canonicality_state: CanonicalityState::Finalized,
        })
        .collect::<Vec<_>>();
    upsert_test_token_lineages(&database.pool, &token_lineages).await?;

    let resources = specs
        .iter()
        .map(|spec| Resource {
            resource_id: spec.resource_id(),
            token_lineage_id: Some(spec.token_lineage_id()),
            chain_id: spec.chain_id().to_owned(),
            block_hash: spec.block_hash(),
            block_number: spec.block_number,
            provenance: json!({"seed": "v2_search"}),
            canonicality_state: CanonicalityState::Finalized,
        })
        .collect::<Vec<_>>();
    upsert_test_resources(&database.pool, &resources).await?;

    let bindings = specs
        .iter()
        .map(|spec| SurfaceBinding {
            surface_binding_id: spec.surface_binding_id(),
            logical_name_id: spec.logical_name_id(),
            resource_id: spec.resource_id(),
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(1_717_190_000 + spec.block_number),
            active_to: None,
            chain_id: spec.chain_id().to_owned(),
            block_hash: spec.block_hash(),
            block_number: spec.block_number,
            provenance: json!({"seed": "v2_search"}),
            canonicality_state: CanonicalityState::Finalized,
        })
        .collect::<Vec<_>>();
    upsert_test_surface_bindings(&database.pool, &bindings).await?;

    for spec in &specs {
        database
            .insert_name_current_row(address_name_name_current_row(
                &spec.logical_name_id(),
                spec.name,
                spec.name,
                &spec.namehash(),
                spec.surface_binding_id(),
                spec.resource_id(),
                Some(spec.token_lineage_id()),
                spec.block_number,
                spec.declared_summary(),
            ))
            .await?;
    }

    Ok(())
}

#[derive(Default)]
struct V2SearchSpec {
    namespace: &'static str,
    name: &'static str,
    id: u128,
    block_number: i64,
    owner: &'static str,
    control_owner: Option<&'static str>,
    registrant: &'static str,
    control_registrant: Option<&'static str>,
    registered_at: &'static str,
    created_at: &'static str,
    expires_at: &'static str,
}

impl V2SearchSpec {
    fn logical_name_id(&self) -> String {
        format!("{}:{}", self.namespace, self.name)
    }

    fn namehash(&self) -> String {
        format!("node:{}", self.name)
    }

    fn chain_id(&self) -> &'static str {
        chain_id_for_namespace(self.namespace)
    }

    fn block_hash(&self) -> String {
        format!("0xsearch{:04x}", self.block_number)
    }

    fn resource_id(&self) -> Uuid {
        Uuid::from_u128(self.id)
    }

    fn token_lineage_id(&self) -> Uuid {
        Uuid::from_u128(self.id + 1)
    }

    fn surface_binding_id(&self) -> Uuid {
        Uuid::from_u128(self.id + 2)
    }

    fn declared_summary(&self) -> Value {
        let mut control = json!({ "registry_owner": self.owner, "expiry": self.expires_at });
        if let Some(control_owner) = self.control_owner {
            control["owner"] = json!(control_owner);
        }
        if let Some(control_registrant) = self.control_registrant {
            control["registrant"] = json!(control_registrant);
        }
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar",
                "registrant": self.registrant,
                "registered_at": self.registered_at,
                "created_at": self.created_at,
                "expiry": self.expires_at
            },
            "control": control
        })
    }
}

fn v2_search_specs() -> Vec<V2SearchSpec> {
    vec![
        V2SearchSpec {
            namespace: "ens",
            name: "alpha.eth",
            id: 0xa100,
            block_number: 201,
            owner: "0x00000000000000000000000000000000000000a1",
            registrant: "0x00000000000000000000000000000000000000a2",
            registered_at: "2024-01-02T00:00:00Z",
            created_at: "2023-01-02T00:00:00Z",
            expires_at: "2027-01-02T00:00:00Z",
            ..V2SearchSpec::default()
        },
        V2SearchSpec {
            namespace: "ens",
            name: "alpine.eth",
            id: 0xa200,
            block_number: 202,
            owner: "0x0000000000000000000000000000000000000a21",
            registrant: "0x0000000000000000000000000000000000000a22",
            registered_at: "2024-02-02T00:00:00Z",
            created_at: "2023-02-02T00:00:00Z",
            expires_at: "2027-02-02T00:00:00Z",
            ..V2SearchSpec::default()
        },
        V2SearchSpec {
            namespace: "ens",
            name: "gamma.eth",
            id: 0xa300,
            block_number: 203,
            owner: "0x0000000000000000000000000000000000000a31",
            registrant: "0x0000000000000000000000000000000000000a32",
            registered_at: "2024-03-02T00:00:00Z",
            created_at: "2023-03-02T00:00:00Z",
            expires_at: "2027-03-02T00:00:00Z",
            ..V2SearchSpec::default()
        },
        // `_under.eth` and `bunder.eth` both match the unescaped LIKE prefix `_und%`.
        V2SearchSpec {
            namespace: "ens",
            name: "_under.eth",
            id: 0xa400,
            block_number: 204,
            owner: "0x0000000000000000000000000000000000000a41",
            registrant: "0x0000000000000000000000000000000000000a42",
            registered_at: "2024-04-02T00:00:00Z",
            created_at: "2023-04-02T00:00:00Z",
            expires_at: "2027-04-02T00:00:00Z",
            ..V2SearchSpec::default()
        },
        V2SearchSpec {
            namespace: "ens",
            name: "bunder.eth",
            id: 0xa500,
            block_number: 205,
            owner: "0x0000000000000000000000000000000000000a51",
            registrant: "0x0000000000000000000000000000000000000a52",
            registered_at: "2024-05-02T00:00:00Z",
            created_at: "2023-05-02T00:00:00Z",
            expires_at: "2027-05-02T00:00:00Z",
            ..V2SearchSpec::default()
        },
        V2SearchSpec {
            namespace: "ens",
            name: "precedence.eth",
            id: 0xd100,
            block_number: 206,
            owner: V2_SEARCH_REGISTRY_OWNER,
            control_owner: Some(V2_SEARCH_CONTROL_OWNER),
            registrant: V2_SEARCH_REGISTRATION_REGISTRANT,
            control_registrant: Some(V2_SEARCH_CONTROL_REGISTRANT),
            registered_at: "2024-07-03T00:00:00Z",
            created_at: "2023-07-03T00:00:00Z",
            expires_at: "2027-07-03T00:00:00Z",
        },
        V2SearchSpec {
            namespace: "basenames",
            name: "alpha.base.eth",
            id: 0xb100,
            block_number: 207,
            owner: "0x0000000000000000000000000000000000000b11",
            registrant: "0x0000000000000000000000000000000000000b12",
            registered_at: "2024-08-02T00:00:00Z",
            created_at: "2023-08-02T00:00:00Z",
            expires_at: "2027-08-02T00:00:00Z",
            ..V2SearchSpec::default()
        },
        V2SearchSpec {
            namespace: "basenames",
            name: "alpine.base.eth",
            id: 0xb200,
            block_number: 208,
            owner: "0x0000000000000000000000000000000000000b21",
            registrant: "0x0000000000000000000000000000000000000b22",
            registered_at: "2024-08-03T00:00:00Z",
            created_at: "2023-08-03T00:00:00Z",
            expires_at: "2027-08-03T00:00:00Z",
            ..V2SearchSpec::default()
        },
        // Not a public namespace: the default namespace set must exclude it.
        V2SearchSpec {
            namespace: "internal",
            name: "alpha.internal",
            id: 0xc100,
            block_number: 209,
            owner: "0x0000000000000000000000000000000000000c11",
            registrant: "0x0000000000000000000000000000000000000c12",
            registered_at: "2024-09-02T00:00:00Z",
            created_at: "2023-09-02T00:00:00Z",
            expires_at: "2027-09-02T00:00:00Z",
            ..V2SearchSpec::default()
        },
    ]
}
