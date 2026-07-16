use super::*;

fn test_transfer_request(index: usize) -> TransferRequest {
    TransferRequest {
        event_index: index,
        chain_id: "ethereum-sepolia".to_owned(),
        namespace: "ens".to_owned(),
        source_family: "ens_v2_registry_l1".to_owned(),
        source_manifest_id: 7,
        manifest_version: 1,
        registry_contract_instance_id: Uuid::from_u128(9),
        registry_address: "0x0000000000000000000000000000000000000001".to_owned(),
        token_id: format!("0x{:064x}", index + 1),
        block_number: 12,
        block_hash: format!("0x{:064x}", 12),
        transaction_index: 1,
        log_index: 2,
    }
}

fn test_target_request(index: usize) -> TargetRequest {
    TargetRequest {
        event_index: i64::try_from(index).expect("test request index must fit i64"),
        chain_id: "ethereum-sepolia".to_owned(),
        from_contract_instance_id: Uuid::from_u128(1),
        target_address: "0x0000000000000000000000000000000000000002".to_owned(),
        block_number: 12,
        block_hash: format!("0x{:064x}", index + 1),
    }
}

#[test]
fn subregistry_hydration_chunks_queries_below_the_postgres_bind_limit() {
    let former_single_query_limit = POSTGRES_BIND_PARAMETER_LIMIT / SUBREGISTRY_REQUEST_BIND_COUNT;
    let requests = (0..=former_single_query_limit)
        .map(test_target_request)
        .collect::<Vec<_>>();
    let chunks = requests
        .chunks(SUBREGISTRY_REQUEST_CHUNK_SIZE)
        .collect::<Vec<_>>();

    assert_eq!(requests.len(), former_single_query_limit + 1);
    assert_eq!(
        chunks.iter().map(|chunk| chunk.len()).collect::<Vec<_>>(),
        vec![10_000, 923]
    );
    for chunk in chunks {
        let query = build_subregistry_target_query(chunk);
        let bind_count = subregistry_query_bind_count(chunk.len());
        assert!(bind_count < POSTGRES_BIND_PARAMETER_LIMIT);
        assert!(query.sql().contains(&format!("${bind_count}")));
        assert!(!query.sql().contains(&format!("${}", bind_count + 1)));
    }
}

#[test]
fn registry_suffix_lookups_deduplicate_the_same_registry_position() {
    let requests = (0..20_000).map(test_transfer_request).collect::<Vec<_>>();
    let keyed = requests
        .iter()
        .map(|request| (TransferHydrationKey::from(request), request))
        .collect::<Vec<_>>();
    let positions = unique_registry_suffix_positions(&keyed);
    let authorities = unique_registry_authorities(&positions);

    assert_eq!(keyed.len(), 20_000);
    assert_eq!(positions.len(), 1);
    assert_eq!(authorities.len(), 1);
}

#[test]
fn registry_suffix_history_queries_chunk_below_the_postgres_bind_limit() {
    let former_single_query_limit =
        POSTGRES_BIND_PARAMETER_LIMIT / REGISTRY_SUFFIX_REQUEST_BIND_COUNT;
    let requests = (0..=former_single_query_limit)
        .map(|index| {
            let mut request = test_transfer_request(index);
            request.block_hash = format!("0x{:064x}", index + 1);
            request
        })
        .collect::<Vec<_>>();
    let keyed = requests
        .iter()
        .map(|request| (TransferHydrationKey::from(request), request))
        .collect::<Vec<_>>();
    let positions = unique_registry_suffix_positions(&keyed);
    let chunks = positions
        .chunks(REGISTRY_SUFFIX_REQUEST_CHUNK_SIZE)
        .collect::<Vec<_>>();

    assert_eq!(positions.len(), former_single_query_limit + 1);
    assert_eq!(
        chunks.iter().map(|chunk| chunk.len()).collect::<Vec<_>>(),
        vec![12_000, 1_108]
    );
    for chunk in chunks {
        let query = build_registry_suffix_history_query(chunk);
        let bind_count = registry_suffix_query_bind_count(chunk.len());
        assert!(bind_count < POSTGRES_BIND_PARAMETER_LIMIT);
        assert!(query.sql().contains(&format!("${bind_count}")));
        assert!(!query.sql().contains(&format!("${}", bind_count + 1)));
    }
}
