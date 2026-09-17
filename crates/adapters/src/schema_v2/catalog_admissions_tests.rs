use super::*;

fn admission(n: usize) -> AddressAdmissionInput {
    AddressAdmissionInput {
        address: format!("0x{:040x}", n % 29),
        contract_instance_id: Uuid::from_u128((n % 31) as u128),
        source_manifest_id: Some((n % 7) as i64),
        role: n.is_multiple_of(3).then(|| format!("role-{}", n % 5)),
        discovery_edge_kind: (!n.is_multiple_of(7)).then(|| format!("edge-{}", n % 2)),
        discovery_from_contract_instance_id: (!n.is_multiple_of(11))
            .then(|| Uuid::from_u128((n % 3) as u128)),
        discovery_observation_key: (!n.is_multiple_of(13))
            .then(|| format!("observation-{}", n % 17)),
        active_from_block: Some(n as i64),
        active_to_block: n.is_multiple_of(2).then_some(n as i64 + 9),
    }
}

fn flat_retire(rows: &mut Vec<AddressAdmissionInput>, edge: &str, from: Uuid, key: &str) {
    rows.retain(|row| {
        row.discovery_edge_kind.as_deref() != Some(edge)
            || row.discovery_from_contract_instance_id != Some(from)
            || row.discovery_observation_key.as_deref() != Some(key)
    });
}

fn compare(index: &Admissions, rows: &[AddressAdmissionInput]) {
    assert_eq!(
        index.iter().collect::<Vec<_>>(),
        rows.iter().collect::<Vec<_>>()
    );
    for n in 0..32 {
        let address = format!("0X{n:040X}");
        assert_eq!(
            index.for_address(&address).collect::<Vec<_>>(),
            rows.iter()
                .filter(|row| row.address.eq_ignore_ascii_case(&address))
                .collect::<Vec<_>>()
        );
        let instance = Uuid::from_u128(n);
        assert_eq!(
            index.for_instance(instance).collect::<Vec<_>>(),
            rows.iter()
                .filter(|row| row.contract_instance_id == instance)
                .collect::<Vec<_>>()
        );
    }
    for n in 0..6 {
        let role = format!("role-{n}");
        assert_eq!(
            index.for_role(&role).collect::<Vec<_>>(),
            rows.iter()
                .filter(|row| row.role.as_deref() == Some(&role))
                .collect::<Vec<_>>()
        );
    }
    for ids in index.by_observation.values() {
        assert!(!ids.is_empty());
        assert!(ids.iter().all(|id| index.entries.contains_key(id)));
    }
}

#[test]
fn indexed_admissions_match_flat_history_through_replacement_and_retirement() {
    let mut rows = (0..400).map(admission).collect::<Vec<_>>();
    // Preserve duplicate historical ranges, casing, and insertion order on load.
    rows.push(rows[25].clone());
    rows[10].address = rows[10].address.to_ascii_uppercase();
    let mut index = Admissions::new(rows.clone());
    compare(&index, &rows);
    for n in 400..650 {
        let row = admission(n);
        if let Some((edge, from, key)) = retirement_key(&row) {
            flat_retire(&mut rows, &edge, from, &key);
            index.retire(&edge, from, &key);
        }
        rows.push(row.clone());
        index.push(row);
        if n.is_multiple_of(3) {
            let edge = format!("edge-{}", n % 2);
            let from = Uuid::from_u128((n % 3) as u128);
            let key = format!("observation-{}", n % 17);
            flat_retire(&mut rows, &edge, from, &key);
            index.retire(&edge, from, &key);
        }
        compare(&index, &rows);
    }
}

#[test]
fn retirement_releases_all_secondary_index_entries() {
    let mut index = Admissions::default();
    for n in 0..1_000 {
        let mut row = admission(1);
        row.address = format!("0x{n:040x}");
        row.role = Some(format!("role-{n}"));
        row.contract_instance_id = Uuid::from_u128(n);
        index.push(row);
    }
    let (edge, from, key) = retirement_key(&admission(1)).unwrap();
    index.retire(&edge, from, &key);
    assert!(index.entries.is_empty());
    assert!(index.by_address.is_empty());
    assert!(index.by_instance.is_empty());
    assert!(index.by_role.is_empty());
    assert!(index.by_observation.is_empty());
    index.push(admission(1));
    compare(&index, &[admission(1)]);
}

#[test]
#[ignore = "manual release-mode scaling benchmark"]
fn admission_lookup_scaling() {
    use std::{hint::black_box, time::Instant};
    for size in [100_000, 1_000_000] {
        let rows = (0..size)
            .map(|n| {
                let mut row = admission(n);
                row.address = format!("0x{n:040x}");
                row.contract_instance_id = Uuid::from_u128(n as u128);
                row.discovery_observation_key = Some(format!("observation-{n}"));
                row
            })
            .collect::<Vec<_>>();
        let started = Instant::now();
        let mut index = Admissions::new(rows.clone());
        let build_ms = started.elapsed().as_millis();
        let address = rows[size - 1].address.to_ascii_uppercase();
        let started = Instant::now();
        for _ in 0..100 {
            black_box(
                rows.iter()
                    .filter(|row| row.address.eq_ignore_ascii_case(black_box(&address)))
                    .count(),
            );
        }
        let flat_us = started.elapsed().as_micros();
        let started = Instant::now();
        for _ in 0..100 {
            assert_eq!(black_box(index.for_address(black_box(&address)).count()), 1);
        }
        let indexed_us = started.elapsed().as_micros();
        let row = &rows[size - 2];
        let (edge, from, key) = retirement_key(row).unwrap();
        let started = Instant::now();
        for _ in 0..1_000 {
            index.retire(&edge, from, &key);
            index.push(row.clone());
        }
        eprintln!(
            "admissions={size} build_ms={build_ms} lookups=100 flat_us={flat_us} indexed_us={indexed_us} replacements=1000 indexed_replacement_us={}",
            started.elapsed().as_micros()
        );
    }
}
