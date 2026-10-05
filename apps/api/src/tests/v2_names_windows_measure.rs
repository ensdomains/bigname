// Manual, serialized measurement. One family-event-published 20k fixture, the public router,
// and actual loopback HTTP. Run this ignored test alone.
#[derive(Clone)]
struct WindowMeasuredName { name: String, expiry: i64, authority: &'static str, namehash: String }

async fn seed_window_measurement(database: &TestDatabase) -> Result<Vec<WindowMeasuredName>> {
    seed_bounded_membership_blocks(database, 240).await?;
    let mut fixture = OracleFixture::new();
    let mut names = Vec::new();
    for chunk in 0..40 {
        let mut rows = Vec::new();
        for index in chunk * 500..(chunk + 1) * 500 {
            let name = if index % 97 == 0 { format!("w{index:05}.branch.eth") }
                else { format!("w{index:05}.eth") };
            let (logical, namehash) = phase_logical_identity("ens", &name)?;
            let seed = 0x7900_0000 + index as u128 * 16;
            let resource = Uuid::from_u128(seed);
            let band = if index < 2000 { 0 } else { 1 + (index - 2000) % 64 };
            let expiry = 1_850_000_000 + band as i64 * 1000;
            let authority = ["ens_v2", "ens_v0", "ens_v1"][index % 3];
            let arm = if authority == "ens_v2" { "ens_v2" } else { "ens_v1" };
            let labels: Vec<_> = name.split('.').collect();
            let hashes: Vec<_> = labels.iter().map(|label| format!("{:#x}", alloy_primitives::keccak256(label.as_bytes()))).collect();
            rows.push(json!({"logical":logical,"name":name,"namehash":namehash,
                "labels":labels,"labelhashes":hashes,"resource":resource,
                "token":Uuid::from_u128(seed+1),"binding":Uuid::from_u128(seed+2),"arm":arm}));
            let registrar = json!({"authority_kind":"registrar","registrant":ORACLE_HOLDER,"expiry":expiry});
            let grant = if authority == "ens_v2" {
                fixture.event(&logical, resource, "RegistrationGranted", "ens_v2_registry_l1",
                    json!({"source_event":"NameRegistered","authority_kind":"ens_v2_registry",
                        "owner":ORACLE_HOLDER,"registrant":ORACLE_HOLDER,"expiry":expiry}))
            } else { fixture.event(&logical, resource, "RegistrationGranted", "ens_v1_registrar_l1", registrar) };
            fixture.events.push(grant);
            if authority == "ens_v2" {
                let event = fixture.event(&logical, resource, "TokenControlTransferred", "ens_v2_registry_l1",
                    json!({"source_event":"Transfer","from":ORACLE_ZERO,"to":ORACLE_HOLDER}));
                fixture.events.push(event);
            } else if authority == "ens_v0" {
                let event = fixture.event(&logical, resource, "AuthorityTransferred", "ens_v1_registry_l1",
                    json!({"source_event":"Transfer","node":namehash,"owner":ORACLE_HOLDER,
                        "owner_getter":ORACLE_HOLDER,"emitter_role":"registry_old","registry_contract":ORACLE_OLD_REGISTRY}));
                fixture.events.push(event);
            }
            names.push(WindowMeasuredName { name, expiry, authority, namehash });
        }
        let rows = Value::Array(rows);
        for sql in [
            "INSERT INTO token_lineages (token_lineage_id,chain_id,block_hash,block_number,provenance,canonicality_state)
             SELECT token,'ethereum-mainnet','0xhistory200',200,'{}','canonical' FROM jsonb_to_recordset($1) AS x(token uuid)",
            "INSERT INTO resources (resource_id,token_lineage_id,chain_id,block_hash,block_number,provenance,canonicality_state)
             SELECT resource,token,'ethereum-mainnet','0xhistory200',200,'{}','canonical' FROM jsonb_to_recordset($1) AS x(resource uuid,token uuid)",
            "INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,
                normalizer_version,visibility_state,normalization_errors,chain_id,block_hash,block_number,provenance,canonicality_state)
             SELECT logical,'ens',name,labels,convert_to(name,'UTF8'),namehash,labelhashes,'fixture','active','[]',
                'ethereum-mainnet','0xhistory200',200,'{}','canonical'
             FROM jsonb_to_recordset($1) AS x(logical text,name text,labels text[],namehash text,labelhashes text[])",
            "INSERT INTO surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,
                active_from,chain_id,block_hash,block_number,provenance,canonicality_state)
             SELECT binding,logical,resource,'declared_registry_path',arm,to_timestamp(1700000200),
                'ethereum-mainnet','0xhistory200',200,'{}','canonical'
             FROM jsonb_to_recordset($1) AS x(binding uuid,logical text,resource uuid,arm text)",
        ] { sqlx::query(sql).bind(&rows).execute(&database.pool).await?; }
        // Keep publication work representative: 500 names (833 or 834 events) per block,
        // with the same expiry/authority/parent distribution as the original single-block run.
        let block = 201 + chunk as i64;
        for event in &mut fixture.events {
            event.block_number = Some(block);
            event.block_hash = Some(format!("0xhistory{block}"));
            event.transaction_hash = Some(format!("0xtx{block}"));
        }
        fixture.insert(database).await?;
    }
    publish_test_families(database, 240).await?;
    sqlx::raw_sql("ANALYZE").execute(&database.pool).await?;
    let summaries: i64 = sqlx::query_scalar("SELECT count(*) FROM project_name_summary WHERE expiry_listable")
        .fetch_one(&database.pool).await?;
    anyhow::ensure!(summaries == 20_000, "expected 20k listable family summaries, got {summaries}");
    Ok(names)
}

fn window_measure_bounds(count: usize) -> Vec<String> {
    (1..=count).rev().map(|band| {
        let after = 1_850_000_000 + band * 2000;
        format!("{after}..{}", after+1)
    }).collect()
}

fn window_measure_expected<'a>(names: &'a [WindowMeasuredName], windows: &[String],
    order: &str, extra: &str) -> Vec<(&'a WindowMeasuredName, usize)> {
    let mut expected: Vec<_> = names.iter().filter_map(|name| {
        if extra.contains("parent=eth") && name.name.matches('.').count() != 1 { return None; }
        if extra.contains("parent=branch.eth") && !name.name.ends_with(".branch.eth") { return None; }
        if extra.contains("authority=ens_v1") && name.authority != "ens_v1" { return None; }
        windows.iter().position(|window| {
            let (after,before) = window.split_once("..").unwrap();
            name.expiry >= after.parse::<i64>().unwrap() && name.expiry < before.parse::<i64>().unwrap()
        }).map(|index| (name,index))
    }).collect();
    expected.sort_by(|(a,_),(b,_)| {
        let expiry = if order == "desc" { b.expiry.cmp(&a.expiry) } else { a.expiry.cmp(&b.expiry) };
        expiry.then_with(|| a.name.cmp(&b.name)).then_with(|| a.namehash.cmp(&b.namehash))
    });
    expected
}

async fn window_curl(base: &str, uri: &str) -> Result<(Value, f64)> {
    let url = format!("{base}{uri}");
    let output = tokio::task::spawn_blocking(move || std::process::Command::new("curl")
        .args(["--silent","--show-error","--fail-with-body","--write-out","\n%{time_total}",&url]).output()).await??;
    anyhow::ensure!(output.status.success(), "curl: {} {}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
    let output = String::from_utf8(output.stdout)?;
    let (body, elapsed) = output.rsplit_once('\n').context("curl time")?;
    Ok((serde_json::from_str(body)?,elapsed.parse::<f64>()? * 1000.0))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "serialized 20k expiry-window HTTP/plan measurement"]
async fn v2_names_windows_measure_20k() -> Result<()> {
    use std::sync::{Arc, atomic::{AtomicBool,AtomicU64,Ordering}};
    use bigname_storage::families::name::seams;
    use sqlx::Connection;
    let output = std::env::var("BIGNAME_WINDOWS_MEASURE_OUTPUT").context("measurement output directory required")?;
    std::fs::create_dir_all(&output)?;
    // A published disposable fixture can be retained while the timing harness is rebuilt.
    // Reuse never writes its rows or publication marker and leaves cleanup to its owner.
    let database = if std::env::var("BIGNAME_WINDOWS_REUSE_DATABASE").is_ok() { None }
        else { Some(TestDatabase::new_migrated().await?) };
    let (pool,names) = if let Some(database) = &database {
        eprintln!("window fixture database {}",database.database_name);
        let started=std::time::Instant::now();
        let names=seed_window_measurement(database).await?;
        eprintln!("published 20k names in {:?}",started.elapsed());
        (database.pool.clone(),names)
    } else {
        let reuse=std::env::var("BIGNAME_WINDOWS_REUSE_DATABASE")?;
        anyhow::ensure!(reuse.starts_with("bigname_api_test_"),"only disposable test fixtures may be reused");
        let options=PgConnectOptions::from_str(&std::env::var("BIGNAME_DATABASE_URL")?)?
            .database(&reuse).options([("search_path","bigname_phase".to_owned())]);
        let pool=PgPoolOptions::new().max_connections(6).connect_with(options).await?;
        let names=(0..20_000).map(|index| {
            let name=if index%97==0 {format!("w{index:05}.branch.eth")}else{format!("w{index:05}.eth")};
            let (_,namehash)=phase_logical_identity("ens",&name)?;
            let band=if index<2000{0}else{1+(index-2000)%64};
            Ok(WindowMeasuredName{name,expiry:1_850_000_000+band*1000,
                authority:["ens_v2","ens_v0","ens_v1"][index as usize%3],namehash})
        }).collect::<Result<Vec<_>>>()?;
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM project_name_summary WHERE expiry_listable").fetch_one(&pool).await?;
        anyhow::ensure!(count==20_000,"reused fixture must hold20k listable names");
        eprintln!("reusing published fixture {reuse}");
        (pool,names)
    };
    let settings: Value = sqlx::query_scalar("SELECT jsonb_build_object('version',version(),
        'jit',current_setting('jit'),'shared_buffers',current_setting('shared_buffers'),
        'work_mem',current_setting('work_mem'),'plan_cache_mode',current_setting('plan_cache_mode'),
        'max_parallel_workers_per_gather',current_setting('max_parallel_workers_per_gather'),
        'database',current_database(),'collation',(SELECT datcollate FROM pg_database WHERE datname=current_database()))")
        .fetch_one(&pool).await?;
    std::fs::write(format!("{output}/postgres.json"),serde_json::to_vec_pretty(&settings)?)?;
    if let Ok(gate)=std::env::var("BIGNAME_WINDOWS_MEASURE_GATE") {
        eprintln!("fixture ready; timing waits for {gate}");
        while !std::path::Path::new(&gate).exists() { tokio::time::sleep(std::time::Duration::from_secs(1)).await; }
    }

    let counters = [Arc::new(AtomicU64::new(0)),Arc::new(AtomicU64::new(0)),Arc::new(AtomicU64::new(0))];
    let counts = counters.clone();
    // Use production middleware without the test router's JSON-schema validation overhead.
    let app = crate::app_router_with_bounds(AppState::new_with_rpc_urls(pool.clone(),
        bigname_lookup::ChainRpcUrls::default()).with_public_namespaces_for_test(["ens","basenames"]),
        pool.clone(), &ApiBoundsConfig::default()).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request,next: axum::middleware::Next| {
            let counts = counts.clone();
            async move { seams::with_composed_names_counter(counts[0].clone(),
                seams::with_peak_source_counter(counts[1].clone(),
                    seams::with_submitted_rows_counter(counts[2].clone(),next.run(request)))).await }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}",listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener,app).await });
    let mut records = Vec::new();
    let cases = [
        ("one",window_measure_bounds(1),"asc","",0),
        ("seven",window_measure_bounds(7),"asc","",0),
        ("thirty_two",window_measure_bounds(32),"asc","",0),
        ("descending",window_measure_bounds(32),"desc","",0),
        ("authority_broad_parent",window_measure_bounds(32),"asc","&authority=ens_v1&parent=eth",0),
        ("narrow_underfilled",window_measure_bounds(32),"asc","&parent=branch.eth",0),
        ("deep",window_measure_bounds(32),"asc","",20),
        ("tied_deep",vec!["1850000000..1850000001".to_owned()],"desc","",7),
    ];
    for (label,windows,order,extra,skip_pages) in cases {
        let expected = window_measure_expected(&names,&windows,order,extra);
        let uri = names_windows_uri(&windows,order,200,extra);
        let mut request = uri.clone();
        for page in 0..skip_pages {
            let (body,_) = window_curl(&base,&request).await?;
            let actual = body["data"].as_array().context("rows")?;
            for (row,(name,index)) in actual.iter().zip(&expected[page*200..]) {
                assert_eq!(row["name"],json!(name.name)); assert_eq!(row["expires_window_index"],json!(index));
            }
            request = format!("{uri}&cursor={}",body["page"]["next_cursor"].as_str().context("deep cursor")?);
        }
        let offset = skip_pages*200;
        let end = expected.len().min(offset+200);
        for repeat in 0..3 {
            for counter in &counters { counter.store(0,Ordering::Relaxed); }
            let before = resident_kib();
            let peak = Arc::new(AtomicU64::new(before));
            let done = Arc::new(AtomicBool::new(false));
            let (p,d) = (peak.clone(),done.clone());
            let sampler = std::thread::spawn(move || { while !d.load(Ordering::Relaxed) {
                p.fetch_max(resident_kib(),Ordering::Relaxed); std::thread::sleep(std::time::Duration::from_millis(20));
            }});
            let measured = window_curl(&base,&request).await;
            done.store(true,Ordering::Relaxed); sampler.join().unwrap();
            let (body,elapsed_ms) = measured?;
            let rows = body["data"].as_array().context("rows")?;
            assert_eq!(rows.len(),end-offset,"{label}");
            for (row,(name,index)) in rows.iter().zip(&expected[offset..end]) {
                assert_eq!(row["name"],json!(name.name),"{label}"); assert_eq!(row["expires_window_index"],json!(index));
            }
            let more = end < expected.len();
            assert_eq!(body["page"]["has_more"],json!(more));
            assert_eq!(body["page"]["next_cursor"].is_string(),more);
            let counts: Vec<_> = counters.iter().map(|c|c.load(Ordering::Relaxed)).collect();
            assert_eq!(counts,vec![rows.len() as u64+u64::from(more);3],"{label}");
            let record = json!({"case":label,"repeat":repeat,"windows":windows.len(),"offset":offset,
                "rows":rows.len(),"has_more":more,"http_ms":elapsed_ms,"composed_peak_submitted":counts,
                "rss_before_kib":before,"rss_peak_kib":peak.load(Ordering::Relaxed)});
            println!("{record}"); records.push(record);
        // Scalar calls carry their own public cursors. For the first page, assemble their
        // exact disjoint groups and truncate globally; compare the entire API row payload.
        if skip_pages == 0 {
            let mut scalar_groups = Vec::new(); let mut scalar_ms = 0.0; let mut scalar_composed = 0;
            for (index,window) in windows.iter().enumerate() {
                let (after,before) = window.split_once("..").unwrap();
                counters[0].store(0,Ordering::Relaxed);
                let scalar_uri = format!("/v1/names?namespace=ens&expires_after={after}&expires_before={before}&order={order}&page_size=200{extra}");
                let (body,ms) = window_curl(&base,&scalar_uri).await?;
                scalar_ms += ms; scalar_composed += counters[0].load(Ordering::Relaxed);
                let mut rows = body["data"].as_array().unwrap().clone();
                for row in &mut rows { assert!(row.get("expires_window_index").is_none());row["expires_window_index"]=json!(index); }
                scalar_groups.push((after.parse::<i64>()?,rows));
            }
            scalar_groups.sort_by_key(|(after,_)|if order=="desc" {-*after} else {*after});
            let scalar: Vec<_> = scalar_groups.into_iter().flat_map(|(_,rows)|rows).take(200).collect();
            assert_eq!(*body["data"].as_array().unwrap(),scalar,"{label}");
            let record=json!({"case":label,"repeat":repeat,"paired_union_http_ms":elapsed_ms,
                "scalar_http_sum_ms":scalar_ms,"scalar_calls":windows.len(),"scalar_composed":scalar_composed});
            println!("{record}"); records.push(record);
        }
        }
        let filter = bigname_storage::NameCurrentExpiringFilter {
            namespace:"ens".to_owned(),windows: windows.iter().map(|window| {
                let (a,b)=window.split_once("..").unwrap(); Ok(bigname_storage::NameCurrentExpiryWindow {
                    expires_after:Some(a.parse()?),expires_before:Some(b.parse()?)})
            }).collect::<Result<_>>()?,
            authorities:extra.contains("authority=").then(||vec!["ens_v1".to_owned()]),
            parent:if extra.contains("parent=branch.eth"){Some("branch.eth".to_owned())}
                else if extra.contains("parent=eth"){Some("eth".to_owned())} else {None},
        };
        let cursor=offset.checked_sub(1).map(|index| {let name=expected[index].0; bigname_storage::NameCurrentListCursor {
            sort_value:bigname_storage::NameCurrentListCursorValue::Timestamp(WindowSeconds::from_seconds(name.expiry as i128)),
            namespace:"ens".to_owned(),normalized_name:name.name.clone(),namehash:name.namehash.clone() }});
        let sort=if order=="desc"{bigname_storage::NameCurrentListOrder::Desc}else{bigname_storage::NameCurrentListOrder::Asc};
        // Keep forced plan-cache modes off the HTTP serving pool.
        let mut conn=sqlx::PgConnection::connect_with(&pool.connect_options()).await?;
        for mode in ["auto","force_custom_plan","force_generic_plan"] {
            let plan=seams::explain_expiring_selection(&mut conn,&filter,sort,cursor.as_ref(),200,mode).await?;
            assert_eq!(plan["plan"][0]["Plan"]["Actual Rows"],json!((expected.len()-offset).min(201)));
            if mode=="force_generic_plan" {assert_eq!(plan["generic_plans"],json!(1));}
            std::fs::write(format!("{output}/{label}-{mode}.json"),serde_json::to_vec_pretty(&plan)?)?;
        }
        conn.close().await?;
    }
    std::fs::write(format!("{output}/http.json"),serde_json::to_vec_pretty(&records)?)?;
    server.abort();
    if let Some(database)=database { database.cleanup().await?; } else { pool.close().await; }
    Ok(())
}
