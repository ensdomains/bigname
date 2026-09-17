INSERT INTO project_events
SELECT 1000000+i,CASE WHEN i%10<7 THEN NULL ELSE 'unrelated:'||i END,
       md5(('unrelated-resource:'||i)::text)::uuid,
       CASE WHEN i%10<2 THEN 'ResolverChanged' ELSE 'Other' END,
       CASE WHEN i%100=0 THEN 'ens_v2_root_l1' ELSE 'ens_v2_registry_l1' END,
       'ethereum-sepolia',i,0,0,'unrelated-event:'||i,'{"resolver":"0x123"}'::jsonb
FROM generate_series($1::bigint,$2::bigint) i;
