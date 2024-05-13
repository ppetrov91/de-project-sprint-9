INSERT INTO dds.h_order(h_order_pk, order_id, order_dt, load_dt, load_src)
SELECT gen_random_uuid() AS h_order_pk
     , (s.payload->>'id')::int AS order_id
     , (s.payload->>'date')::timestamp AS order_dt
     , now() AS load_dt
     , %s AS load_src
  FROM stg.order_events s
 WHERE s.object_id = ANY(%s)
   AND s.object_type = %s
    ON CONFLICT (order_id) DO NOTHING;
