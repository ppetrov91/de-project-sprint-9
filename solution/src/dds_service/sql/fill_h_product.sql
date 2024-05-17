INSERT INTO dds.h_product(h_product_pk, product_id, load_dt, load_src)
SELECT gen_random_uuid() AS h_product_pk
     , s.product_id
     , now() AS load_dt
     , %(load_src)s AS load_src
  FROM (SELECT DISTINCT s.prod->>'id' AS product_id
          FROM (SELECT jsonb_array_elements(s.payload->'products') AS prod
                  FROM stg.order_events s
                 WHERE s.object_id = ANY(%(object_ids)s)
                   AND s.object_type = %(object_type)s
               ) s
       ) s
    ON CONFLICT (product_id) DO NOTHING;
