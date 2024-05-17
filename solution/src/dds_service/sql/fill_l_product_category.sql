INSERT INTO dds.l_product_category(hk_product_category_pk, h_product_pk,
	                           h_category_pk, load_dt, load_src)
SELECT gen_random_uuid() AS hk_product_category_pk
     , hp.h_product_pk
     , hc.h_category_pk
     , now() AS load_dt
     , %(load_src)s AS load_src
  FROM (SELECT DISTINCT s.prod->>'id' AS product_id
  	     , s.prod->>'category' AS category 
          FROM (SELECT jsonb_array_elements(s.payload->'payload'->'order_items') AS prod
                  FROM stg.order_events s
                 WHERE s.object_id = ANY(%(object_ids)s)
                   AND s.object_type = %(object_type)s
               ) s
       ) s
  JOIN dds.h_category hc
    ON hc.category_name = s.category
  JOIN dds.h_product hp
    ON hp.product_id = s.product_id
    ON CONFLICT (h_product_pk, h_category_pk) DO NOTHING;
