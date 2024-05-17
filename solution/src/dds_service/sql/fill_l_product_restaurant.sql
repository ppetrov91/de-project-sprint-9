INSERT INTO dds.l_product_restaurant(hk_product_restaurant_pk, h_product_pk, 
	                             h_restaurant_pk, load_dt, load_src)
SELECT gen_random_uuid() AS hk_product_restaurant_pk
     , hp.h_product_pk
     , hr.h_restaurant_pk
     , now() AS load_dt
     , %(load_src)s AS load_src
  FROM (SELECT DISTINCT s.restaurant_id 
     	     , s.prod->>'id' AS product_id
          FROM (SELECT s.payload->'payload'->'restaurant'->>'id' AS restaurant_id
          	     , jsonb_array_elements(s.payload->'payload'->'products') AS prod
                  FROM stg.order_events s
                 WHERE s.object_id = ANY(%(object_ids)s)
                   AND s.object_type = %(object_type)s
               ) s
       ) s
  JOIN dds.h_restaurant hr
    ON hr.restaurant_id = s.restaurant_id
  JOIN dds.h_product hp
    ON hp.product_id = s.product_id
    ON CONFLICT (h_product_pk, h_restaurant_pk) DO NOTHING;
