INSERT INTO dds.l_order_product(hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
SELECT gen_random_uuid() AS hk_order_product_pk
     , ho.h_order_pk
     , hp.h_product_pk
     , now() AS load_dt
     , %(load_src)s AS load_src
  FROM (SELECT DISTINCT s.order_id 
     	     , s.prod->>'id' AS product_id
  	  FROM (SELECT s.order_id
  	       	     , jsonb_array_elements(s.prod_arr) AS prod
                  FROM (SELECT (s.payload->>'id')::int AS order_id
          	       	     , s.payload->'products' AS prod_arr
                          FROM stg.order_events s
                 	 WHERE s.object_id = ANY(%(object_ids)s)
                   	   AND s.object_type = %(object_type)s
               	       ) s
               ) s
       ) s
  JOIN dds.h_order ho
    ON ho.order_id = s.order_id
  JOIN dds.h_product hp
    ON hp.product_id = s.product_id
    ON CONFLICT(h_order_pk, h_product_pk) DO NOTHING;
