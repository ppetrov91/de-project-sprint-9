WITH ds AS (
SELECT hp.h_product_pk
     , s.productname
  FROM (SELECT DISTINCT s.prod->>'id' AS product_id
	     , s.prod->>'name' AS productname
          FROM (SELECT jsonb_array_elements(s.payload->'payload'->'order_items') AS prod
                  FROM stg.order_events s
                 WHERE s.object_id = ANY(%(object_ids)s)
                   AND s.object_type = %(object_type)s
               ) s
       ) s
  JOIN dds.h_product hp
    ON hp.product_id = s.product_id
),
upd_rec AS (
UPDATE dds.s_product_names spn
   SET end_dt = now() - interval '1 second'
  FROM ds d
 WHERE spn.h_product_pk = d.h_product_pk
   AND spn.end_dt = '4000-01-01'::timestamp
   AND spn.productname != d.productname
RETURNING hk_product_names_pk
),
upd_rec_cnt AS (
SELECT COUNT(1) AS cnt
  FROM upd_rec
)
INSERT INTO dds.s_product_names(hk_product_names_pk, h_product_pk, productname, 
	                        load_src, start_dt, end_dt)
SELECT gen_random_uuid() AS hk_product_names_pk
     , d.h_product_pk
     , d.productname
     , %(load_src)s AS load_src
     , now() AS start_dt
     , '4000-01-01' AS end_dt
  FROM ds d
  JOIN upd_rec_cnt u
    ON (1 = 1)
    ON CONFLICT (h_product_pk, end_dt) DO NOTHING;
