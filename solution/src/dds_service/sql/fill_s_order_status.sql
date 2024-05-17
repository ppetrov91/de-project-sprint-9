WITH ds AS (
SELECT ho.h_order_pk
     , s.payload->>'status' AS status
  FROM stg.order_events s 
  JOIN dds.h_order ho
    ON ho.order_id = s.object_id
 WHERE s.object_id = ANY(%(object_ids)s)
   AND s.object_type = %(object_type)s
),
upd_rec AS (
UPDATE dds.s_order_status soc
   SET end_dt = now() - interval '1 second'
  FROM ds d
 WHERE soc.h_order_pk = d.h_order_pk
   AND soc.end_dt = '4000-01-01'::timestamp
   AND soc.status != d.status
 RETURNING hk_order_status_pk
),
upd_rec_cnt AS (
SELECT COUNT(1) AS cnt
  FROM upd_rec
)
INSERT INTO dds.s_order_status(hk_order_status_pk, h_order_pk, status, load_src, start_dt, end_dt)
SELECT gen_random_uuid() AS hk_order_cost_pk
     , d.h_order_pk
     , d.status
     , %(load_src)s AS load_src
     , now() AS start_dt
     , '4000-01-01' AS end_dt
  FROM ds d
  JOIN upd_rec_cnt u
    ON (1 = 1)
    ON CONFLICT (h_order_pk, end_dt) DO NOTHING;
