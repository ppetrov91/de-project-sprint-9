WITH ds AS (
SELECT hr.h_restaurant_pk
     , s.restaurantname
  FROM (SELECT DISTINCT s.payload->'restaurant'->>'id' AS restaurant_id
             , s.payload->'restaurant'->>'name' AS restaurantname
          FROM stg.order_events s
         WHERE s.object_id = ANY(%s)
           AND s.object_type = %s
       ) s
  JOIN dds.h_restaurant hr
    ON hr.restaurant_id = s.restaurant_id
),
upd_rec AS (
UPDATE dds.s_restaurant_names spn
   SET end_dt = now() - interval '1 second'
  FROM ds d
 WHERE spn.h_restaurant_pk = d.h_restaurant_pk
   AND spn.end_dt = '4000-01-01'::timestamp
   AND spn.restaurantname != d.restaurantname
RETURNING hk_restaurant_names_pk
),
upd_rec_cnt AS (
SELECT COUNT(1) AS cnt
  FROM upd_rec
)
INSERT INTO dds.s_restaurant_names(hk_restaurant_names_pk, h_restaurant_pk, restaurantname,
				   load_src, start_dt, end_dt)
SELECT gen_random_uuid() AS hk_restaurant_names_pk
     , d.h_restaurant_pk
     , d.restaurantname
     , %s AS load_src
     , now() AS start_dt
     , '4000-01-01' AS end_dt
  FROM ds d
  JOIN upd_rec_cnt u
    ON (1 = 1)
    ON CONFLICT (h_restaurant_pk, end_dt) DO NOTHING;
