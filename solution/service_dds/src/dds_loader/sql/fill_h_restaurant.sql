INSERT INTO dds.h_restaurant(h_restaurant_pk, restaurant_id, load_dt, load_src)
SELECT gen_random_uuid() AS h_restaurant_pk
     , s.restaurant_id
     , now() AS load_dt
     , %s AS load_src
  FROM (SELECT DISTINCT s.restaurant_id
          FROM (SELECT s.payload->'restaurant'->>'id' AS restaurant_id
                  FROM stg.order_events s
                 WHERE s.object_id = ANY(%s)
                   AND s.object_type = %s
               ) s
       ) s
    ON CONFLICT (restaurant_id) DO NOTHING;
