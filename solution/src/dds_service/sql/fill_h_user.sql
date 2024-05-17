INSERT INTO dds.h_user(h_user_pk, user_id, load_dt, load_src)
SELECT gen_random_uuid() AS h_user_pk
     , s.user_id
     , now() AS load_dt
     , %(load_src)s AS load_src
  FROM (SELECT DISTINCT s.payload->'user'->>'id' AS user_id
          FROM stg.order_events s
         WHERE s.object_id = ANY(%(object_ids)s)
           AND s.object_type = %(object_type)s
       ) s
    ON CONFLICT (user_id) DO NOTHING;
