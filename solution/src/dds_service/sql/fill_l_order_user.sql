INSERT INTO dds.l_order_user(hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
SELECT gen_random_uuid() AS hk_order_user_pk
     , ho.h_order_pk
     , hu.h_user_pk
     , now() AS load_dt
     , %(load_src)s AS load_src
  FROM (SELECT DISTINCT s.payload->'user'->>'id' AS user_id
             , (s.payload->>'id')::int AS order_id
          FROM stg.order_events s
 	 WHERE s.object_id = ANY(%(object_ids)s)
   	   AND s.object_type = %(object_type)s
       ) s
  JOIN dds.h_order ho
    ON ho.order_id = s.order_id
  JOIN dds.h_user hu
    ON hu.user_id = s.user_id
    ON CONFLICT (h_order_pk, h_user_pk) DO NOTHING;
