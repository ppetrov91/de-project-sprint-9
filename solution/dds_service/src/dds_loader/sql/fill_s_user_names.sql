WITH ds AS (
SELECT hu.h_user_pk
     , s.username
     , s.userlogin
  FROM (SELECT DISTINCT s.payload->'user'->>'id' AS user_id
	     , s.payload->'user'->>'name' AS username
	     , s.payload->'user'->>'login' AS userlogin
          FROM stg.order_events s
 	 WHERE s.object_id = ANY(%s)
           AND s.object_type = %s
       ) s
  JOIN dds.h_user hu
    ON hu.user_id = s.user_id
),
upd_rec AS (
UPDATE dds.s_user_names sun
   SET end_dt = now() - interval '1 second'
  FROM ds d
 WHERE sun.h_user_pk = d.h_user_pk
   AND sun.end_dt = '4000-01-01'::timestamp
   AND (sun.username != d.username OR
   	sun.userlogin != d.userlogin
       )
RETURNING hk_user_names_pk
),
upd_rec_cnt AS (
SELECT COUNT(1) AS cnt
  FROM upd_rec
)
INSERT INTO dds.s_user_names(hk_user_names_pk, h_user_pk, username, userlogin, load_src, start_dt, end_dt)
SELECT gen_random_uuid() AS hk_user_names_pk
     , d.h_user_pk
     , d.username
     , d.userlogin
     , %s AS load_src
     , now() AS start_dt
     , '4000-01-01' AS end_dt
  FROM ds d
  JOIN upd_rec_cnt u
    ON (1 = 1)
    ON CONFLICT (h_user_pk, end_dt) DO NOTHING;
