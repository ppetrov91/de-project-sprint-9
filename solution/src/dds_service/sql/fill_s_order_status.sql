INSERT INTO dds.s_order_status(hk_order_status_pk, h_order_pk, status, load_src, start_dt, end_dt)
SELECT gen_random_uuid() AS hk_order_cost_pk
     , v.h_order_pk
     , v.status
     , %(load_src)s AS load_src
     , v.start_dt
     , LEAD(v.start_dt - interval '1 second', 1, '4000-01-01'::timestamp) OVER(PARTITION BY v.h_order_pk ORDER BY v.start_dt) AS end_dt 
  FROM (SELECT v.h_order_pk
             , v.status->>'status' AS status
             , (v.status->>'dttm')::timestamp AS start_dt
          FROM (SELECT v.h_order_pk
                     , jsonb_array_elements(v.statuses) AS status
                  FROM (SELECT ho.h_order_pk
                             , s.payload->'payload'->'statuses' AS statuses
                          FROM stg.order_events s
                          JOIN dds.h_order ho
                            ON ho.order_id = s.object_id
                         WHERE s.object_id = ANY(%(object_ids)s)
                           AND s.object_type = %(object_type)s
                       ) v
               ) v
       ) v	
    ON CONFLICT (h_order_pk, end_dt) 
    DO UPDATE
	  SET status = EXCLUDED.status
	    , start_dt = EXCLUDED.start_dt
        WHERE s_order_status.h_order_pk = EXCLUDED.h_order_pk
	  AND s_order_status.end_dt = EXCLUDED.end_dt
	  AND (s_order_status.status != EXCLUDED.status OR
	       s_order_status.start_dt != EXCLUDED.start_dt	
	      );
