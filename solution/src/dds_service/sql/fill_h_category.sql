INSERT INTO dds.h_category(h_category_pk, category_name, load_dt, load_src)
SELECT gen_random_uuid() AS h_category_pk
     , s.category_name
     , now() AS load_dt
     , %(load_src)s AS load_src
  FROM (SELECT DISTINCT s.prod->>'category' AS category_name
          FROM (SELECT jsonb_array_elements(s.prod_arr) AS prod
                  FROM (SELECT s.payload->'payload'->'order_items' AS prod_arr
                          FROM stg.order_events s
                         WHERE s.object_id = ANY(%(object_ids)s)
                           AND s.object_type = %(object_type)s
                       ) s
               ) s
       ) s
    ON CONFLICT (category_name) DO NOTHING;
