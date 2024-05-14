INSERT INTO cdm.user_product_counters(user_id, product_id, product_name, order_cnt)
SELECT v.h_user_pk AS user_id
     , v.h_product_pk AS product_id
     , spn.productname
     , v.order_cnt
  FROM (SELECT h.h_user_pk
             , lop.h_product_pk
             , COUNT(lou.h_order_pk) AS order_cnt
          FROM dds.h_user h
          JOIN dds.l_order_user lou
            ON lou.h_user_pk = h.h_user_pk
          JOIN dds.s_order_status sos
            ON sos.h_order_pk = lou.h_order_pk
           AND %s BETWEEN sos.start_dt AND sos.end_dt
           AND sos.status = 'CLOSED'
          JOIN dds.l_order_product lop
            ON lop.h_order_pk = lou.h_order_pk
         WHERE h.user_id = ANY(%s)
         GROUP BY h.h_user_pk, lop.h_product_pk
       ) v
  JOIN dds.s_product_names spn
    ON spn.h_product_pk = v.h_product_pk
   AND %s BETWEEN spn.start_dt AND spn.end_dt
    ON CONFLICT (user_id, product_id)
    DO UPDATE
          SET product_name = EXCLUDED.product_name
            , order_cnt = EXCLUDED.order_cnt
        WHERE user_product_counters.user_id = EXCLUDED.user_id
          AND user_product_counters.product_id = EXCLUDED.product_id
          AND (user_product_counters.product_name != EXCLUDED.product_name OR
               user_product_counters.order_cnt != EXCLUDED.order_cnt);
