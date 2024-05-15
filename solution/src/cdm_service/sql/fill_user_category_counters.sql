INSERT INTO cdm.user_category_counters(user_id, category_id, category_name, order_cnt)
SELECT v.user_id
     , v.h_category_pk AS category_id
     , hc.category_name
     , v.order_cnt
  FROM (SELECT h.h_user_pk AS user_id
             , lpc.h_category_pk
             , COUNT(DISTINCT lou.h_order_pk) AS order_cnt
          FROM dds.h_user h
          JOIN dds.l_order_user lou
            ON lou.h_user_pk = h.h_user_pk
          JOIN dds.s_order_status sos
            ON sos.h_order_pk = lou.h_order_pk
           AND current_timestamp BETWEEN sos.start_dt AND sos.end_dt
           AND sos.status = 'CLOSED'
          JOIN dds.l_order_product lop
            ON lop.h_order_pk = lou.h_order_pk
          JOIN dds.l_product_category lpc
            ON lpc.h_product_pk = lop.h_product_pk
	 WHERE h.user_id = ANY(%s)
         GROUP BY h.h_user_pk, lpc.h_category_pk
         ORDER BY order_cnt DESC
       ) v
  JOIN dds.h_category hc
    ON hc.h_category_pk = v.h_category_pk
    ON CONFLICT(user_id, category_id)
    DO UPDATE
          SET order_cnt = EXCLUDED.order_cnt
        WHERE user_category_counters.user_id = EXCLUDED.user_id
          AND user_category_counters.category_id = EXCLUDED.category_id
          AND user_category_counters.order_cnt != EXCLUDED.order_cnt;
