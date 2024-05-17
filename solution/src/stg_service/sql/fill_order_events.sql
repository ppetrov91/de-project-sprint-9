INSERT INTO stg.order_events(object_id, payload, object_type, sent_dttm)
VALUES (%s, %s, %s, %s)
    ON CONFLICT(object_id)
    DO UPDATE
          SET object_type = EXCLUDED.object_type, 
              sent_dttm = EXCLUDED.sent_dttm, 
              payload = EXCLUDED.payload
        WHERE order_events.object_id = EXCLUDED.object_id
          AND (order_events.object_type != EXCLUDED.object_type OR
               order_events.sent_dttm != EXCLUDED.sent_dttm OR
               order_events.payload != EXCLUDED.payload);
