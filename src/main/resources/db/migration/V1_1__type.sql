create type bridge.t_meta_data as
(
    group_id           bigint,
    meta_id            bigint,
    schema_name        text,
    raw_name           text,
    buf_name           text,
    raw_full_name      text,
    buf_full_name      text,
    prc_exec_name      text,
    prc_exec_full_name text,
    raw_loop_query     text
);