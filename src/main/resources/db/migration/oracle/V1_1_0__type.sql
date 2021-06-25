create type t_meta_data as object
(
    group_id           number,
    meta_id            number,
    schema_name        varchar2(30),
    raw_name           varchar2(30),
    buf_name           varchar2(30),
    raw_full_name      varchar2(100),
    buf_full_name      varchar2(100),
    prc_exec_name      varchar2(30),
    prc_exec_full_name varchar2(100),
    raw_loop_query     varchar2(1000)
);