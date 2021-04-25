create table bridge.fbi_raw_model
(
    id        bigserial                              not null
        constraint fbi_raw_test_pkey
            primary key,
    f_oper    smallint                 default 0     not null
        constraint fbi_raw_test_oper_ch
            check (f_oper = ANY (ARRAY [0, 1])),
    f_payload text,
    f_date    timestamp with time zone default now() not null,
    s_date    timestamp with time zone default now() not null,
    s_status  smallint                 default 0     not null,
    s_action  smallint                 default 0     not null,
    f_id      text                                   not null,
    s_msg     text,
    s_counter integer                  default 0     not null
);

comment on column bridge.fbi_raw_model.s_action is 'Current action. 0 - ready for processing, 2 - currently processing, 1 - will not precessed';

comment on column bridge.fbi_raw_model.f_id is 'foreign id';

comment on column bridge.fbi_raw_model.s_msg is 'Error message';

comment on column bridge.fbi_raw_model.s_counter is 'Count of the processing attempts';

create index fbi_raw_test_index
    on bridge.fbi_raw_model (s_action);

create table bridge.fbi_buf_model
(
    id        bigserial                              not null
        constraint fbi_buf_test_pkey
            primary key,
    f_oper    smallint                 default 0     not null,
    f_payload jsonb,
    s_payload jsonb,
    f_date    timestamp with time zone default now() not null,
    s_date    timestamp with time zone default now() not null,
    f_raw_id  bigint                                 not null,
    f_id      text                                   not null
);

create unique index fbi_buf_test_f_id_index
    on bridge.fbi_buf_model (f_id);

create unique index fbi_buf_test_raw_id_index
    on bridge.fbi_buf_model (f_raw_id);

