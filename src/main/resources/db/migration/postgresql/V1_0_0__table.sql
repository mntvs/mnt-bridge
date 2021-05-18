
create table bridge_group
(
    id          bigserial not null
        constraint bridge_group_pk
            primary key,
    tag         text      not null,
    note        text,
    schema_name text      not null
);

create unique index bridge_group_tag_uindex
    on bridge_group (tag);

create table bridge_meta
(
    id       bigserial not null
        constraint bridge_meta_pk
            primary key,
    tag      text      not null,
    note     text,
    group_id bigint    not null
        constraint bridge_meta_group_fk
            references bridge_group,
    constraint bridge_meta_uk_1
        unique (tag, group_id)
);
