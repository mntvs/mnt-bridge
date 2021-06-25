
create table bridge_group
(
    id          number not null
        constraint bridge_group_pk
            primary key,
    tag         varchar2(1000)      not null,
    note        varchar2(4000),
    schema_name varchar2(30)      not null
);


create unique index bridge_group_tag_uindex
    on bridge_group (tag);

create table bridge_meta
(
    id       number not null
        constraint bridge_meta_pk
            primary key,
    tag      varchar2(1000)      not null,
    note     varchar2(4000),
    group_id number    not null
        constraint bridge_meta_group_fk
            references bridge_group,
    constraint bridge_meta_uk_1
        unique (tag, group_id)
);
