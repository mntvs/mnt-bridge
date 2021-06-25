create sequence ${schemaName}.sq_bridge_group;

create sequence ${schemaName}.sq_bridge_meta;

create table ${schemaName}.bridge_group
(
    id          number         not null
        constraint bridge_group_pk
            primary key,
    tag         varchar2(1000) not null,
    note        varchar2(4000),
    schema_name varchar2(30)   not null
);


create unique index ${schemaName}.bridge_group_tag_uindex
    on ${schemaName}.bridge_group (tag);

create table ${schemaName}.bridge_meta
(
    id       number         not null
        constraint bridge_meta_pk
            primary key,
    tag      varchar2(1000) not null,
    note     varchar2(4000),
    group_id number         not null
        constraint bridge_meta_group_fk
            references bridge_group,
    constraint bridge_meta_uk_1
        unique (tag, group_id)
);

create view ${schemaName}.bridge_meta_v
            (group_tag, meta_tag, group_id, meta_id, schema_name, raw_name, buf_name, prc_exec_name, raw_full_name,
             buf_full_name, prc_exec_full_name,
             raw_loop_query)
as
SELECT tt2.group_tag,
       tt2.meta_tag,
       tt2.group_id,
       tt2.meta_id,
       tt2.schema_name,
       tt2.raw_name,
       tt2.buf_name,
       tt2.prc_exec_name,
       tt2.raw_full_name,
       tt2.buf_full_name,
       tt2.prc_exec_full_name,
       ('select id from ' || tt2.raw_full_name) ||
       ' where s_action=0 order by s_date desc, id desc' AS raw_loop_query
FROM (SELECT tt1.group_tag,
             tt1.meta_tag,
             tt1.group_id,
             tt1.meta_id,
             tt1.schema_name,
             tt1.raw_name,
             tt1.buf_name,
             tt1.prc_exec_name,
             tt1.schema_name || '.' || tt1.raw_name      AS raw_full_name,
             tt1.schema_name || '.' || tt1.buf_name      AS buf_full_name,
             tt1.schema_name || '.' || tt1.prc_exec_name AS prc_exec_full_name
      FROM (SELECT bg.tag                   group_tag,
                   bm.tag                   meta_tag,
                   bm.group_id,
                   bm.id                    meta_id,
                   bg.schema_name,
                   'FBI_RAW_' || bm.tag  AS raw_name,
                   'FBI_BUF_' || bm.tag  AS buf_name,
                   'PRC_EXEC_' || bm.tag AS prc_exec_name
            FROM mnt_bridge.bridge_meta bm
                     JOIN mnt_bridge.bridge_group bg ON bg.id = bm.group_id) tt1) tt2;

delimiter $$

CREATE OR REPLACE FUNCTION ${schemaName}.fnc_raw_loop(raw_full_name VARCHAR2) RETURN SYS_REFCURSOR
AS
    c_raw SYS_REFCURSOR;
BEGIN
    open c_raw for 'select id from ' || raw_full_name || ' where s_action=0 order by s_date desc, id desc';
    RETURN c_raw;
END;
$$

create or replace procedure ${schemaName}.prc_create_meta_by_tag(a_group_tag VARCHAR2, a_meta_tag VARCHAR2,
                                                                 a_schema_name VARCHAR2 DEFAULT NULL)
as
    l_raw_full_name      VARCHAR2(100);
    l_buf_full_name      VARCHAR2(100);
    l_raw_name           VARCHAR2(100);
    l_buf_name           VARCHAR2(100);
    l_prc_exec_full_name VARCHAR2(100);
    l_prc_exec_name      VARCHAR2(100);
    l_group_id           NUMBER;
    l_meta_id            NUMBER;
    l_is_user_exists     NUMBER;
begin

    select count(*) into l_is_user_exists from all_users where username = a_schema_name;
    if (l_is_user_exists = 0) then
        execute immediate 'create user ' || a_schema_name || ' identified by ' || a_schema_name || ' default tablespace users';
    end if;

    begin
        select id into l_group_id from ${schemaName}.bridge_group where tag = a_group_tag;
    exception
        when no_data_found then
            insert into ${schemaName}.bridge_group (id, tag, schema_name)
            values (${schemaName}.sq_bridge_group.nextval, a_group_tag, NVL(a_schema_name, '${schemaName}'))
            returning id into l_group_id;
    end;

    begin
        select id into l_meta_id from ${schemaName}.bridge_meta where group_id = l_group_id and tag = a_meta_tag;
    exception
        when no_data_found then
            insert into ${schemaName}.bridge_meta (id,tag, group_id)
            values (${schemaName}.sq_bridge_meta.nextval, a_meta_tag, l_group_id)
            returning id into l_meta_id;
    end;

    select raw_full_name, raw_name, buf_full_name, buf_name, prc_exec_full_name, prc_exec_name
    into l_raw_full_name, l_raw_name, l_buf_full_name, l_buf_name, l_prc_exec_full_name, l_prc_exec_name
    from ${schemaName}.bridge_meta_v
    where group_id = l_group_id
      and meta_id = l_meta_id;

    /* RAW table creation */
    EXECUTE IMMEDIATE 'CREATE TABLE ' || l_raw_full_name || ' (id NUMBER PRIMARY KEY,' ||
                      ' f_oper NUMBER DEFAULT 0 NOT NULL ,' ||
                      ' f_payload CLOB,' ||
                      ' f_date DATE DEFAULT SYSDATE,' ||
                      ' s_date DATE DEFAULT SYSDATE,' ||
                      ' s_status NUMBER DEFAULT 0 NOT NULL,' ||
                      ' s_action NUMBER DEFAULT 0 NOT NULL,' ||
                      ' f_id VARCHAR2(2000) NOT NULL,' ||
                      ' s_msg CLOB,' ||
                      ' s_counter NUMBER DEFAULT 0 NOT NULL' ||
                      ')';

    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || l_raw_full_name ||
                      '.s_action is ''Current action. 0 - ready for processing, 1 - will not precessed''';

    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || l_raw_full_name || '.f_id is ''foreign id''';

    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || l_raw_full_name || '.s_msg is ''Error message''';

    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || l_raw_full_name || '.s_counter is ''Count of the processing attempts''';

    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || l_raw_full_name ||
                      '.s_status is ''Status: 0: nothing happend; 1: successfull processing; -3: processing error (repeat); 4: filtered finally; -4: filtered (repeat); 5: skiped without repeat; 3: critical error''';

    EXECUTE IMMEDIATE 'CREATE INDEX IF NOT EXISTS ' || l_raw_name || '_index
        ON ' || l_raw_full_name || ' (s_action)';

    EXECUTE IMMEDIATE 'ALTER TABLE ' || l_raw_full_name || ' ADD CONSTRAINT ' || l_raw_name ||
                      '_oper_ch CHECK (f_oper IN (0,1))';

    EXECUTE IMMEDIATE 'ALTER TABLE ' || l_raw_full_name || ' ADD CONSTRAINT ' || l_raw_name ||
                      '_action_ch CHECK (s_action IN (0,1))';

    EXECUTE IMMEDIATE 'ALTER TABLE ' || l_raw_full_name || ' ADD CONSTRAINT ' || l_raw_name ||
                      '_status_ch CHECK (s_action IN (0,1,3,4,5,-3,-4))';
    /* BUF table creation */

    EXECUTE IMMEDIATE 'CREATE TABLE ' || l_buf_full_name || ' (' || 'id NUMBER PRIMARY KEY,' ||
                      ' f_oper NUMBER DEFAULT 0 NOT NULL ,' ||
                      ' f_payload CLOB,' ||
                      ' s_payload CLOB,' ||
                      ' f_date DATE DEFAULT SYSDATE NOT NULL ,' ||
                      ' s_date DATE DEFAULT SYSDATE NOT NULL ,' ||
                      ' f_raw_id NUMBER NOT NULL,' ||
                      ' f_id VARCHAR2(2000) NOT NULL' ||
                      ')';

    EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX ' || l_buf_name || '_f_id_index
        on ' || l_buf_full_name || ' (f_id)';


    EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX ' || l_buf_name || '_raw_id_index
        on ' || l_buf_full_name || ' (f_raw_id)';


    EXECUTE IMMEDIATE 'ALTER TABLE ' || l_buf_full_name || ' ADD CONSTRAINT %s_oper_ch CHECK (f_oper IN (0,1))';

    /* Procedure creation */
    BEGIN
        EXECUTE IMMEDIATE
                'create procedure ' || l_prc_exec_full_name || '(a_buf_id bigint, a_action_tag text)
                as
                    l_sqlerrm VARCHAR2(2000);
                begin
                null;
        exception when others then
                l_sqlerrm:=sqlerrm;
            raise_application_error(-20001, '' || lower(l_prc_exec_name) || '' error : '' || l_sqlerrm || '' {buf.id='' || a_buf_id || '', action_tag='' || a_action_tag || ''}'');
    end;';
    end;
EXCEPTION WHEN OTHERS THEN
    raise_application_error(-20001, 'prc_create_meta_by_tag error : ' || sqlerrm || ' {group_tag=' || a_group_tag || ',meta_tag=' || a_meta_tag || ',schema_name=' || a_schema_name || '}');
END;
$$


create procedure ${schemaName}.prc_drop_meta_by_tag(a_group_tag varchar2, a_meta_tag varchar2)
as
    l_raw_full_name      varchar2(100);
    l_buf_full_name      varchar2(100);
    l_prc_exec_full_name varchar2(100);
    l_raw_name           VARCHAR2(100);
    l_buf_name           VARCHAR2(100);
    l_prc_exec_name      VARCHAR2(100);
    l_schema_name        VARCHAR2(30);
    l_group_id           number;
    l_count              number;
begin
    begin
        select raw_full_name,
               buf_full_name,
               prc_exec_full_name,
               group_id,
               raw_name,
               buf_name,
               prc_exec_name,
               schema_name
        into l_raw_full_name, l_buf_full_name, l_prc_exec_full_name,l_group_id,l_raw_name,l_buf_name,l_prc_exec_name, l_schema_name
        from ${schemaName}.bridge_meta_v
        where group_tag = a_group_tag
          and meta_tag = a_meta_tag;
    exception
        when no_data_found then
            raise_application_error(-20001,
                                    'prc_drop_meta_by_tag error : bridge meta not found {group_tag=' || a_group_tag ||
                                    ', meta_tag=' || a_meta_tag || '}');
    end;


    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_prc_exec_name)
      AND OWNER = UPPER(l_schema_name)
      AND OBJECT_TYPE = 'PROCEDURE';

    if l_count > 0 then
        EXECUTE IMMEDIATE 'DROP PROCEDURE ' || l_prc_exec_full_name;
    end if;

    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_raw_name)
      AND OWNER = UPPER(l_schema_name)
      AND OBJECT_TYPE = 'TABLE';

    if l_count > 0 then
        EXECUTE IMMEDIATE 'DROP TABLE ' || l_raw_full_name;
    end if;

    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_buf_name)
      AND OWNER = UPPER(l_schema_name)
      AND OBJECT_TYPE = 'TABLE';

    if l_count > 0 then
        EXECUTE IMMEDIATE 'DROP TABLE ' || l_buf_full_name;
    end if;

    delete from ${schemaName}.bridge_meta where group_id = l_group_id and tag = a_meta_tag;

    select count(*) into l_count from ${schemaName}.bridge_meta where group_id = l_group_id;

    if l_count = 0 then
        delete from ${schemaName}.bridge_meta where group_id = l_group_id;
    end if;
end;
$$


create or replace procedure ${schemaName}.prc_pre_process(a_raw_id IN NUMBER, a_raw_full_name IN VARCHAR2,
                                                          a_buf_full_name IN VARCHAR2,
                                                          a_prc_exec_full_name IN VARCHAR2,
                                                          a_processed_status IN OUT NUMBER,
                                                          a_error_message IN OUT VARCHAR2)
as
    l_action_tag    VARCHAR2(1);
    l_buf_id        NUMBER;
    l_buf_f_date    DATE;
    l_raw_id        NUMBER;
    l_raw_f_id      VARCHAR2(2000);
    l_raw_f_payload CLOB;
    l_raw_f_date    DATE;
    l_raw_s_status  NUMBER;
begin
    execute IMMEDIATE 'select id,f_id,f_payload,f_date,s_status from ' || a_raw_full_name ||
                      ' where s_action=0 and id=:1 for update skip locked'
        into l_raw_id,l_raw_f_id,l_raw_f_payload,l_raw_f_date,l_raw_s_status
        using a_raw_id;

    if not l_raw_id is null then
        execute immediate 'select id,f_date from ' || a_buf_full_name ||
                          ' where f_id=:1 for update' into l_buf_id,l_buf_f_date using l_raw_f_id;

        if l_buf_id is null or l_buf_f_date <= l_raw_f_date then

            if l_buf_id is null then
                execute immediate 'insert into ' || a_buf_full_name ||
                                  ' (f_raw_id,f_id, f_payload, f_date) values ( :1, :2, :3, :4 ) returning id into :5' using
                    l_raw_id,l_raw_f_id,l_raw_f_payload, l_raw_f_date returning into l_buf_id;
                l_action_tag := 'I';
            else
                execute immediate 'update ' || a_buf_full_name ||
                                  ' set (f_raw_id, f_payload, f_date)=(:1,:2,:3) where f_id=:4' using l_raw_id, l_raw_f_payload, l_raw_f_date, l_raw_f_id;
                l_action_tag := 'U';
            end if;


            if l_action_tag = 'U' OR l_action_tag = 'D' OR
               (l_action_tag = 'I' AND NOT l_raw_f_date IS NULL) then
                execute immediate 'begin ' || a_prc_exec_full_name || ' (:1,:2); end;' using l_buf_id,l_action_tag;
                l_raw_s_status := 1; -- Success
            else
                l_raw_s_status := 5; -- Skiped
            end if;
        else
            l_raw_s_status := 5; -- Skiped
        end if;

        a_processed_status := l_raw_s_status;
        -- processing finished successfully
    else
        a_processed_status := 0; -- processing not happened. Omitted
    end if;
exception
    when others then
        a_processed_status := -3; -- processing happend with error
        a_error_message := sqlerrm;
end;
$$

create or replace procedure ${schemaName}.prc_post_process(a_raw_id IN NUMBER, a_raw_full_name IN VARCHAR2,
                                                           a_processed_status IN NUMBER,
                                                           a_error_message IN VARCHAR2)
as
begin
    if a_processed_status <> 0 then
        execute immediate 'update ' || a_raw_full_name ||
                          ' set (s_status,s_msg,s_date,s_action, s_counter)=(:1,:2,:3,:4,s_counter+1) where id=:5' using a_processed_status,a_error_message,sysdate,case when a_processed_status < 0 then 0 else 1 end, a_raw_id;
    end if;
end;
$$


create or replace procedure ${schemaName}.prc_start_task(a_group_tag VARCHAR2, a_meta_tag VARCHAR2,
                                                         a_raw_id NUMBER DEFAULT NULL)
as
    l_count              NUMBER := 0;
    l_error_message      VARCHAR2(4000);
    l_processed_status   NUMBER;
    l_raw_loop_query     VARCHAR2(4000);
    l_raw_full_name      VARCHAR2(100);
    l_buf_full_name      VARCHAR2(100);
    l_prc_exec_full_name VARCHAR2(100);
    TYPE cur_typ IS REF CURSOR;
    c_raw_rec            cur_typ;
    l_raw_id             NUMBER;
begin

    select RAW_LOOP_QUERY, raw_full_name, buf_full_name, prc_exec_full_name
    into l_raw_loop_query,l_raw_full_name,l_buf_full_name,l_prc_exec_full_name
    from ${schemaName}.BRIDGE_META_V
    where GROUP_tag = a_group_tag
      and META_TAG = a_meta_tag;
    OPEN c_raw_rec FOR l_raw_loop_query;


    loop
        FETCH c_raw_rec INTO l_raw_id;

        /* process the result row */
        ${schemaName}.prc_pre_process(l_raw_id, l_raw_full_name, l_buf_full_name,
                                      l_prc_exec_full_name, l_processed_status, l_error_message);

        ${schemaName}.prc_post_process(l_raw_id, l_raw_full_name, l_processed_status, l_error_message);

        if l_processed_status <> 0 then
            l_count := l_count + 1;
        end if;
        if MOD(l_count, 100) = 0 then
            commit;
        end if;
    end loop;
    commit;
end;
$$


