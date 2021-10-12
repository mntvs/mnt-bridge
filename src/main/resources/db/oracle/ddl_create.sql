create sequence ${schemaName}.sq_bridge_group;

create sequence ${schemaName}.sq_bridge_meta;

create table ${schemaName}.bridge_group
(
    id          number(19)     not null
        constraint bridge_group_pk
            primary key,
    tag         varchar2(1000) not null,
    note        varchar2(4000),
    schema_name varchar2(30)   not null,
    param       varchar2(4000) default '<PARAM><ORDER>LIFO</ORDER><ATTEMPT>-1</ATTEMPT></PARAM>'
);

comment on table ${schemaName}.bridge_group is 'Contains groups descriptions ${versionStr}';

create unique index ${schemaName}.bridge_group_tag_uindex
    on ${schemaName}.bridge_group (tag);

create table ${schemaName}.bridge_meta
(
    id       number(19)     not null
        constraint bridge_meta_pk
            primary key,
    tag      varchar2(1000) not null,
    note     varchar2(4000),
    param    varchar2(4000),
    group_id number(19)     not null
        constraint bridge_meta_group_fk
            references bridge_group,
    constraint bridge_meta_uk_1
        unique (tag, group_id)
);

comment on table ${schemaName}.bridge_meta is 'Contains meta information ${versionStr}';

create view ${schemaName}.bridge_meta_v
            (group_tag, meta_tag, group_id, meta_id, schema_name, raw_name, buf_name, prc_exec_name, raw_full_name,
             buf_full_name, prc_exec_full_name, param, param_type)
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
       tt2.param,
       'XML' param_type
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
             tt1.schema_name || '.' || tt1.prc_exec_name AS prc_exec_full_name,
             tt1.param
      FROM (SELECT bg.tag                         group_tag,
                   bm.tag                         meta_tag,
                   bm.group_id,
                   bm.id                          meta_id,
                   bg.schema_name,
                   'FBI_RAW_' || bm.tag  AS       raw_name,
                   'FBI_BUF_' || bm.tag  AS       buf_name,
                   'PRC_EXEC_' || bm.tag AS       prc_exec_name,
                   UPPER(NVL(bm.param, bg.param)) param
            FROM ${schemaName}.bridge_meta bm
                     JOIN ${schemaName}.bridge_group bg ON bg.id = bm.group_id) tt1) tt2;

comment on table ${schemaName}.bridge_meta_v is 'Meta data view ${versionStr}';


delimiter $$

CREATE OR REPLACE FUNCTION ${schemaName}.fnc_raw_loop(raw_full_name VARCHAR2) RETURN SYS_REFCURSOR
AS
    c_raw SYS_REFCURSOR;
BEGIN
    /* Returns cursor for loop for the raw table ${versionStr} */
    open c_raw for 'select id from ' || raw_full_name || ' where s_action=0 order by f_date asc, id asc';
    RETURN c_raw;
END;
$$

create trigger ${schemaName}.TRG_BRIDGE_GROUP_PK
    before insert
    on ${schemaName}.BRIDGE_GROUP
    for each row
    when (new.id IS NULL)
BEGIN
    /* Sequence trigger ${versionStr} */
    SELECT ${schemaName}.SQ_BRIDGE_GROUP.nextval INTO :NEW.id FROM DUAL;
END;
$$

create trigger ${schemaName}.TRG_BRIDGE_META_PK
    before insert
    on ${schemaName}.BRIDGE_META
    for each row
    when (new.id IS NULL)
BEGIN
    /* Sequence trigger ${versionStr} */
    SELECT ${schemaName}.SQ_BRIDGE_META.nextval INTO :NEW.id FROM DUAL;
END;

$$



create or replace procedure ${schemaName}.prc_create_meta_by_tag(a_group_tag VARCHAR2, a_meta_tag VARCHAR2,
                                                                 a_schema_name VARCHAR2 DEFAULT NULL,
                                                                 a_param VARCHAR2 DEFAULT NULL)
as
    l_raw_full_name      VARCHAR2(100);
    l_buf_full_name      VARCHAR2(100);
    l_raw_name           VARCHAR2(100);
    l_buf_name           VARCHAR2(100);
    l_prc_exec_full_name VARCHAR2(100);
    l_prc_exec_name      VARCHAR2(100);
    l_name               VARCHAR2(100);
    l_schema_name        ${schemaName}.bridge_group.schema_name%TYPE;
    l_group_id           NUMBER(19);
    l_meta_id            NUMBER(19);
    l_is_user_exists     NUMBER;
    l_count              NUMBER;
    l_time_created       VARCHAR2(100);
begin
    /* Creates db objects ${versionStr} */

    l_time_created := TO_CHAR(sysdate, 'yyyy-mm-dd hh24:mi:ss');

    begin
        select id into l_group_id from ${schemaName}.bridge_group where tag = a_group_tag;
    exception
        when no_data_found then
            insert into ${schemaName}.bridge_group (id, tag, schema_name)
            values (${schemaName}.sq_bridge_group.nextval, a_group_tag, UPPER(NVL(a_schema_name, '${schemaName}')))
            returning id into l_group_id;
    end;

    begin
        select id into l_meta_id from ${schemaName}.bridge_meta where group_id = l_group_id and tag = a_meta_tag;
    exception
        when no_data_found then
            insert into ${schemaName}.bridge_meta (id, tag, group_id, param)
            values (${schemaName}.sq_bridge_meta.nextval, a_meta_tag, l_group_id, a_param)
            returning id into l_meta_id;
    end;

    select raw_full_name, raw_name, buf_full_name, buf_name, prc_exec_full_name, prc_exec_name, schema_name
    into l_raw_full_name, l_raw_name, l_buf_full_name, l_buf_name, l_prc_exec_full_name, l_prc_exec_name, l_schema_name
    from ${schemaName}.bridge_meta_v
    where group_id = l_group_id
      and meta_id = l_meta_id;

    select count(*) into l_is_user_exists from all_users where username = l_schema_name;
    if (l_is_user_exists = 0) then
        execute immediate 'create user ' || a_schema_name || ' identified by "' || l_schema_name ||
                          '" default tablespace users';
    end if;

    /* RAW table creation */

    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_raw_name)
      AND OWNER = UPPER(l_schema_name)
      AND OBJECT_TYPE = 'TABLE';

    if l_count = 0 then
        EXECUTE IMMEDIATE 'CREATE TABLE ' || l_raw_full_name || ' (id NUMBER(19) PRIMARY KEY,' ||
                          ' f_oper NUMBER DEFAULT 0 CONSTRAINT ' || l_raw_name ||
                          '_oper_ch CHECK (f_oper IN (0,1))  NOT NULL,' ||
                          ' f_payload CLOB,' ||
                          ' f_date DATE DEFAULT SYSDATE,' ||
                          ' s_date DATE DEFAULT SYSDATE,' ||
                          ' s_status NUMBER DEFAULT 0 CONSTRAINT ' || l_raw_name ||
                          '_status_ch CHECK (s_status IN (0,1,3,4,5,-3,-4)) NOT NULL,' ||
                          ' s_action NUMBER DEFAULT 0 CONSTRAINT ' || l_raw_name ||
                          '_action_ch CHECK (s_action IN (0,1)) NOT NULL,' ||
                          ' f_id VARCHAR2(2000) NOT NULL,' ||
                          ' s_msg CLOB,' ||
                          ' f_msg CLOB,' ||
                          ' s_counter NUMBER DEFAULT 0 NOT NULL,' ||
                          ' f_group_id VARCHAR2(2000)' ||
                          ')';
    end if;

    EXECUTE IMMEDIATE 'COMMENT ON TABLE ' || l_raw_full_name ||
                      ' IS ''Raw table. Generated ' || l_time_created || ' ${versionStr}''';


    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || l_raw_full_name ||
                      '.s_action is ''Current action. 0 - ready for processing, 1 - will not precessed''';

    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || l_raw_full_name || '.f_id is ''foreign id''';

    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || l_raw_full_name || '.s_msg is ''Error message''';

    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || l_raw_full_name || '.s_counter is ''Count of the processing attempts''';

    EXECUTE IMMEDIATE 'COMMENT ON COLUMN ' || l_raw_full_name ||
                      '.s_status is ''Status: 0: nothing happend; 1: successfull processing; -3: processing error (repeat); 4: filtered finally; -4: filtered (repeat); 5: skiped without repeat; 3: critical error''';

    l_name := l_raw_name || '_index';
    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_name)
      AND OWNER = UPPER(l_schema_name)
      AND OBJECT_TYPE = 'INDEX';

    if l_count = 0 then
        EXECUTE IMMEDIATE 'CREATE INDEX ' || l_schema_name || '.' || l_name ||
                          ' ON ' || l_raw_full_name || ' (s_action)';
    end if;

    l_name := l_raw_name || '_g_index';
    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_name)
      AND OWNER = UPPER(l_schema_name)
      AND OBJECT_TYPE = 'INDEX';

    if l_count = 0 then
        EXECUTE IMMEDIATE 'CREATE INDEX ' || l_schema_name || '.' || l_name ||
                          ' ON ' || l_raw_full_name || ' (f_group_id)';
    end if;

    /* BUF table creation */
    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_buf_name)
      AND OWNER = UPPER(l_schema_name)
      AND OBJECT_TYPE = 'TABLE';

    if l_count = 0 then
        EXECUTE IMMEDIATE 'CREATE TABLE ' || l_buf_full_name || ' (' || 'id NUMBER(19) PRIMARY KEY,' ||
                          ' f_oper NUMBER DEFAULT 0 CONSTRAINT ' || l_buf_name ||
                          '_s_oper_ch CHECK (f_oper IN (0,1)) NOT NULL ,' ||
                          ' f_payload CLOB,' ||
                          ' s_payload CLOB,' ||
                          ' f_date DATE DEFAULT SYSDATE NOT NULL ,' ||
                          ' s_date DATE DEFAULT SYSDATE NOT NULL ,' ||
                          ' f_raw_id NUMBER(19) NOT NULL,' ||
                          ' f_id VARCHAR2(2000) NOT NULL,' ||
                          ' s_counter NUMBER DEFAULT 0 NOT NULL,' ||
                          ' f_group_id VARCHAR2(2000)' ||
                          ')';
    end if;

    EXECUTE IMMEDIATE 'COMMENT ON TABLE ' || l_buf_full_name ||
                      ' IS ''Buf table. Generated ' || l_time_created || ' ${versionStr}''';

    l_name := l_buf_name || '_f_id_index';
    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_name)
      AND OWNER = UPPER(l_schema_name)
      AND OBJECT_TYPE = 'INDEX';

    if l_count = 0 then
        EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX ' || l_schema_name || '.' || l_name ||
                          ' on ' || l_buf_full_name || ' (f_id)';
    end if;


    l_name := l_buf_name || '_g_index';
    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_name)
      AND OWNER = UPPER(l_schema_name)
      AND OBJECT_TYPE = 'INDEX';

    if l_count = 0 then
        EXECUTE IMMEDIATE 'CREATE INDEX ' || l_schema_name || '.' || l_name ||
                          ' on ' || l_buf_full_name || ' (f_group_id)';
    end if;

    l_name := l_buf_name || '_raw_id_index';
    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_name)
      AND OWNER = UPPER(l_schema_name)
      AND OBJECT_TYPE = 'INDEX';

    if l_count = 0 then
        EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX ' || l_schema_name || '.' || l_name ||
                          ' on ' || l_buf_full_name || ' (f_raw_id)';
    end if;


    l_name := 'SQ_' || l_raw_name;
    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_name)
      AND OWNER = UPPER(l_schema_name)
      AND OBJECT_TYPE = 'SEQUENCE';


    if l_count = 0 then
        EXECUTE IMMEDIATE 'CREATE SEQUENCE ' || l_schema_name || '.' || l_name;
    end if;

    l_name := 'SQ_' || l_buf_name;
    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_name)
      AND OWNER = UPPER(l_schema_name)
      AND OBJECT_TYPE = 'SEQUENCE';

    if l_count = 0 then
        EXECUTE IMMEDIATE 'CREATE SEQUENCE ' || l_schema_name || '.' || l_name;
    end if;


    l_name := 'TRG_' || l_buf_name || '_PK';
    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_name)
      AND OWNER = l_schema_name
      AND OBJECT_TYPE = 'TRIGGER';

    if l_count = 0 then
        EXECUTE IMMEDIATE 'create trigger ' || l_schema_name || '.' || l_name ||
                          ' before insert' ||
                          ' on ' || l_schema_name || '.' || l_buf_name ||
                          ' for each row' ||
                          ' when (new.id IS NULL)' ||
                          ' BEGIN
                            /* Generated ' || l_time_created || ' ${versionStr} */
                            SELECT ' || l_schema_name || '.' || 'SQ_' || l_buf_name ||
                          '.nextval INTO :NEW.id FROM DUAL;' ||
                          ' END;';
    end if;

    l_name := 'TRG_' || l_raw_name || '_PK';
    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_name)
      AND OWNER = l_schema_name
      AND OBJECT_TYPE = 'TRIGGER';

    if l_count = 0 then
        EXECUTE IMMEDIATE 'create trigger ' || l_schema_name || '.' || l_name ||
                          ' before insert' ||
                          ' on ' || l_schema_name || '.' || l_raw_name ||
                          ' for each row' ||
                          ' when (new.id IS NULL)' ||
                          ' BEGIN
                            /* Generated ' || l_time_created || ' ${versionStr} */
                            SELECT ' || l_schema_name || '.' || 'SQ_' || l_raw_name ||
                          '.nextval INTO :NEW.id FROM DUAL;' ||
                          ' END;';
    end if;

    /* Procedure creation */
    SELECT count(*)
    into l_count
    FROM ALL_OBJECTS
    WHERE OBJECT_NAME = UPPER(l_prc_exec_name)
      AND OWNER = UPPER(l_schema_name)
      AND OBJECT_TYPE = 'PROCEDURE';

    if l_count = 0 then
        EXECUTE IMMEDIATE
                'create procedure ' || l_prc_exec_full_name || '(a_raw_id NUMBER, a_buf_id NUMBER)
                as
                begin
                /* Generated ' || l_time_created || ' ${versionStr} */
                null;
    end;';
    end if;

EXCEPTION
    WHEN OTHERS THEN
        raise_application_error(-20801, 'prc_create_meta_by_tag error : ' || sqlerrm || ' {group_tag=' || a_group_tag ||
                                        ',meta_tag=' || a_meta_tag || ',schema_name=' || a_schema_name || '}');
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
    l_group_id           NUMBER;
    l_count              NUMBER;
    l_name               VARCHAR2(30);
begin
    /* Drops db objects ${versionStr} */
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
            DBMS_OUTPUT.PUT_LINE('prc_drop_meta_by_tag : bridge meta not found {group_tag=' || a_group_tag ||
                                 ', meta_tag=' || a_meta_tag || '}');

        /* raise_application_error(-20001,
                                 'prc_drop_meta_by_tag error : bridge meta not found {group_tag=' || a_group_tag ||
                                 ', meta_tag=' || a_meta_tag || '}');*/

    end;

    IF NOT l_group_id IS NULL THEN
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

        l_name := 'SQ_' || l_buf_name;
        SELECT count(*)
        into l_count
        FROM ALL_OBJECTS
        WHERE OBJECT_NAME = UPPER(l_name)
          AND OWNER = UPPER(l_schema_name)
          AND OBJECT_TYPE = 'SEQUENCE';

        if l_count > 0 then
            EXECUTE IMMEDIATE 'DROP SEQUENCE ' || l_schema_name || '.' || l_name;
        end if;

        l_name := 'SQ_' || l_raw_name;
        SELECT count(*)
        into l_count
        FROM ALL_OBJECTS
        WHERE OBJECT_NAME = UPPER(l_name)
          AND OWNER = UPPER(l_schema_name)
          AND OBJECT_TYPE = 'SEQUENCE';

        if l_count > 0 then
            EXECUTE IMMEDIATE 'DROP SEQUENCE ' || l_schema_name || '.' || l_name;
        end if;


        delete from ${schemaName}.bridge_meta where group_id = l_group_id and tag = a_meta_tag;

        select count(*) into l_count from ${schemaName}.bridge_meta where group_id = l_group_id;

        if l_count > 0 then
            delete from ${schemaName}.bridge_meta where group_id = l_group_id;
        end if;

    END IF;
end;
$$


create or replace procedure ${schemaName}.prc_pre_process(a_raw_id IN NUMBER, a_raw_full_name IN VARCHAR2,
                                                          a_buf_full_name IN VARCHAR2,
                                                          a_f_group_id IN VARCHAR2,
                                                          a_processed_status IN OUT NUMBER,
                                                          a_error_message IN OUT VARCHAR2,
                                                          a_buf_id OUT NUMBER)
as
    l_raw_id        NUMBER(19);
    l_raw_f_id      VARCHAR2(2000);
    l_raw_f_payload CLOB;
    l_raw_f_date    DATE;
    l_raw_s_status  NUMBER;
    l_f_group_id    VARCHAR2(2000);
begin
    a_buf_id := null;

    /* Executes before process ${versionStr} */
    execute IMMEDIATE 'select id,f_id,f_payload,f_date,s_status from ' || a_raw_full_name ||
                      ' where s_action=0 and id=:1 for update skip locked'
        into l_raw_id,l_raw_f_id,l_raw_f_payload,l_raw_f_date,l_raw_s_status
        using a_raw_id;

    execute immediate 'merge into ' || a_buf_full_name ||
                      ' a using (select :1 as raw_id, :2 raw_f_id, :3 raw_f_payload, :4 f_date, :5 f_group_id from dual) b on (a.f_id = b.raw_f_id) when not matched then ' ||
                      'insert (f_raw_id,f_id, f_payload, f_date, s_counter, f_group_id) values (b.raw_id, b.raw_f_id, b.raw_f_payload, b.f_date, 1, b.f_group_id) when matched then ' ||
                      'update set a.f_raw_id=b.raw_id,a.f_payload=b.raw_f_payload,a.f_date=b.f_date, s_counter=s_counter+1, a.f_group_id=b.f_group_id where :6>a.f_date OR (:7=a.f_date AND :8>=a.f_raw_id)' using
        l_raw_id,l_raw_f_id,l_raw_f_payload, l_raw_f_date, a_f_group_id,l_raw_f_date,l_raw_f_date, l_raw_id;

    if SQL%ROWCOUNT > 0 then
        execute immediate 'select id from ' || a_buf_full_name || ' where f_id=:1' into a_buf_id using l_raw_f_id;
        l_raw_s_status := 1; -- Success
    else
        l_raw_s_status := 5; -- Skiped
    end if;

    a_processed_status := l_raw_s_status;
    -- processing finished successfully
exception
    when no_data_found then
        a_processed_status := 0; -- processing not happened. Omitted
        a_error_message := '';
    when others then
        if sqlcode = -20993 then
            a_processed_status := 3; -- unrepeatable status
        else
            a_processed_status := -3; -- processing happened with error
        end if;
        a_error_message := sqlerrm;
end;
$$

create or replace procedure ${schemaName}.prc_process(a_raw_id IN NUMBER, a_buf_id IN NUMBER,
                                                      a_prc_exec_full_name IN VARCHAR2,
                                                      a_processed_status IN OUT NUMBER,
                                                      a_error_message IN OUT VARCHAR2)
as
begin
    execute immediate 'begin ' || a_prc_exec_full_name || ' (:1,:2); end;' using a_raw_id,a_buf_id;
exception
    when others then
        if sqlcode = -20993 then
            a_processed_status := 3; -- unrepeatable status
        else
            a_processed_status := -3; -- processing happened with error
        end if;
        a_error_message := sqlerrm;
end;
$$

create or replace procedure ${schemaName}.prc_post_process(a_raw_id IN NUMBER, a_raw_full_name IN VARCHAR2,
                                                           a_processed_status IN NUMBER,
                                                           a_error_message IN VARCHAR2,
                                                           a_msg IN CLOB default NULL)
as
begin
    /* Executes after process ${versionStr} */
    if a_processed_status <> 0 then
        execute immediate 'update ' || a_raw_full_name ||
                          ' set (s_status,s_msg,s_date,s_action, s_counter, f_msg)=(select :1,:2,:3,:4,s_counter+1,:5 from dual) where id=:6'
            using a_processed_status,a_error_message,sysdate,case when a_processed_status = -3 then 0 else 1 end, a_msg, a_raw_id;
    end if;
end;
$$


create or replace procedure ${schemaName}.prc_start_task(a_group_tag VARCHAR2, a_meta_tag VARCHAR2,
                                                         a_raw_id NUMBER DEFAULT NULL,
                                                         a_f_group_id VARCHAR2 DEFAULT NULL,
                                                         a_msg IN CLOB default NULL,
                                                         a_param IN VARCHAR2 default null)
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
    l_raw_id             NUMBER(19);
    l_buf_id             NUMBER(19);
    l_attempt            NUMBER;
    l_counter            NUMBER;
begin
    /* Start processing ${versionStr} */
    select raw_full_name,
           buf_full_name,
           prc_exec_full_name,
           case
               when a_raw_id is null then
                   case
                       when a_f_group_id is null then
                           case extract(param, 'PARAM/ORDER/text()').getStringVal()
                               when 'FIFO' then 'select id, s_counter from ' || raw_full_name ||
                                                ' where s_action=0 order by s_date asc, id asc'
                               when 'LIFO' then 'select id, s_counter from ' || raw_full_name ||
                                                ' where s_action=0 order by s_date desc, id desc'
                               end
                       else
                           case extract(param, 'PARAM/ORDER/text()').getStringVal()
                               when 'FIFO' then 'select id, s_counter from ' || raw_full_name ||
                                                ' where s_action=0 and f_group_id=:1  order by s_date asc, id asc'
                               when 'LIFO' then 'select id, s_counter from ' || raw_full_name ||
                                                ' where s_action=0 and f_group_id=:1 order by s_date desc, id desc'
                               end
                       end
               end,
           nvl(extract(param, 'PARAM/ATTEMPT/text()').getNumberVal(), -1)
    into l_raw_full_name,l_buf_full_name,l_prc_exec_full_name,l_raw_loop_query,l_attempt
    from (select raw_full_name,
                 buf_full_name,
                 prc_exec_full_name,
                 XMLType(nvl(a_param, param)) param
          from ${schemaName}.BRIDGE_META_V
          where GROUP_tag = a_group_tag
            and META_TAG = a_meta_tag);


    if a_raw_id is null then
        if l_raw_loop_query is null then
            raise_application_error(-20802, 'Param ''ORDER'' is not defined');
        end if;
        if a_f_group_id is null then
            open c_raw_rec for l_raw_loop_query;
        else
            open c_raw_rec for l_raw_loop_query using a_f_group_id;
        end if;
    else
        open c_raw_rec for 'select id, s_counter from ' || l_raw_full_name ||
                           ' where s_action=0 and id=:1' using a_raw_id;
    end if;

    loop
        FETCH c_raw_rec INTO l_raw_id, l_counter;
        EXIT WHEN c_raw_rec%NOTFOUND;
        /* process the result row */
        l_error_message := NULL;
        l_processed_status := NULL;
        ${schemaName}.prc_pre_process(l_raw_id, l_raw_full_name, l_buf_full_name,
                                      a_f_group_id, l_processed_status, l_error_message, l_buf_id);


        if l_processed_status = 1 then
            ${schemaName}.prc_process(l_raw_id, l_buf_id, l_prc_exec_full_name, l_processed_status, l_error_message);
        end if;

        if l_attempt <> -1 and l_counter + 1 >= l_attempt and l_processed_status = -3 then
            l_processed_status := 3;
        end if;

        if l_processed_status <> 1 then
            rollback;
        end if;

        ${schemaName}.prc_post_process(l_raw_id, l_raw_full_name, l_processed_status, l_error_message, a_msg);

        if l_processed_status <> 0 then
            l_count := l_count + 1;
        end if;
        commit;
    end loop;
    close c_raw_rec;
    commit;
end;
$$


