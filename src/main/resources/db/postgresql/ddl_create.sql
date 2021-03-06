create schema if not exists ${schemaName};

create table ${schemaName}.bridge_group
(
    id          bigserial not null
        constraint bridge_group_pk
            primary key,
    tag         text      not null,
    note        text,
    schema_name text      not null,
    param       text      not null default '{
      "ORDER": "LIFO",
      "ATTEMPT": -1,
      "SKIP":0 }'
);

comment on table ${schemaName}.bridge_group is 'Contains groups descriptions ${versionStr}';

create unique index bridge_group_tag_uindex
    on ${schemaName}.bridge_group (tag);

create table ${schemaName}.bridge_meta
(
    id       bigserial not null
        constraint bridge_meta_pk
            primary key,
    tag      text      not null,
    note     text,
    param    text,
    group_id bigint    not null
        constraint bridge_meta_group_fk
            references ${schemaName}.bridge_group,
    constraint bridge_meta_uk_1
        unique (tag, group_id)
)
;
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
       'JSON' param_type
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
      FROM (SELECT bg.tag                       group_tag,
                   bm.tag                       meta_tag,
                   bm.group_id,
                   bm.id                        meta_id,
                   bg.schema_name,
                   'FBI_RAW_' || bm.tag  AS     raw_name,
                   'FBI_BUF_' || bm.tag  AS     buf_name,
                   'PRC_EXEC_' || bm.tag AS     prc_exec_name,
                   COALESCE(bm.param, bg.param) param
            FROM ${schemaName}.bridge_meta bm
                     JOIN ${schemaName}.bridge_group bg ON bg.id = bm.group_id) tt1) tt2;

comment on view ${schemaName}.bridge_meta_v is 'Meta data view ${versionStr}';

delimiter ++

create function ${schemaName}.fnc_raw_loop(raw_full_name TEXT) returns refcursor
    language plpgsql
as
$$
DECLARE
    c_raw refcursor := 'raw_cursor';
BEGIN
    /* Returns cursor for loop for the raw table ${versionStr} */

    OPEN c_raw FOR EXECUTE format('select id from %s where s_action=0 order by f_date desc, id desc',
                                  raw_full_name);
    RETURN c_raw;
END;
$$;

++

create procedure ${schemaName}.prc_create_meta_by_tag(a_group_tag text, a_meta_tag text,
                                                      a_schema_name text default null, a_param text default null)
    language plpgsql
as
$$
declare
    l_raw_full_name      TEXT;
    l_buf_full_name      TEXT;
    l_raw_name           TEXT;
    l_buf_name           TEXT;
    l_prc_exec_full_name TEXT;
    l_prc_exec_name      TEXT;
    l_group_id           BIGINT;
    l_meta_id            BIGINT;
    l_schemaName         TEXT;
begin
    /* Creates db objects ${versionStr} */

    begin
        select id into strict l_group_id from ${schemaName}.bridge_group where tag = a_group_tag;
    exception
        when no_data_found then
            insert into ${schemaName}.bridge_group (tag, schema_name)
            values (a_group_tag, COALESCE(a_schema_name, '${schemaName}'))
            returning id into l_group_id;
    end;

    begin
        select id into strict l_meta_id from ${schemaName}.bridge_meta where group_id = l_group_id and tag = a_meta_tag;
    exception
        when no_data_found then
            insert into ${schemaName}.bridge_meta (tag, group_id, param)
            values (a_meta_tag, l_group_id, a_param)
            returning id into l_meta_id;
    end;

    select raw_full_name, raw_name, buf_full_name, buf_name, prc_exec_full_name, prc_exec_name, schema_name
    into l_raw_full_name, l_raw_name, l_buf_full_name, l_buf_name, l_prc_exec_full_name, l_prc_exec_name, l_schemaName
    from ${schemaName}.bridge_meta_v
    where group_id = l_group_id
      and meta_id = l_meta_id;


    EXECUTE 'create schema if not exists ' || l_schemaName;


    /* RAW table creation */

    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || l_raw_full_name || ' (' || 'id BIGSERIAL PRIMARY KEY,' ||
            ' f_oper SMALLINT NOT NULL DEFAULT 0 ,' ||
            ' f_payload TEXT,' ||
            ' f_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),' ||
            ' s_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),' ||
            ' s_status SMALLINT NOT NULL DEFAULT 0,' ||
            ' s_action SMALLINT NOT NULL DEFAULT 0,' ||
            ' f_id TEXT NOT NULL,' ||
            ' s_msg TEXT,' ||
            ' f_msg TEXT,' ||
            ' s_counter INTEGER DEFAULT 0 NOT NULL,' ||
            ' f_group_id TEXT' ||
            ')';

    EXECUTE format(
            'COMMENT ON COLUMN %s.s_action is ''Current action. 0 - ready for processing, 1 - will not precessed''',
            l_raw_full_name);

    EXECUTE format('COMMENT ON COLUMN %s.f_id is ''foreign id''', l_raw_full_name);

    EXECUTE format('COMMENT ON COLUMN %s.s_msg is ''Error message''', l_raw_full_name);

    EXECUTE format('COMMENT ON COLUMN %s.s_counter is ''Count of the processing attempts''', l_raw_full_name);

    EXECUTE format(
            'COMMENT ON COLUMN %s.s_status is ''Status: 0: nothing happend; 1: successfull processing; -3: processing error (repeat); 4: filtered finally; -4: filtered (repeat); 5: skiped without repeat; 3: critical error''',
            l_raw_full_name);

    EXECUTE format('CREATE INDEX IF NOT EXISTS %s_index
        ON %s (s_action)', l_raw_name, l_raw_full_name);

    EXECUTE format('CREATE INDEX IF NOT EXISTS %s_g_index
        ON %s (f_group_id)', l_raw_name, l_raw_full_name);

    BEGIN
        EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s_oper_ch CHECK (f_oper IN (0,1))', l_raw_full_name,
                       l_raw_name);
    EXCEPTION
        WHEN duplicate_object THEN NULL;
    END;
    BEGIN
        EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s_action_ch CHECK (s_action IN (0,1))',
                       l_raw_full_name,
                       l_raw_name);
    EXCEPTION
        WHEN duplicate_object THEN NULL;
    END;
    BEGIN
        EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s_status_ch CHECK (s_action IN (0,1,3,4,5,-3,-4))',
                       l_raw_full_name, l_raw_name);
    EXCEPTION
        WHEN duplicate_object THEN NULL;
    END;
    /* BUF table creation */

    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || l_buf_full_name || ' (' || 'id BIGSERIAL PRIMARY KEY,' ||
            ' f_oper SMALLINT NOT NULL DEFAULT 0,' ||
            ' f_payload TEXT,' ||
            ' s_payload TEXT,' ||
            ' f_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),' ||
            ' s_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),' ||
            ' f_raw_id BIGINT NOT NULL,' ||
            ' f_id TEXT NOT NULL,' ||
            ' s_counter INTEGER DEFAULT 0 NOT NULL,' ||
            ' f_group_id TEXT' ||
            ')';

    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS ' || l_buf_name || '_f_id_index
        on ' || l_buf_full_name || ' (f_id)';

    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS ' || l_buf_name || '_raw_id_index
        on ' || l_buf_full_name || ' (f_raw_id)';

    EXECUTE 'CREATE INDEX IF NOT EXISTS ' || l_buf_name || '_g_index
        on ' || l_buf_full_name || ' (f_group_id)';

    BEGIN
        EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s_oper_ch CHECK (f_oper IN (0,1))', l_buf_full_name,
                       l_buf_name);
    EXCEPTION
        WHEN duplicate_object THEN NULL;
    END;


    /* Procedure creation */
    BEGIN
        EXECUTE format(
                $string$ create procedure %s(a_raw_id bigint, a_buf_id bigint)
                   language plpgsql
                   as
$f1$
begin
    null;
exception when others then
    raise exception '%s error : %% {buf.id=%%}', sqlerrm, a_buf_id USING ERRCODE=sqlstate;
end;
$f1$;
                   $string$, l_prc_exec_full_name, lower(l_prc_exec_name));
    EXCEPTION
        WHEN duplicate_function THEN NULL;
    END;


end
$$;

++


create procedure ${schemaName}.prc_drop_meta_by_tag(a_group_tag text, a_meta_tag text)
    language plpgsql
as
$$
declare
    l_raw_full_name      text;
    l_buf_full_name      text;
    l_prc_exec_full_name text;
    l_group_id           bigint;
    l_meta_count         integer;
begin
    /* Drops db objects ${versionStr} */
    begin
        select raw_full_name, buf_full_name, prc_exec_full_name, group_id
        into strict l_raw_full_name, l_buf_full_name, l_prc_exec_full_name,l_group_id
        from ${schemaName}.bridge_meta_v
        where group_tag = a_group_tag
          and meta_tag = a_meta_tag;
    exception
        when no_data_found then
            raise notice 'prc_drop_meta_by_tag : bridge meta not found {group_tag=%, meta_tag=%}', a_group_tag,a_meta_tag;
    end;

    IF NOT l_group_id IS NULL THEN

        EXECUTE 'DROP PROCEDURE IF EXISTS ' || l_prc_exec_full_name || ' (bigint,bigint)';
        EXECUTE 'DROP TABLE IF EXISTS ' || l_raw_full_name;
        EXECUTE 'DROP TABLE IF EXISTS ' || l_buf_full_name;

        delete from ${schemaName}.bridge_meta where group_id = l_group_id and tag = a_meta_tag;

        select count(*) into l_meta_count from ${schemaName}.bridge_meta where group_id = l_group_id;

        if l_meta_count = 0 then
            delete from ${schemaName}.bridge_meta where group_id = l_group_id;
        end if;
    END IF;
end
$$;

++

create procedure ${schemaName}.prc_pre_process(IN a_raw_id BIGINT, IN a_raw_full_name TEXT, IN a_buf_full_name TEXT,
                                               a_f_group_id TEXT, INOUT a_processed_status INTEGER,
                                               INOUT a_error_message TEXT, INOUT a_buf_id BIGINT)
    language plpgsql
as
$$
declare
    l_buf_id        bigint;
    l_raw_id        bigint;
    l_raw_f_id      text;
    l_raw_f_payload text;
    l_raw_f_date    timestamp with time zone;
    l_update_count  integer;
    l_count integer;
begin
    /* Executes before process ${versionStr} */

    a_buf_id := null;
    execute 'select id,f_id,f_payload,f_date from ' || a_raw_full_name ||
            ' where s_action=0 and id=$1 for update skip locked'
        using a_raw_id into l_raw_id,l_raw_f_id,l_raw_f_payload,l_raw_f_date;

    if not l_raw_id is null then
        execute 'WITH t AS  (  insert into ' || a_buf_full_name ||
                ' as t (f_raw_id,f_id, f_payload, f_date, s_counter, f_group_id) values ( $1, $2, $3, $4, 1, $5 ) on conflict (f_id) do ' ||
                'update set (f_raw_id, f_payload, f_date, s_counter, f_group_id) = ($1,$3,$4, t.s_counter+1, $5)  where $4>t.f_date OR ($4=t.f_date AND $1>=t.f_raw_id) returning  xmax,id
            )
        SELECT COUNT(*) AS update_count,
               max(id)
        FROM t'
            using l_raw_id,l_raw_f_id,l_raw_f_payload, l_raw_f_date, a_f_group_id into l_update_count, l_buf_id;

        if l_update_count > 0 then
            if a_processed_status = 1 then
                execute 'select count(*) from ' || a_raw_full_name ||
                        ' where id<>$1 and f_id=$2 and (f_date>$3 or (f_date=$3 and id>$1))'
                    into l_count
                    using l_raw_id,l_raw_f_id,l_raw_f_date;
                if l_count>0 then
                    a_processed_status := 5; -- Skipped
                end if;
            else
                a_processed_status := 1; -- Success
            end if;
            a_buf_id := l_buf_id;
        else
            a_processed_status := 5; -- Skipped
        end if;

        -- processing finished successfully
    else
        a_processed_status := 0; -- processing not happened. Omitted
    end if;
exception
    when others then
        if sqlstate = '20993' then
            a_processed_status := 3; -- unrepeatable status
        else
            a_processed_status := -3; -- processing happened with error
        end if;
        a_error_message := sqlerrm;
end;
$$;

++

create procedure ${schemaName}.prc_process(IN a_raw_id bigint, IN a_buf_id bigint,
                                           IN a_prc_exec_full_name text,
                                           INOUT a_processed_status integer,
                                           INOUT a_error_message text)
    language plpgsql
as
$$
begin
    execute 'call ' || a_prc_exec_full_name || '($1,$2)' using a_raw_id,a_buf_id;
exception
    when others then
        if sqlstate = '20993' then
            a_processed_status := 3; -- unrepeatable status
        else
            a_processed_status := -3; -- processing happened with error
        end if;
        a_error_message := sqlerrm;
end;
$$
    ++

create procedure ${schemaName}.prc_post_process(IN a_raw_id bigint, IN a_raw_full_name text,
                                                IN a_processed_status integer,
                                                IN a_error_message text, a_msg IN text default NULL)
    language plpgsql
as
$$
begin
    /* Executes after process ${versionStr} */

    if a_processed_status <> 0 then
        execute 'update ' || a_raw_full_name ||
                ' set (s_status,s_msg,s_date,s_action, s_counter, f_msg)=($1,$2,$3,$4,s_counter+1,$5) where id=$6'
            using a_processed_status,a_error_message,now(),case when a_processed_status = -3 then 0 else 1 end, a_msg, a_raw_id;
    end if;
end ;
$$;

++

create procedure ${schemaName}.prc_start_task(a_group_tag text, a_meta_tag text, a_raw_id bigint DEFAULT NULL::bigint,
                                              a_f_group_id text DEFAULT NULL, a_msg IN text default NULL,
                                              a_param IN text default null)
    language plpgsql
as
$$
declare
    c_raw_rec            record;
    l_count              integer := 0;
    l_error_message      text;
    l_processed_status   integer;
    l_raw_loop_query     text;
    l_raw_full_name      text;
    l_buf_full_name      text;
    l_prc_exec_full_name text;
    l_buf_id             bigint;
    l_attempt            integer;
    l_param_skip         integer;
begin
    /* Start processing ${versionStr} */

    select raw_full_name,
           buf_full_name,
           prc_exec_full_name,
           case
               when a_raw_id is null then
                   case
                       when a_f_group_id is null then
                           case
                               when param ->> 'ORDER' = 'FIFO' then 'select id,s_counter from ' || raw_full_name ||
                                                                    ' where s_action=0 order by s_date asc, id asc'
                               when param ->> 'ORDER' = 'LIFO' or param::JSONB ->> 'ORDER' is null THEN
                                           'select id,s_counter from ' || raw_full_name ||
                                           ' where s_action=0 order by s_date desc, id desc'
                               end
                       else
                           case
                               when param ->> 'ORDER' = 'FIFO' then 'select id,s_counter from ' || raw_full_name ||
                                                                    ' where s_action=0 and f_group_id=$2 order by s_date asc, id asc'
                               when param ->> 'ORDER' = 'LIFO' or param::JSONB ->> 'ORDER' is null THEN
                                           'select id,s_counter from ' || raw_full_name ||
                                           ' where s_action=0 and f_group_id=$2 order by s_date desc, id desc'
                               end
                       end end,
           coalesce((param ->> 'ATTEMPT')::INTEGER, -1),coalesce((param ->> 'SKIP')::INTEGER, -1)
    into l_raw_full_name,l_buf_full_name,l_prc_exec_full_name, l_raw_loop_query, l_attempt, l_param_skip
    from (select raw_full_name,
                 buf_full_name,
                 prc_exec_full_name,
                 COALESCE(a_param::JSONB, param::JSONB) param
          from ${schemaName}.BRIDGE_META_V
          where GROUP_tag = a_group_tag
            and META_TAG = a_meta_tag) tt;


    if a_raw_id is null then
        if l_raw_loop_query is null then
            raise exception 'Param ''ORDER'' is not defined';
        end if;
    else
        l_raw_loop_query := 'select id,s_counter from ' || l_raw_full_name || ' where s_action=0 and id=$1';
    end if;

    for c_raw_rec in execute l_raw_loop_query using a_raw_id, a_f_group_id
        loop
            /* process the result row */
            l_error_message := NULL;
            l_processed_status := l_param_skip;

            call ${schemaName}.prc_pre_process(c_raw_rec.id, l_raw_full_name, l_buf_full_name,
                                               a_f_group_id, l_processed_status, l_error_message, l_buf_id);

            if l_processed_status = 1 then
                call ${schemaName}.prc_process(c_raw_rec.id, l_buf_id, l_prc_exec_full_name, l_processed_status,
                                               l_error_message);
            end if;


            if l_attempt <> -1 and c_raw_rec.s_counter + 1 >= l_attempt and l_processed_status = -3 then
                l_processed_status := 3;
            end if;

            if l_processed_status <> 1 then
                rollback;
            end if;

            call ${schemaName}.prc_post_process(c_raw_rec.id, l_raw_full_name, l_processed_status, l_error_message,
                                                a_msg);

            if l_processed_status <> 0 then
                l_count := l_count + 1;
            end if;
            commit;

        end loop;
    commit;
end ;
$$;




