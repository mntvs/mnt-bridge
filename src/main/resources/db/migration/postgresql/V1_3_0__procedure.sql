create procedure prc_create_meta_by_tag(a_group_tag text, a_meta_tag text)
    language plpgsql
as
$$
declare
    l_meta_data t_meta_data;
begin
    l_meta_data := fnc_get_meta_data(a_group_tag, a_meta_tag);

    /* RAW table creation */

    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || l_meta_data.raw_full_name || ' (' || 'id BIGSERIAL PRIMARY KEY,' ||
            ' f_oper SMALLINT NOT NULL DEFAULT 0 ,' ||
            ' f_payload TEXT,' ||
            ' f_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),' ||
            ' s_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),' ||
            ' s_status SMALLINT NOT NULL DEFAULT 0,' ||
            ' s_action SMALLINT NOT NULL DEFAULT 0,' ||
            ' f_id TEXT NOT NULL,' ||
            ' s_msg TEXT,' ||
            ' s_counter INTEGER DEFAULT 0 NOT NULL' ||
            ')';

    EXECUTE format(
            'COMMENT ON COLUMN %s.s_action is ''Current action. 0 - ready for processing, 1 - will not precessed''',
            l_meta_data.raw_full_name);

    EXECUTE format('COMMENT ON COLUMN %s.f_id is ''foreign id''', l_meta_data.raw_full_name);

    EXECUTE format('COMMENT ON COLUMN %s.s_msg is ''Error message''', l_meta_data.raw_full_name);

    EXECUTE format('COMMENT ON COLUMN %s.s_counter is ''Count of the processing attempts''', l_meta_data.raw_full_name);

    EXECUTE format(
            'COMMENT ON COLUMN %s.s_status is ''Status: 0: nothing happend; 1: successfull processing; -3: processing error (repeat); 4: filtered finally; -4: filtered (repeat); 5: skiped without repeat; 3: critical error''',
            l_meta_data.raw_full_name);

    EXECUTE format('CREATE INDEX IF NOT EXISTS %s_index
        ON %s (s_action)', l_meta_data.raw_name, l_meta_data.raw_full_name);

    BEGIN
        EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s_oper_ch CHECK (f_oper IN (0,1))', l_meta_data.raw_full_name,
                       l_meta_data.raw_name);
    EXCEPTION
        WHEN duplicate_object THEN NULL;
    END;
    BEGIN
        EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s_action_ch CHECK (s_action IN (0,1))',
                       l_meta_data.raw_full_name,
                       l_meta_data.raw_name);
    EXCEPTION
        WHEN duplicate_object THEN NULL;
    END;
    BEGIN
        EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s_status_ch CHECK (s_action IN (0,1,3,4,5,-3,-4))',
                       l_meta_data.raw_full_name, l_meta_data.raw_name);
    EXCEPTION
        WHEN duplicate_object THEN NULL;
    END;
    /* BUF table creation */

    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || l_meta_data.buf_full_name || ' (' || 'id BIGSERIAL PRIMARY KEY,' ||
            ' f_oper SMALLINT NOT NULL DEFAULT 0,' ||
            ' f_payload TEXT,' ||
            ' s_payload TEXT,' ||
            ' f_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),' ||
            ' s_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),' ||
            ' f_raw_id BIGINT NOT NULL,' ||
            ' f_id TEXT NOT NULL' ||
            ')';

    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS ' || l_meta_data.buf_name || '_f_id_index
        on ' || l_meta_data.buf_full_name || ' (f_id)';


    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS ' || l_meta_data.buf_name || '_raw_id_index
        on ' || l_meta_data.buf_full_name || ' (f_raw_id)';

    BEGIN
        EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s_oper_ch CHECK (f_oper IN (0,1))', l_meta_data.buf_full_name,
                       l_meta_data.buf_name);
    EXCEPTION
        WHEN duplicate_object THEN NULL;
    END;


    /* Procedure creation */
    BEGIN
        EXECUTE format(
                $string$ create procedure %s(a_buf_id bigint, a_action_tag text)
                   language plpgsql
                   as
$f1$
begin
    null;
exception when others then
    raise exception '%s error : %% {buf.id=%%, action_tag=%%}', sqlerrm, a_buf.id,a_action_tag;
end;
$f1$;
                   $string$, l_meta_data.prc_exec_full_name, lower(l_meta_data.prc_exec_name));
    EXCEPTION
        WHEN duplicate_function THEN NULL;
    END;


end
$$;


create procedure prc_drop_meta_by_tag(a_group_tag text, a_meta_tag text)
    language plpgsql
as
$$
declare
    l_meta_data t_meta_data;
begin
    l_meta_data := fnc_get_meta_data(a_group_tag, a_meta_tag);
    EXECUTE 'DROP PROCEDURE IF EXISTS ' || l_meta_data.prc_exec_full_name || ' (bigint,text)';
    EXECUTE 'DROP TABLE IF EXISTS ' || l_meta_data.raw_full_name;
    EXECUTE 'DROP TABLE IF EXISTS ' || l_meta_data.buf_full_name;
end
$$;
