create function fnc_get_group_by_tag(a_group_tag text) returns bigint
    language plpgsql
as
$$
declare
    l_group_id bigint;
begin
    select id into strict l_group_id from bridge_group where tag = a_group_tag;
    return l_group_id;
exception
    when no_data_found then
        raise exception 'Group not found (get_group_by_tag) {group_tag=%}' , a_group_tag;
end;
$$;

create function fnc_get_group_schema(a_group_id bigint) returns text
    language plpgsql
as
$$
declare
    l_schema_name text;
begin
    select schema_name into strict l_schema_name from bridge_group where id = a_group_id;
    return l_schema_name;
exception
    when no_data_found then
        raise exception 'Schema not found (get_group_schema) {group_id=%}' , a_group_id;
end;
$$;

create function fnc_get_meta_by_tag(a_group_id bigint, a_meta_tag text) returns bigint
    language plpgsql
as
$$
declare
    l_meta_id bigint;
begin
    select id into strict l_meta_id from bridge_meta where tag = a_meta_tag and group_id = a_group_id;
    return l_meta_id;
exception
    when no_data_found then
        raise exception 'Meta not found (get_meta_by_tag) {group_id=%, meta_tag=%}' , a_group_id, a_meta_tag;
end;
$$;

create function fnc_get_meta_by_tag(a_group_tag text, a_meta_tag text) returns bigint
    language plpgsql
as
$$
declare
    l_group_id bigint;
    l_meta_id  bigint;
begin
    l_group_id := fnc_get_group_by_tag(a_group_tag);
    select id into strict l_meta_id from bridge_meta where tag = a_meta_tag and group_id = l_group_id;
    return l_meta_id;
exception
    when no_data_found then
        raise exception 'Meta not found (get_meta_by_tag) {group_tag=%, meta_tag=%}' , a_group_tag, a_meta_tag;
end;
$$;


create function fnc_get_meta_data(IN a_group_tag text,IN a_meta_tag text, OUT a_return t_meta_data)
    language plpgsql
as
$$
DECLARE
    l_meta_data t_meta_data;
BEGIN
    l_meta_data.group_id := fnc_get_group_by_tag(a_group_tag);
    l_meta_data.meta_id := fnc_get_meta_by_tag(l_meta_data.group_id, a_meta_tag);
    l_meta_data.schema_name := fnc_get_group_schema(l_meta_data.group_id);

    l_meta_data.raw_name := 'FBI_RAW_' || a_meta_tag;
    l_meta_data.buf_name := 'FBI_BUF_' || a_meta_tag;

    l_meta_data.raw_full_name := l_meta_data.schema_name || '.' || l_meta_data.raw_name;
    l_meta_data.buf_full_name := l_meta_data.schema_name || '.' || l_meta_data.buf_name;

    l_meta_data.prc_exec_name := 'PRC_EXEC_' || a_meta_tag;
    l_meta_data.prc_exec_full_name := l_meta_data.schema_name || '.' || l_meta_data.prc_exec_name;

    l_meta_data.raw_loop_query := format('select id from %s where s_action=0 order by s_date desc, id desc',
                                         l_meta_data.raw_full_name);
    a_return:= l_meta_data;
END;
$$;
