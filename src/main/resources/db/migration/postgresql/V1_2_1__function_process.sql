create function fnc_raw_loop(a_meta_data t_meta_data) returns refcursor
    language plpgsql
as
$$
DECLARE
    c_raw refcursor  := 'raw_cursor';
BEGIN
    OPEN c_raw FOR EXECUTE format('select id from %s where s_action=0 order by s_date desc, id desc',
                               a_meta_data.raw_full_name);
    RETURN c_raw;
END;
$$;
