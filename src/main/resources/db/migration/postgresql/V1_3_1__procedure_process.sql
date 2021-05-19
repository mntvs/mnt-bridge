create procedure prc_pre_process(IN a_raw_id bigint, IN a_raw_full_name TEXT,IN a_buf_full_name TEXT,a_prc_exec_full_name TEXT, INOUT a_processed_status integer, INOUT a_error_message text)
    language plpgsql
as
$$
declare
    l_action_tag   text;
    l_buf_model    fbi_buf_model;
    l_raw_model    fbi_raw_model;
    l_buf_id       bigint;
    l_update_count integer;
begin
    execute 'select * from ' || a_raw_full_name ||
            ' where s_action=0 and id=$1 for update skip locked'
        using a_raw_id into l_raw_model;

    if not l_raw_model is null then
        execute 'select id,f_date from ' || a_buf_full_name ||
                ' where f_id=$1 for update' using l_raw_model.f_id into l_buf_model.id,l_buf_model.f_date;

        if l_buf_model.id is null or l_buf_model.f_date <= l_raw_model.f_date then
            execute 'WITH t AS  (  insert into ' || a_buf_full_name ||
                    ' as t (f_raw_id,f_id, f_payload, f_date) values ( $1, $2, $3, $4 ) on conflict (f_id) do ' ||
                    'update set (f_raw_id, f_payload, f_date) = ($1,$3,$4)  where t.f_date<$4 returning  xmax,id,t.f_date
                )
            SELECT COUNT(*) AS update_count,
                   case when SUM(xmax::text::int) > 0 then ''U'' else ''I'' end,
                   max(id), max(f_date)
            FROM t'
                using l_raw_model.id,l_raw_model.f_id,l_raw_model.f_payload::jsonb, l_raw_model.f_date into l_update_count, l_action_tag, l_buf_model.id,l_buf_model.f_date;
            raise notice 'action_tag=%',l_action_tag;

            if l_action_tag = 'U' OR l_action_tag = 'D' OR
               (l_action_tag = 'I' AND NOT l_raw_model.f_date IS NULL) then
                execute 'call ' || a_prc_exec_full_name || ' ($1,$2)' using l_buf_id,l_action_tag;
                l_raw_model.s_status := 1; -- Success
            else
                l_raw_model.s_status := 5; -- Skiped
            end if;
        else
            l_raw_model.s_status := 5; -- Skiped
        end if;

        a_processed_status := l_raw_model.s_status;
        -- processing finished successfully
        /*execute 'update ' || a_meta_data.raw_full_name ||
                ' set (s_status,s_msg,s_date,s_action, s_counter)=($1,$2,$3,$4,s_counter+1) where id=$5' using 1,null,now(),1,a_raw_id;
        */
    else
        a_processed_status := 0; -- processing not happend. Ommited
    end if;
exception
    when others then
        a_processed_status := -3; -- processing happend with error
        a_error_message := sqlerrm;
    /*  execute 'update ' || a_meta_data.raw_full_name ||
              ' set (s_status,s_msg,s_counter) = ($1,$2,s_counter+1) where id=$3' using -3,sqlerrm,a_raw_id;*/

end;
$$;

create procedure prc_post_process(IN a_raw_id bigint,IN a_raw_full_name text,IN a_processed_status integer,IN a_error_message text)
    language plpgsql
as
$$
begin
    if a_processed_status<>0 then
        execute 'update ' || a_raw_full_name ||
                ' set (s_status,s_msg,s_date,s_action, s_counter)=($1,$2,$3,$4,s_counter+1) where id=$5' using a_processed_status,a_error_message,now(),case when a_processed_status<0 then 0 else 1 end, a_raw_id;
    end if;
end ;
$$;

create procedure prc_start_task(a_group_tag text, a_meta_tag text, a_raw_id bigint DEFAULT NULL::bigint)
    language plpgsql
as
$$
declare
    l_meta_data        t_meta_data;
    c_raw_rec          record;
    l_count            integer := 0;
    l_error_message    text;
    l_processed_status integer;
begin

    l_meta_data := fnc_get_meta_data(a_group_tag, a_meta_tag);

    for c_raw_rec in execute l_meta_data.raw_loop_query
        loop
            /* process the result row */
            call prc_pre_process(c_raw_rec.id, l_meta_data.raw_full_name,l_meta_data.buf_full_name,l_meta_data.prc_exec_full_name, l_processed_status, l_error_message);

            call prc_post_process(c_raw_rec.id, l_meta_data.raw_full_name, l_processed_status, l_error_message);

            if l_processed_status <> 0 then
                l_count := l_count + 1;
            end if;
            if l_count % 100 = 0 then
                commit;
            end if;
        end loop;
    commit;
end ;
$$;
