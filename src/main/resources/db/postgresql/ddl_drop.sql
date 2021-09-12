drop function if exists ${schemaName}.fnc_raw_loop;
drop procedure if exists ${schemaName}.prc_create_meta_by_tag;
drop procedure if exists ${schemaName}.prc_start_task;
drop procedure if exists ${schemaName}.prc_process;
drop procedure if exists ${schemaName}.prc_pre_process;
drop procedure if exists ${schemaName}.prc_post_process;
drop procedure if exists ${schemaName}.prc_drop_meta_by_tag;

drop view if exists ${schemaName}.bridge_meta_v;
drop table if exists ${schemaName}.bridge_meta;
drop table if exists ${schemaName}.bridge_group;
