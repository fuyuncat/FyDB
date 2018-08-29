select log(256,65536) from dual

select * from user_tab_cols where table_name = 'T_TEST2'

select "*" from t_test1

select count(*) from t_test2 where (OWNER = 'DEMO' and STATUS = 'VALID') or TABLESPACE_NAME!='AAA';

select count(*) from t_test2 where (OWNER = 'DEMO' and STATUS = 'VALID') or AVG_ROW_LEN < 30;


select count(*) from t_test2 
where (OWNER = 'BBS' and STATUS = 'VALID') 
   or (LAST_ANALYZED >= TO_DATE('2007-03-01 13:15:00','YYYY-MM-DD HH24:MI:SS') and LAST_ANALYZED < TO_DATE('2007-03-15 22:00:00','YYYY-MM-DD HH24:MI:SS'));

select * from t_test2 
where (OWNER = 'BBS' and STATUS = 'VALID') 
   or (LAST_ANALYZED >= TO_DATE('2007-03-01 13:15:00','YYYY-MM-DD HH24:MI:SS') and LAST_ANALYZED <= TO_DATE('2007-03-15 22:00:16','YYYY-MM-DD HH24:MI:SS'));

select * from t_test2 where LAST_ANALYZED > TO_DATE('2007-03-01 13:15:00','YYYY-MM-DD HH24:MI:SS') and LAST_ANALYZED <= TO_DATE('2007-03-15 22:00:00','YYYY-MM-DD HH24:MI:SS');

select * from t_test2 where LAST_ANALYZED >= TO_DATE('2007-03-01 13:15:00','YYYY-MM-DD HH24:MI:SS') and LAST_ANALYZED < TO_DATE('2007-03-15 22:01:00','YYYY-MM-DD HH24:MI:SS');
select * from t_test2 where LAST_ANALYZED < TO_DATE('2007-03-01 13:15:00','YYYY-MM-DD HH24:MI:SS');

select * from t_test2 where LAST_ANALYZED = TO_DATE('2007-03-15 22:00:15','YYYY-MM-DD HH24:MI:SS') and table_name >= 'M7B';

select /*+index(t T_TEST2_IDX1)*/* from t_test2 t where (status != 'INVALID' or tablespace_name > 'RING') and pct_free = 1000 and table_name = 'T_TEST1'

select /*+index(t T_TEST2_IDX1)*/* from t_test2 t 
where ((owner='DEMO' and (pct_free = 0 or ini_trans = 1)) and (STATUS='VALID' or INITIAL_EXTent = 7)) or (TABLE_NAME='T_TEST1' or TABLESPACE_NAME='RING');

select /*+index(t T_TEST2_NULL)*/* from t_test2 t 
where ((owner='DEMO' and (pct_free = 0 or ini_trans = 1)) or (STATUS='VALID' or INITIAL_EXTent = 7)) and (NUM_ROWS=0 and TABLESPACE_NAME='RING');

select /*+index(t T_TEST2_NULL)*/count(*) from t_test2 t 
where ((owner='DEMO' and (pct_free = 0 or ini_trans = 1)) or (STATUS='VALID' or INITIAL_EXTent = 7)) and (NUM_ROWS=0 and TABLESPACE_NAME='RING');

select /*+index(t T_TEST2_NULL)*/count(*) from t_test2 t 
where ((owner='DEMO' and (pct_free = 0 or ini_trans = 1)) or (STATUS='VALID' or INITIAL_EXTent = 7)) and (TABLESPACE_NAME='RING');

((owner='DEMO' and (pct_free = 0 or ini_trans = 1)) and (STATUS='VALID' or INITIAL_EXTent = 7)) and (TABLE_NAME='T_TEST1' or TABLESPACE_NAME='RING')

(("TABLESPACE_NAME"=:2 OR "TABLE_NAME"=:1) AND ("CLUSTER_NAME"=:4 OR "IOT_NAME"=:5) AND ("STATUS"=:6 OR "PCT_FREE"=TO_NUMBER(:7)))

select count(*) from t_test2 where TABLESPACE_NAME!='AAA'
select * from t_test2 where OWNER = 'DEMO' and STATUS = 'VALID'
select count(*) from t_test2 where STATUS = 'VALID'

select count(*) from t_test2 where AVG_ROW_LEN < 30

"9/22/2006 10:00:10 "


create public synonym parse_predtree for sys.parse_predtree;
create public synonym parse_predtree_idx for sys.parse_predtree_idx;

grant execute on parse_predtree to demo
grant execute on parse_predtree_idx to demo

select upper(substr('((owner =:1 or owner = :2) and (table_name=:3 or tablespace_name > :4)) and status != :5', 1)), regexp_instr(upper(substr('((owner =:1 or owner = :2) and (table_name=:3 or tablespace_name > :4)) and status != :5', 1)), '\s+OR\s+') from dual;

declare
  typsel number;
  sel number;
  str varchar2(32767) := '((owner =:1 or owner = :2) and (table_name=:3 or tablespace_name > :4)) and status != :5';
begin
     str := '("OBJECT_ID"=TO_NUMBER(:1) OR "OBJECT_ID"=TO_NUMBER(:2)) OR ("OBJECT_NAME"=:3 OR "OBJECT_NAME"=:4) AND "OBJECT_ID">TO_NUMBER(:5) OR OWNER=:6';
     parse_predtree(str, 'demo', 't_test2', sel, typsel);
     --pred_calculator(str, 'demo', 't_test2', sel, typsel);
end;

declare
  --p varchar2(32767) := '("OBJECT_ID"=TO_NUMBER OR "OBJECT_ID"=TO_NUMBER) OR ("OBJECT_NAME">:5 AND "OBJECT_NAME"<:6) AND ("OBJECT_NAME"=:3 OR "OBJECT_NAME"=:4) AND "OWNER"=:7';
  --p varchar2(32767) := '(("OBJECT_ID" = TO_NUMBER1 OR ("OBJECT_ID" = TO_NUMBER2 OR "OBJECT_NAME">:5) AND "OBJECT_NAME"<:6) AND ("OBJECT_NAME"=:3 OR "OBJECT_NAME"=:4)) AND "OWNER"=:7';
  --p varchar2(32767) := ' "OBJECT_ID" = TO_NUMBER1 ( :1, :2 ) OR "OBJECT_ID" = TO_NUMBER2 ( :3 ) OR "OBJECT_NAME"> :5 AND "OBJECT_NAME"< :6 AND ( "OBJECT_NAME"=:3 OR "OBJECT_NAME"= :4 AND "OWNER"= :7 )';
  --p varchar2(32767) := '"CREATED" = :1';
  p varchar2(32767) := '(("owner" =:1 or "owner" = :2) and ("table_name"=:3 or "tablespace_name" > :4)) and "status" != :5';
  --p varchar2(32767) := '"CREATED" = :5';
  --p varchar2(32767) := '(("OWNER" =:1 OR "OWNER" = :2) AND ("TABLE_NAME"=:3 OR "TABLESPACE_NAME" > :4)) AND "STATUS" != :5';
  v_sel number;
  v_typsel number;
  v_oricard number:=47582;
begin
  --parse_pred.parse(p,'DEMO','T_TEST2',v_sel,v_typsel);
  parse_predtree(p,'DEMO','T_TEST2',v_sel,v_typsel);
  --parse_pred.print(p);
  --dbms_output.put_line(to_char(v_oricard*v_sel));
end;


select * from table(parse_pred.get_tree('(("owner" =:1 or "owner" = :2) and ("table_name"=:3 or "tablespace_name" > :4)) and "status" != :5'))
select parse_pred.get_tree('(("owner" =:1 or "owner" = :2) and ("table_name"=:3 or "tablespace_name" > :4)) and "status" != :5') from dual


create type id_list_type is table of pls_integer;
create   type node_type is record (id pls_integer, predstr varchar2(32767), ntype pls_integer, pid pls_integer, cidlist id_list_type);
create or replace type node_list_type is table of parse_pred.node_type;

drop type id_list_type;
drop   type node_type;
drop  type node_list_type;


select regexp_instr('((owner ="AA" or owner ="ad") and (table_name="asdfas" or tablespace_name> "dadsa")) and status != :5', '([A-Za-z]+[A-Za-z0-9$_]*)\s*(=|!=|<>|>|<|>=|<=)', 1) from dual

