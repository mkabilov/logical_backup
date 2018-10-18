do
$_$
declare v_type smallint;
        v_value integer := random() * 10000;
begin
  v_type := random() * 2;

  if v_type = 0 then
    insert into test(id, value, hash) values (default, v_value, md5(v_value::text));
  elsif v_type = 1 then
    update test set value = v_value, hash = md5(v_value::text) where id = (select id from test order by random() limit 1);
  elsif v_type = 2 then
    delete from test where id = (select id from test order by random() limit 1);
  end if;
end;
$_$;