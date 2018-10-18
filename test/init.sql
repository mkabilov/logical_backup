drop database if exists lb_test1;
drop database if exists lb_test2;

create database lb_test1;
create database lb_test2;

\c lb_test1
create table test(
   id serial not null primary key,
   value integer not null,
   hash text not null
);

\c lb_test2
create table test(
  id serial not null primary key,
  value integer not null,
  hash text not null
);

