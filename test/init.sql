drop database if exists lb_test_src;
drop database if exists lb_test_dst;

create database lb_test_src;
create database lb_test_dst;

\c lb_test_src
create table test(
   id serial not null primary key,
   value integer not null,
   hash text not null
);

create table test2(
   id serial not null primary key,
   value integer not null,
   hash text not null
);


\c lb_test_dst
create table test(
  id serial not null primary key,
  value integer not null,
  hash text not null
);

create table test2(
  id serial not null primary key,
  value integer not null,
  hash text not null
);
