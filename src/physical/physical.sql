
--create database graphh;

create type graphh_value AS(id int,value float8,other int);
create type vertex_ AS(flag int,to_node int,value graphh_value);
create table vertex(flag int,value graphh_value) distributed by (flag);
create table edge(from_node int,to_node int) distributed by (from_node);

-- 后续版本考虑加入以顶点为中心算法模型中的active设计考虑，需要加入v_update表来模拟顶点活跃
-- create table v_update(flag int,value graphh_value) distributed by (flag);   


