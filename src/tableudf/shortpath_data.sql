-- 最短路测试数据
insert into vertex(flag,value) values(1,'(1,0,0)');
insert into vertex(flag,value) values(2,'(2,100,0)');
insert into vertex(flag,value) values(3,'(3,100,0)');
insert into vertex(flag,value) values(4,'(4,100,0)');
insert into vertex(flag,value) values(5,'(5,100,0)');
insert into vertex(flag,value) values(6,'(6,100,0)');

insert into edge(from_node,to_node) values(1,4);
insert into edge(from_node,to_node) values(4,2);
insert into edge(from_node,to_node) values(4,3);
insert into edge(from_node,to_node) values(2,6);
insert into edge(from_node,to_node) values(3,6);
