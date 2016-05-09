insert into vertex(flag,value) values(1,'(1,0.15,2)');
insert into vertex(flag,value) values(2,'(2,0.15,3)');
insert into vertex(flag,value) values(3,'(3,0.15,1)');
insert into vertex(flag,value) values(4,'(4,0.15,5)');

insert into edge(from_node,to_node) values(1,2);
insert into edge(from_node,to_node) values(1,3);
insert into edge(from_node,to_node) values(1,4);
insert into edge(from_node,to_node) values(2,3);
insert into edge(from_node,to_node) values(4,2);
