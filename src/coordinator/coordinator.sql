-- 每次需要改： 1. 函数名 2. 调用tableudf名	3. 产生msg的表名vertexSendMsg

CREATE OR REPLACE FUNCTION graphh_shortpath(iter integer)
RETURNS character varying AS
$BODY$
declare
	v_number int;		--记录迭代次数
	v_count int;		--临时变量，一些sql计算结果
	v_active int;		--标记是否含v_update表，从而确定需要使用哪张表来产生msg。
	sql text;			--sql执行语句
	vertexSendMsg text;	--表示生成msg的表，需要用户自己去指定（或者写到配置文件中，需要设定，不能依靠是否存在v_update表）
	v_msg int;			--是否包含msg表，确定是直接使用msg表，还是使用子查询生成
	v_tableudf text;	--需要调用的tableudf的名称
begin
	-- 表示需要调用的tableudf的名称
	v_tableudf := 'shortpath' || '_tableudf';	
	--用户指定使用哪张表来发送消息，含v_update表与vertex两种选择
	vertexSendMsg := 'v_update';	
	
	v_number := 1;
	while v_number <= iter loop
		RAISE NOTICE 'iter here is %', v_number;
		
		v_msg := 1;
		if not exists(select * from information_schema.tables where table_catalog = CURRENT_CATALOG and table_schema = CURRENT_SCHEMA
			and table_name = 'msg') then
			v_msg := 0;
		end if;	
		
		sql := ' ';
		--没有msg表，自己生成	-- 其中graphh_append的使用，要将参数换成字符串形式
		if v_msg=0 then
			sql := sql || 'CREATE TABLE v_prime_ AS(';
			sql := sql || '	SELECT vertex.flag AS flag, ' || v_tableudf || ' ( ' || v_number || ',vertex.value,new_msg.value) AS value FROM (';
			sql := sql || '		SELECT edge.to_node AS to_node, graphh_append(''('' || (vertex.value).id  || '',''|| (vertex.value).value || '',''|| (vertex.value).other || '')'') AS value FROM ' || vertexSendMsg || ' AS vertex,edge WHERE vertex.flag=edge.from_node ';
			sql := sql || '		GROUP BY edge.to_node ';
			sql := sql || '    	) AS new_msg LEFT JOIN vertex ';
			sql := sql || '    ON vertex.flag=new_msg.to_node ';
			sql := sql || ')DISTRIBUTED BY (flag); ';
		--存在上一轮返回的单点发送的msg表，那么将其group by一下，使用graphh_append函数聚集起每个顶点的所有messages
		else
			sql := sql || 'CREATE TABLE v_prime_ AS( ';
			sql := sql || '    SELECT vertex.flag AS flag, ' || v_tableudf || '(' || v_number || ',vertex.value,new_msg.value) AS value FROM (';
			sql := sql || '		SELECT to_node, graphh_append(''('' || (value).id  || '',''|| (value).value || '',''|| (value).other || '')'') AS value FROM msg GROUP BY msg.to_node ';
			sql := sql || '		) AS new_msg LEFT JOIN vertex ';
			sql := sql || '    ON vertex.flag=new_msg.to_node ';
			sql := sql || ')DISTRIBUTED BY (flag);';
		end if;
		execute sql; sql := ' ';
		
		--vertex(flag,graphh_value)
		--msg(to_node,value)
		--v_prime_(flag,value);   其中flag表示为to_node		value(flag,to_node,graphh_value);	外层的flag = to_node
		sql := sql || 'DROP TABLE IF EXISTS msg;';
		sql := sql || 'CREATE TABLE msg AS( ';
		sql := sql || '	SELECT (value).to_node AS to_node, (v_prime_.value).value AS value from v_prime_ where (value).flag=-1 ';
		sql := sql || ')DISTRIBUTED BY(to_node);';
		execute sql; sql := ' ';

		sql := sql || 'CREATE TABLE v_prime AS(';
		sql := sql || '	SELECT (v_prime_.value).flag AS flag,(v_prime_.value).value AS value FROM v_prime_ where (value).flag>=0 ';
		sql := sql || ')DISTRIBUTED BY(flag);';
		execute sql; sql := ' ';

		--当没有点被更新时，表示收敛 跳出迭代
		execute 'SELECT COUNT(*) FROM v_prime' into v_count;
		if v_count=0 then 
			execute 'SELECT COUNT(*) FROM msg;' into v_count;
			if v_count=0 then
				return 'songjs: ' || v_number;
			end if;
		end if;

		--对于生成的msg表，如果没有任何消息，那么DROP掉，防止下一轮迭代时会使用msg表，而不自己去重新生成
		execute 'SELECT COUNT(*) from msg' into v_msg;
		if v_msg=0 then 
			execute 'DROP TABLE msg;';
		end if;
		--将v_prime表中的内容合并到之前的顶点表上
		sql := sql || 'DROP TABLE IF EXISTS vertex_prime;';
		sql := sql || 'CREATE TABLE vertex_prime AS(';
		sql := sql || '	SELECT vertex.flag AS flag , COALESCE(v_prime.value, vertex.value) AS value ';
		sql := sql || '    FROM vertex LEFT JOIN v_prime ON vertex.flag = v_prime.flag ';
		sql := sql || ')DISTRIBUTED BY(flag);';
		sql := sql || 'DROP TABLE IF EXISTS vertex;';
		sql := sql || 'ALTER TABLE vertex_prime RENAME TO vertex;';
		execute sql; sql := ' ';

		--新生成的v_update表用不用依据配置文件有说明，不用的话不管它就好了
		sql := sql || 'DROP TABLE IF EXISTS v_update; Alter TABLE v_prime RENAME TO v_update;drop table v_prime_;';
		execute sql; sql := ' ';



		v_number := v_number+1;
	end loop;
	return 'complement!' || v_number;
end;
$BODY$
LANGUAGE plpgsql STRICT;








