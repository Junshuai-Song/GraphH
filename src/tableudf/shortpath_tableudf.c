#include "postgres.h"
#include "executor/executor.h"
#include "funcapi.h"
#include <string.h>
#include "stdio.h"
#include "stdlib.h"
#include "fmgr.h"
#include "utils/geo_decls.h"
#include "utils/builtins.h"


PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(shortpath_tableudf);
PG_FUNCTION_INFO_V1(graphh_state);
PG_FUNCTION_INFO_V1(graphh_merge);

/*
 
 CREATE OR REPLACE FUNCTION shortpath_tableudf(int, graphh_value,TEXT)
 RETURNS setof vertex_ AS
 '/data/workspace/hawq/lib/postgresql/shortpath_tableudf', 'shortpath_tableudf'
 LANGUAGE c STRICT;
 
 */

typedef struct LNode{
	int flag;
	int to_node;
	int id;
	double value;
	int other;
}LNode;
#define Message LNode
#define Vertex LNode



Vertex vertex;			// 旧的顶点值，在compute函数过后，可能会生成新的顶点值
char *all_msgs;			// 所有消息的聚集字符串
int msgReceiveNum = 0;	// 接收消息的数量，由all_msgs解析的结果而定
LNode *msgs;			// 所有消息解析保存至此数组中
int msgSendNum = 0;		// 发送单点消息个数
int ansLen = 0;    		// 结果长度
LNode *ans;				// 保存当前顶点新生成的顶点值以及可能发送的单点消息
FuncCallContext     *funcctx;

/* 辅助initialMsg函数的处理 */
void addMsg(char *str,int number,int sum){
    int a=0,c=0; float8 b=0.0;int level=0;
    int n_number=0;

    int length = strlen(str);
    char ans2[30];
    int i=0; int start=0; int flag=0;
    int num=0;
    while(i<length){
        if(str[i]==','){
            strncpy(ans2,str+start,num);
            start = i+1;

            if(level==0){
                a = atoi(ans2); level++;
            }else if(level == 1){
                if(flag==1){
                    b = atof(ans2); level++;
                }else{
                    b = atoi(ans2); level++;
                }
            }else{
                c = atoi(ans2);
            }
            n_number++;
            flag=0;
            num =0;
        }
        num++;
        if(str[i]=='.') flag=1;
        i++;
    }
    strncpy(ans2,str+start,num);
    c = atoi(ans2);
    
    msgs[number].id = a;
    msgs[number].value = b;
    msgs[number].other = c;
}

/* 处理message字符串：将聚集的msg分开保存至一个msg数组中 */
void initialMessages(char *v_all_msgs){
	all_msgs = v_all_msgs;
	int tot=1;
	int length = strlen(all_msgs);
	for(int i=0;i<length;i++)
		if(all_msgs[i]=='(') tot++;
	msgs = (Message *) palloc((tot+1) * sizeof(LNode));

	// all_msgs的处理
	int tot_number=0;
	int i=0; int sum=1; int start=0;
	while(i<length){
		if(all_msgs[i]=='('){
			sum=0;
			start = i+1;
			i++;
			continue;
		}
		if(all_msgs[i]==')'){
			char *str = (char *) palloc(sum * sizeof(char));
			strncpy(str,(all_msgs+start),sum);
			addMsg(str,tot_number,sum);
			tot_number++;
			pfree(str);
		}
		sum++;i++;
	}
	msgReceiveNum = tot_number;
}
void initVertexValue(Vertex v_vertex){
	vertex = v_vertex;
}
Vertex getVertexValue(){
	return vertex;
}
Message* getMessages(){
	return msgs;
}
int getMsgReceiveNum(){ return msgReceiveNum; }
void modifyVertexValue(Vertex v_vertex){
	//直接将更改的顶点value放到ans数组中
	v_vertex.flag=v_vertex.id;
	v_vertex.to_node = -1;
	ans[ansLen] = v_vertex;
	ansLen++;
}
void sendMessage(int to_node, Message v_msg){
	v_msg.flag=-1;
	v_msg.to_node = to_node;
	//ans[ansLen] = v_msg;
	ansLen = ansLen + 1;
}
void setMsgSendNum(int v_msgSendNum){
	msgSendNum = v_msgSendNum;
	v_msgSendNum++;		//这里的+1指多加一个新顶点value值
	funcctx->max_calls = v_msgSendNum;
	ansLen = 0;
	ans = (LNode *) palloc((2) * sizeof(LNode));
}
int getMsgSendNum(){ return msgSendNum; }
int getMaxCalls(){ return msgSendNum+1; }




void compute(int iter){

	setMsgSendNum(0);					// 设置发送消息数量为0，shortestPath算法不存在单点发送消息
	Vertex newVertex = vertex;			// 获取旧的顶点值
	// 循环对当前顶点接收到的每一个顶点进行处理
	for(int i=0;i<msgReceiveNum;i++){
		// 默认每条边距离为1，也可以依据实际情况做相应更改（这里默认源点为1）
		if( msgs[i].value + 1.0 < newVertex.value)
			newVertex.value = msgs[i].value + 1.0;
	}
	// 如果源点到某点的距离+1小于源点到当前点的距离，那么更新源点到当前点的距离
	if(newVertex.value < vertex.value)
        modifyVertexValue(newVertex);
}


Datum shortpath_tableudf(PG_FUNCTION_ARGS){
    int                  call_cntr;
    int                  max_calls;
    TupleDesc            tupdesc;
    AttInMetadata       *attinmeta;


    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext   oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* total number of tuples to be returned */
        funcctx->max_calls = 1;

        /* Build a tuple descriptor for our result type */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));

        /*
         * generate attribute metadata needed later to produce tuples from raw
         * C strings
         */
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;

        MemoryContextSwitchTo(oldcontext);




    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    attinmeta = funcctx->attinmeta;

    if (call_cntr < max_calls)    /* do when there is more left to send */
    {
        // 做我们自己的逻辑
        //接收三个参数
		int32            iter = PG_GETARG_INT32(0);
		HeapTupleHeader  old_value = PG_GETARG_HEAPTUPLEHEADER(1);
        all_msgs = text_to_cstring(PG_GETARG_TEXT_P(2));
		bool isnull;

		Datum id,value,other;
		id = GetAttributeByName(old_value, "id", &isnull);
		value = GetAttributeByName(old_value, "value", &isnull);	//value还是定义为double类型
		other = GetAttributeByName(old_value, "other", &isnull);
		if (isnull) PG_RETURN_BOOL(false);

		int id_value = DatumGetInt32(id);
		float8 value_value = DatumGetFloat8(value);
		int other_value = DatumGetInt32(other);	 //属性 1表示左   2表示右

		// old value 顶点值
		vertex.id = id_value; vertex.value = value_value; vertex.other = other_value;
		initialMessages(all_msgs);		//初始化msg数组
		compute(iter);

        //使用call_cntr作为数组下标，在第一次唤醒的时候就构造我们需要的东西，之后把要返回的结果放到数组中。
        /* Alternatively, we might prefer to do PG_RETURN_NULL() for null salary. */

        pfree(all_msgs);
        pfree(msgs);

        if(ansLen > 0){

			char       **values;
			HeapTuple    tuple;
			Datum        result;
			values = (char **) palloc(3 * sizeof(char *));
			values[0] = (char *) palloc(32 * sizeof(char));
			values[1] = (char *) palloc(32 * sizeof(char));
			values[2] = (char *) palloc(120 * sizeof(char));


            int32 t1 = ans[call_cntr].flag;
            int32 t2 = ans[call_cntr].to_node;
        	float8 t = ans[call_cntr].value;
            int32 t3 = ans[call_cntr].id;
            int32 t4 = ans[call_cntr].other;

            snprintf(values[0], 32, "%d", t1);		//表示是msg
            snprintf(values[1], 32, "%d", t2);
            snprintf(values[2], 120, "(%d,%f,%d)", t3,t,t4);

//	        snprintf(values[0], 32, "%d", 1);		//表示是msg
//	        snprintf(values[1], 32, "%d", -1);
//	        snprintf(values[2], 120, "(%d,%f,%d)", 1,0.0,0);


			/* build a tuple */
			tuple = BuildTupleFromCStrings(attinmeta, values);

			/* make the tuple into a datum */
			result = HeapTupleGetDatum(tuple);
			pfree(values[0]);
			pfree(values[1]);
			pfree(values[2]);
			pfree(values);


            pfree(ans); 

        	SRF_RETURN_NEXT(funcctx, result);
         }else    /* do when there is no more left */
         {
             pfree(ans);
             funcctx->max_calls = 0;
             SRF_RETURN_DONE(funcctx);
         }

        // 这样的话每次的第一轮会很耗时间，对于GraphX来说消耗很小；但是我们这里还要进行一轮完整的join筛选出当前未匹配的顶点；
        // 其实可以换成如下的逻辑：在2、3、4的情况下，每次都筛选出属性为L且未匹配的点作为v_prime表，但是这样的话会使得上面三轮的时间增加，甚至可能倍增，因为每一轮接收消息的点变多了。

        // -- 这样
        // 现在不是第一轮迭代确定哪些点发送消息消耗比较大吗，那么这里就依据我们写的函数里面的调用函数情况，来自动优化一下SQL脚本，使得这部分只是一个vertex表的自身筛选，不用join。如何实现？


    }
    else    /* do when there is no more left */
    {
        //pfree(ans);
        SRF_RETURN_DONE(funcctx);
    }

}






Datum graphh_state(PG_FUNCTION_ARGS){
    text *arg1 = PG_GETARG_TEXT_P(0);
    text *arg2 = PG_GETARG_TEXT_P(1);
    int32 new_text_size = VARSIZE(arg1) + VARSIZE(arg2) - VARHDRSZ;

    text *new_text = (text *)palloc(new_text_size);
    SET_VARSIZE(new_text,new_text_size);
    memcpy(VARDATA(new_text),VARDATA(arg1),VARSIZE(arg1)-VARHDRSZ);
    memcpy(VARDATA(new_text) + (VARSIZE(arg1)-VARHDRSZ),VARDATA(arg2),VARSIZE(arg2)-VARHDRSZ);

    PG_RETURN_TEXT_P(new_text);
}

Datum graphh_merge(PG_FUNCTION_ARGS){
    text *arg1 = PG_GETARG_TEXT_P(0);
    text *arg2 = PG_GETARG_TEXT_P(1);
    int32 new_text_size = VARSIZE(arg1) + VARSIZE(arg2) - VARHDRSZ;

    text *new_text = (text *)palloc(new_text_size);
    SET_VARSIZE(new_text,new_text_size);
    memcpy(VARDATA(new_text),VARDATA(arg1),VARSIZE(arg1)-VARHDRSZ);
    memcpy(VARDATA(new_text) + (VARSIZE(arg1)-VARHDRSZ),VARDATA(arg2),VARSIZE(arg2)-VARHDRSZ);

    PG_RETURN_TEXT_P(new_text);
}


