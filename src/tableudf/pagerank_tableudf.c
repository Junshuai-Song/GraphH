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
PG_FUNCTION_INFO_V1(pagerank_tableudf);
PG_FUNCTION_INFO_V1(graphh_state);
PG_FUNCTION_INFO_V1(graphh_merge);


/*
 CREATE OR REPLACE FUNCTION pagerank_tableudf(int, graphh_value,TEXT)
 RETURNS setof vertex_ AS
 '/data/workspace/hawq/lib/postgresql/pagerank_tableudf', 'pagerank_tableudf'
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



//辅助initialMsg函数的处理
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
//处理message字符串：需要知道Message数据中都有哪些数据，封装结构需要写一个配置文件
void initialMessages(char *v_all_msgs){
	all_msgs = v_all_msgs;
	int tot=1;
	int length = strlen(all_msgs);
	for(int i=0;i<length;i++)
		if(all_msgs[i]=='(') tot++;
	msgs = (Message *) palloc((tot+1) * sizeof(Message));

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
	ans[ansLen] = v_msg;
	ansLen = ansLen + 1;
}
void setMsgSendNum(int v_msgSendNum){
	msgSendNum = v_msgSendNum;
	v_msgSendNum++;		//这里的+1指多加一个新顶点value值
	funcctx->max_calls = v_msgSendNum;
	ansLen = 0;
	ans = (LNode *) palloc(v_msgSendNum * sizeof(LNode));
}
int getMsgSendNum(){ return msgSendNum; }
int getMaxCalls(){ return msgSendNum+1; }



void compute(int iter){

	setMsgSendNum(0);

	Vertex newVertex = vertex;
	newVertex.value = 0.0;
	for(int i=0;i<msgReceiveNum;i++){
        if(msgs[i].other!=0)
			newVertex.value += msgs[i].value/msgs[i].other;
	}
	newVertex.value = newVertex.value*0.85 + 0.15;
	modifyVertexValue(newVertex);

	// 这句话是需要默认增加的，已确认我需要发送多少条消息，那么就让TableUDF循环多少次。

}


Datum pagerank_tableudf(PG_FUNCTION_ARGS){
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
        /* 接收三个参数 */
		int32            iter = PG_GETARG_INT32(0);
		HeapTupleHeader  old_value = PG_GETARG_HEAPTUPLEHEADER(1);
		all_msgs = text_to_cstring(PG_GETARG_TEXT_P(2));
		bool isnull;

		Datum id,value,other;
		id = GetAttributeByName(old_value, "id", &isnull);
		value = GetAttributeByName(old_value, "value", &isnull);
		other = GetAttributeByName(old_value, "other", &isnull);
		if (isnull) PG_RETURN_BOOL(false);

		int id_value = DatumGetInt32(id);
		float8 value_value = DatumGetFloat8(value);
		int other_value = DatumGetInt32(other);	 //属性 1表示左   2表示右

		/* old value 顶点值 */
		vertex.id = id_value; vertex.value = value_value; vertex.other = other_value;
		initialMessages(all_msgs);		//初始化msg数组
		compute(iter);


        /* 使用call_cntr作为数组下标，在第一次唤醒的时候就构造我们需要的东西，之后把要返回的结果放到数组中。*/
        /* Alternatively, we might prefer to do PG_RETURN_NULL() for null salary. */

        pfree(msgs);

        char       **values;
        HeapTuple    tuple;
        Datum        result;
        values = (char **) palloc(3 * sizeof(char *));
        values[0] = (char *) palloc(32 * sizeof(char));
        values[1] = (char *) palloc(32 * sizeof(char));
        values[2] = (char *) palloc(120 * sizeof(char));

        float8 t = ans[call_cntr].value;

        snprintf(values[0], 32, "%d", ans[call_cntr].flag);		//表示是否为msg
        snprintf(values[1], 32, "%d", ans[call_cntr].to_node);
        snprintf(values[2], 120, "(%d,%f,%d)", ans[call_cntr].id,t,ans[call_cntr].other);

        /* build a tuple */
        tuple = BuildTupleFromCStrings(attinmeta, values);

        /* make the tuple into a datum */
        result = HeapTupleGetDatum(tuple);
        pfree(values[0]);
        pfree(values[1]);
        pfree(values[2]);
        pfree(values);
        SRF_RETURN_NEXT(funcctx, result);

    }
    else    /* do when there is no more left */
    {
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



