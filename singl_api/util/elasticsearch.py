from elasticsearch5 import Elasticsearch

from basic_info.mylogging import myLog

log=myLog().getLog().logger

# elasticsearch集群服务器的地址
ES = [
    '192.168.1.82:9206'
]

# 创建elasticsearch客户端
es = Elasticsearch(
    ES,
    # 启动前嗅探es集群服务器
    sniff_on_start=True,
    # es集群服务器结点连接异常时是否刷新es节点信息
    sniff_on_connection_fail=True,
    # 每60秒刷新节点信息
    sniffer_timeout=60
)


def get_es_data(index=None,doc_type=None,value=None,name_list=None):
    query = {"from":0,"size":50,"query":{"bool":{"must":[{"term":{"storage":{"value":value,"boost":1.0}}}],"adjust_pure_negative":True,"boost":1.0}},"_source":{"includes":["filesetId","host","storage","format","directory","name","owner","recordTime","modifyTime","size","tags"],"excludes":["content"]},"sort":[{"recordTime":{"order":"desc"}},{"name":{"order":"asc"}}]}
    # query={"from":0,"size":8,"query":{"bool":{"must":[{"ids":{"values":["93c3505153368edaab2bb64562787d3e","0a18b6c6b69bbfb14474011e756d807d"],"boost":1.0}},{"term":{"content":{"value":"test","boost":1.0}}}],"adjust_pure_negative":True,"boost":1.0}},"_source":{"includes":["filesetId","host","storage","format","directory","name","owner","recordTime","modifyTime","size"],"excludes":["content"]},"sort":[{"recordTime":{"order":"desc"}},{"name":{"order":"asc"}}],"highlight":{"pre_tags":["<span style = 'color:red'>"],"post_tags":["</span>"],"fields":{"content":{}}}}
    log.info("开始查询es数据")
    try:
        ret = es.search(index=index, doc_type=doc_type, body=query)
    except Exception as e:
        log.error("es查询异常",e)
        return
    else:
        log.info("es查询结果为%s"%ret)
        ret_list=ret['hits']['hits']
        result_list = []
        if ret_list:
            if not isinstance(name_list, list):
                raise Exception(u'传入参数要求是列表类型,请检查传入参数类型!')
            else:
                for i in ret_list:
                    for j in range(len(name_list)):
                        if i["_source"]["name"]==name_list[j]:
                            result_list.append(i["_id"])
                        j+=1
                return result_list

if __name__=="__main__":
    a=get_es_data("index_file_32","_doc","SFTP",["sftp.sql","different_students_info.csv"])
    print(a)