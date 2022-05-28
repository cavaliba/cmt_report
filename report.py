# (c) Cavaliva - 2022 - report.py


from datetime import date, timedelta
import yaml
import json
from elasticsearch import Elasticsearch
#from ssl import create_default_context
import ssl



elastic_url = ""
elastic_index = ""
elastic_client = ""


# ------------------------------------------
def LoadConfig(file="config.yml"):

    try:
        with open(file) as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)
            #print(json.dumps(jsonconf, indent=2))
            return config
    except Exception as e:
        msg = "Could not load YAML configuration %s" % file
        print(msg)
        print(e)
        exit(0)
        return {}


# ------------------------------------------
def get_daterange(daterange='d'):

    # values:
    # d    : current day
    # d1   : previous day
    # w    : current week
    # w 1  : previous week
    # m    : current month
    # m-1  : previous month


    # response
    # ["YYYY-MM-DD", "YYYY-MM-DD"]


    today = date.today()

    if daterange == 'd':
        d1 = today
        d2 = today + timedelta(days=1)

    if daterange == 'd1':
        d1 = today - timedelta(days=1)
        d2 = today

    if daterange == 'w':
        d1 = today - timedelta(days=today.weekday())
        d2 = d1 + timedelta(days=6)

    if daterange == 'w1':
        d1 = today - timedelta(days = (today.weekday() + 7) )
        d2 = d1 + timedelta(days=6)


    d1st = d1.strftime("%Y-%m-%d")
    d2st = d2.strftime("%Y-%m-%d")

    print("dates = ", d1,d2)
    return (d1st,d2st)

# ------------------------------------------
def q_groups(daterange="d"):
    ''' returns hash { group_name => doc counts }
    '''

    [d1,d2] = get_daterange(daterange)
    mysizemax  = 200

    query_body = {

        "size": 0,
        "query": {
           "bool": {
               "filter": {
                   "range": {
                       "timestamp": {
                           "format": "yyyy-MM-dd",
                           "gte": d1,
                           "lte": d2
                        }
                    }
                }
            }
        },
        "aggs": {
           "mygroups": {
               "terms": { 
                  "field": "cmt_group",
                  "size": mysizemax
                }
            }
        }
    }

    result = elastic_client.search(index=elastic_index, body=query_body, size=0)
    #print(json.dumps(result,indent=2))

    response = {}
    for item in result["aggregations"]["mygroups"]["buckets"]:
        key = item["key"]
        value = item["doc_count"]
        response[key]=value
        #print(key,value)

    return response


# ------------------------------------------
# ------------------------------------------
# ------------------------------------------
# ------------------------------------------
# ------------------------------------------


if __name__ == "__main__":

    configfile = "config.yml"
    config = LoadConfig(configfile)
    print('-'*60)
    print("CONFIG :",configfile)
    print(json.dumps(config, indent=2))


    elastic_url = config.get("elastic_url","http://localhost:9200/")
    elastic_index = config.get("elastic_index","cmt*")


    elastic_client = Elasticsearch([elastic_url])

    print('-'*60)
    print("CLUSTER INFOS")
    info = elastic_client.info()
    print(json.dumps(info, indent=2))

    print('-'*60)
    response = q_groups("d")
    print(response)


    #query_body = { "query": {"match_all": {} } }
    #result = elastic_client.search(index=elastic_index, body=query_body, size=999)
    #print ("total hits:", len(result["hits"]["hits"]))

    # result = elastic_client.search(index="some_index", body=query_body, size=999)
    # all_hits = result['hits']['hits']
    # print ("total hits using 'size' param:", len(result["hits"]["hits"]))

    # for num, doc in enumerate(all_hits):
    #     print ("DOC ID:", doc["_id"], "--->", doc, type(doc), "\n")

    #     # Use 'iteritems()` instead of 'items()' if using Python 2
    #     for key, value in doc.items():
    #         print (key, "-->", value)

    #     # print a few spaces between each doc for readability
    #     print ("\n\n")    