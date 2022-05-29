# (c) Cavaliva - 2022 - report.py


from datetime import date, timedelta
import yaml
import json
from elasticsearch import Elasticsearch
#from ssl import create_default_context
import ssl
from jinja2 import Environment, FileSystemLoader


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
def get_daterange(daterange='d0'):

    # values:
    # d0   : current day
    # d1   : previous day
    # w0   : current week
    # w1   : previous week
    # m0   : current month
    # m1   : previous month


    # response
    # ["YYYY-MM-DD", "YYYY-MM-DD"]


    today = date.today()

    if daterange == 'd0':
        d1 = today
        d2 = today + timedelta(days=1)
        label = "Today (current)"

    if daterange == 'd1':
        d1 = today - timedelta(days=1)
        d2 = today
        label = "Yesterday"

    if daterange == 'w0':
        d1 = today - timedelta(days=today.weekday())
        d2 = d1 + timedelta(days=6)
        label = "Current Week"

    if daterange == 'w1':
        d1 = today - timedelta(days = (today.weekday() + 7) )
        d2 = d1 + timedelta(days=6)
        label = "Previous Week"


    d1st = d1.strftime("%Y-%m-%d")
    d2st = d2.strftime("%Y-%m-%d")

    print("dates = ", d1,d2)
    return (d1st,d2st, label)

# ------------------------------------------
def raw1d_groups_events(d1, d2):
    ''' 
    groups x nb of events
    returns list [ {key: group_name , doc_count: doc_count} , ...]
    '''

    query = {

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
           "2": {
               "terms": { 
                  "field": "cmt_group",
                  "size": 500
                }
            }
        }
    }

    response = elastic_client.search(index=elastic_index, body=query, size=0)
    #print(json.dumps(response,indent=2))

    data = {}
    for group in response["aggregations"]["2"]["buckets"]:
        groupname = group["key"]
        groupcount = group["doc_count"]
        data[groupname] = groupcount
    return data
    #return response["aggregations"]["2"]["buckets"]

# ------------------------------------------
def raw1d_nodes_events(d1, d2):
    ''' 
    returns hash[group.node]=event_count
    '''

    query = {

        "size": 0,

          "aggs": {
            "2": {
              "terms": {
                "field": "cmt_group",
                "size": 500
              },
              "aggs": {
                "3": {
                  "terms": {
                    "field": "cmt_node",
                    "size": 500
                  }
                }
              }
            }
          },

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
        }

    }

    response = elastic_client.search(index=elastic_index, body=query, size=0)
    #print(json.dumps(response,indent=2))

    data = {}
    for group in response["aggregations"]["2"]["buckets"]:
        groupname = group["key"]
        #data[groupname] = {}
        for node in group["3"]["buckets"]:
            nodename = node["key"]
            nodecount = node["doc_count"]
            key = groupname+"."+nodename 
            #print(groupname, nodename, nodecount)
            data[key] = nodecount

    return data


# ------------------------------------------
# ------------------------------------------
# ------------------------------------------
# ------------------------------------------
# ------------------------------------------


if __name__ == "__main__":

    # Load Config
    configfile = "config.yml"
    config = LoadConfig(configfile)
    print('-'*60)
    print("CONFIG :",configfile)
    print(json.dumps(config, indent=2))

    # init Elastic
    elastic_url = config.get("elastic_url","http://localhost:9200/")
    elastic_index = config.get("elastic_index","cmt*")
    elastic_client = Elasticsearch([elastic_url])


    # init Jinja2
    file_loader = FileSystemLoader('templates')
    env = Environment(loader=file_loader)
    html_output_dir = config.get("html_output_dir","/var/www/html/cmt_report")

    # Cluster Info
    print('-'*60)
    print("CLUSTER INFOS")
    info = elastic_client.info()
    print(json.dumps(info, indent=2))


    dateranges = [ "d0", "d1", "w0", "w1"]
    #dateranges = [ "d0"]

    for dr in dateranges:

        print('-'*60)
        [date1,date2, datelabel] = get_daterange(dr)    
        print(datelabel)

        output_blocs = []

        # TABLE : GROUPS x EVENTS
        data = raw1d_groups_events(d1=date1,d2=date2)
        print(json.dumps(data,indent=2))
        template = env.get_template('table_groups.html')
        context = { 
            "title": "Groups and event count", 
            "date_label": datelabel, 
            "date_d1":date1,
            "date_d2":date2
            }
        output = template.render(data=data, context=context)
        output_blocs.append(output)

        # TABLE : NODES x EVENTS
        data = raw1d_nodes_events(d1=date1,d2=date2)
        print(json.dumps(data,indent=2))
        template = env.get_template('table_nodes.html')
        context = { 
            "title": "Nodes event count", 
            "date_label": datelabel, 
            "date_d1":date1,
            "date_d2":date2
            }
        output = template.render(data=data, context=context)
        output_blocs.append(output)
        

        # RENDER full PAGE
        template = env.get_template('page_global.html')
        context = {}
        output = template.render(data=output_blocs, context=context)
        pagename = html_output_dir + "/page_global_" + dr + ".html"
        with open(pagename, "w") as fh:
            fh.write(output)





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