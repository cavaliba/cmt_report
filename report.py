# (c) Cavaliva - 2022 - report.py


import yaml
import json
from elasticsearch import Elasticsearch
#from ssl import create_default_context
import ssl


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
    # User makes a request on client side
    user_request = "some_param"
    query_body = {
      "query": {
        "bool": {
          "must": {
            "match": {      
              "some_field": user_request
            }
          }
        }
      }
    }

    query_body = { "query": {"match_all": {} } }

    # call the client's search() method, and have it return results
    result = elastic_client.search(index=elastic_index, body=query_body, size=999)

    # see how many "hits" it returned using the len() function
    print ("total hits:", len(result["hits"]["hits"]))


    # '''
    # MAKE ANOTHER CALL THAT RETURNS
    # MORE THAN 10 HITS BY USING THE 'size' PARAM
    # '''
    # result = elastic_client.search(index="some_index", body=query_body, size=999)
    # all_hits = result['hits']['hits']

    # # see how many "hits" it returned using the len() function
    # print ("total hits using 'size' param:", len(result["hits"]["hits"]))

    # # iterate the nested dictionaries inside the ["hits"]["hits"] list
    # for num, doc in enumerate(all_hits):
    #     print ("DOC ID:", doc["_id"], "--->", doc, type(doc), "\n")

    #     # Use 'iteritems()` instead of 'items()' if using Python 2
    #     for key, value in doc.items():
    #         print (key, "-->", value)

    #     # print a few spaces between each doc for readability
    #     print ("\n\n")    