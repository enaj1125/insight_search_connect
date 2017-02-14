# This program will implement the webpage by calling render_Templete email_3 and emailop, and also do calculation

# import modules
from app import app
import json, os
from flask import jsonify
from elasticsearch import Elasticsearch
from flask import render_template, request
import redis
from datetime import datetime


# Setting up ES and Redis
es_host = ['ip-of-ES-cluster']
es = Elasticsearch(
                        es_host,
                        port=9200,
                        http_auth=('elastic', 'changeme'),
                        verify_certs=False,
                        sniff_on_start=True,    # sniff before doing anything
                        sniff_on_connection_fail=True,    # refresh node after a node fails to respond
                        sniffer_timeout=60,
                        timeout=15
                        )




@app.route('/')
def email():
 return render_template("email_3.html")



@app.route('/', methods = ['POST'])
def email_post():
    # recieve user input search request
    if request.form["emailid"]:
        search_text = request.form["emailid"] 
    else:
        search_text = None 
 
    # check user inputs 
    if request.form["userid_1"]: 
        search_user = request.form["userid_1"]
    else:
        search_user = request.form["userid_2"]
 
    start_date = request.form["start_time"]
    end_date = request.form["end_time"]
    start_date_ = start_date[:10]
    start_time = datetime.strptime(start_date_, "%m/%d/%Y").strftime("%Y-%m-%d")    
    end_date_ = end_date[:10]
    end_time = datetime.strptime(end_date_, "%m/%d/%Y").strftime("%Y-%m-%d")
    follower_opt = request.form["follower_option"] # 1 for followers, 0 for user
    user_only = True


    # query ES for followers
    if int(follower_opt):
        user_only = False 
        response = es.search(index='twitter_friend_data_2_8_4month', body={
      "query": {
         "bool": {
           "filter":[
             {"terms": { "usr_id": [search_user] }},
             {"range":{
                "early_time":{"gte": start_time}} }
             ]
           }
          },
      "size" : 1000 })

        if response['timed_out'] == True:
            json_response =[]

        else:
            search_results = response['hits']['hits']
            json_response = [X['_source'] for X in search_results ]
            friends_list = [x['follower_id'] for x in json_response]
            search_user2 = friends_list
    else:
        search_user2 = [search_user]
    



    # query ES for tweets 
    if not search_text:  # case 1: no keyword provide
        response = es.search(index='twitter_data_may', request_timeout = 30, body={
         "query": {
         "bool": {
         "filter":[
         {"terms": { "usr_id": search_user2 }},
         {"range":{
                  "ttimes":{"gte": start_time,
                            "lte": end_time}} }
              ]
         }
         },
        "size" : 1000 }
        ) 
  
    else:               # case 2: yes keyword provide 
        response = es.search(index='twitter_data_may', request_timeout = 30, body={
   	 "query": {
    	 "bool": {
    	 "filter":[
      	 {"match": { "ttext": search_text }},
      	 {"terms": { "usr_id": search_user2 }},
      	 {"range":{
        	  "ttimes":{"gte": start_time,
                	    "lte": end_time}} }
      	      ]
    	 }
   	 },
	"size" : 20 }
 	)

    if response['timed_out'] == True:
        json_response =[]
    else:
        search_results = response['hits']['hits']
        json_response = [X['_source'] for X in search_results ]


    
    # Prepare Summary Statistics
    summary_doc = {"search_term": search_text, "counts": len(json_response),  "start_t": start_time, "end_t": end_time}


    
    # Aggrigate json response file
    tree_data_collection = {}

    for element in json_response:
        usr_id = element["usr_id"]
        usr_tweet = element["ttext"] 
        if usr_id not in tree_data_collection.keys():
             tree_data_collection[usr_id] = [usr_tweet]
        else:
             tree_data_collection[usr_id].append(usr_tweet)
     


    # Reformat response file
    followers = []
    tree_response = {"name": search_user, "children": followers}

    for key, value in tree_data_collection.iteritems():
        tweets_list = []
        each_follower = {"name": key, "children": tweets_list}

        for tweet in value:
            element = {"name": tweet}
            tweets_list.append(element)

        followers.append(each_follower)

    if user_only:
        try:
            tree_response = tree_response['children'][0] 
        except:
            tree_response = []
    print "tree_response", tree_response
    # write to json file
    filename = 'app/static/flare_2.json'
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    with open(filename, 'w') as fp:
        json.dump(tree_response, fp)




   # Send final response 	 
    final_response = {"summary_doc": summary_doc, "tweets_res": json_response}  
    return render_template("emailop.html", output = final_response)

