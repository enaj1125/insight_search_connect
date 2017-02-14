curl -u elastic -XGET 'localhost:9200/twitter_friend_data_2_7/_search?pretty' -H 'Content-Type: application/json' -d'
{

"query": {
   "bool": {
     "filter":[
       {"type": { "value": "227626917" }},
       {"range":{
          "early_time":{"from": "2015-01-02"}} }
       ]
     }
    }

}
'
