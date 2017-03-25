curl -u elastic -XGET 'localhost:9200/twitter_friend_data_2_8_4month/_search?pretty' -H 'Content-Type: application/json' -d'
{

"query": {
   "bool": {
     "filter":[
       {"match": { "usr_id": 890506944 }},
       {"range":{
          "early_time":{"from": "2015-05-01"}} }
       ]
     }
    }

}
'
