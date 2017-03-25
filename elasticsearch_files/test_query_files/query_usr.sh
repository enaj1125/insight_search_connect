# query a user using term method
curl -u elastic -XGET 'localhost:9200/twitter_data_may/_search?pretty' -H 'Content-Type: application/json' -d'
{

"query": {
   "bool": {
     "filter":[
       {"terms": { "usr_id": [2238576858] }}
       ]
     }
    }

}
'
