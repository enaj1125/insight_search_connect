# this query use multiple filters 

curl -u elastic -XGET 'localhost:9200/twitter_data_may/_search?pretty' -H 'Content-Type: application/json' -d'
{

"query": {
   "bool": {
     "filter":[
       {"terms": { "usr_id": [84279963] }},
       {"match": { "ttext": " " }},
       {"range":{
          "ttimes":{"from": "2015-05-02"}} }
       ]
     }
    }

}
'
