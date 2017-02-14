curl -u elastic -XGET 'localhost:9200/twitter_friend_data_2_8_4month/_search?pretty' -H 'Content-Type: application/json' -d'
{
"size" : 20,
   "aggs": {
      "usr_id": {
         "terms": {
            "field": "usr_id.keyword"
         }
      }
   }
}
'
