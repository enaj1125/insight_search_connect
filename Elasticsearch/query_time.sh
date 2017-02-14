curl -u elastic -XGET 'localhost:9200/twitter_data_may/_search?pretty' -H 'Content-Type: application/json' -d'
{
  "query": { "range": {
                "ttimes": {
                        "gte": "2015-06-29",
			"lte": "2015-06-30"
                }
         }
 }
}
'
