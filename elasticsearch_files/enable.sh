# this query search for a certain type of file

curl -u -elastic -XPUT 'localhost:9200/twitter_friend_data_2_8/_mapping/input_type?pretty' -H 'Content-Type: application/json' -d'
{
  "properties": {
    "usr_id": { 
      "type":     "text",
      "fielddata": true
    }
  }
}
'


