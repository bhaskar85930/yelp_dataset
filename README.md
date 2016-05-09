We can work on the given dataset in 4 approaches.
I have choosen HIVE to work on the given yelp_dataset.

create table yelp_dataset_raw(data string);
load data local inpath 'local file path' into table yelp_dataset_raw;

Approach 1:
-----------
create table yelp_dataset
row format delimited fields terminated by ','
stored as sequencefile
as 
select user_id, review_id, stars, dateid, text_data, size(split(text_data, ' ')) word_count, type, business_id, funny, useful, cool from
(select get_json_object(data, '$.user_id') as user_id,
get_json_object(data, '$.review_id') as review_id,
get_json_object(data, '$.stars') as stars,
get_json_object(data, '$.date') as dateid,
get_json_object(data, '$.text') as text_data,
get_json_object(data, '$.type') as type,
get_json_object(data, '$.business_id') as business_id,
get_json_object(data, '$.votes.funny') as funny,
get_json_object(data, '$.votes.useful') as useful,
get_json_object(data, '$.votes.cool') as cool
from yelp_dataset_raw) aa;

Explanation  ::::::::::::::::::

Extracts json object from a json string based on json path specified, and returns json string of the extracted json object. 
It will return null if the input json string is invalid. NOTE: The json path can only have the characters [0-9a-z_], 
i.e., no upper-case or special characters. 
Also, the keys *cannot start with numbers.* This is due to restrictions on Hive column names.

______________________________________________________________________________________________________________________________________
 

Approach 2
----------- 

create table yelp_dataset
row format delimited fields terminated by ','
stored as sequencefile
as 
select user_id, review_id, stars, dateid, text_data, size(split(text_data, ' ')) word_count, 
type, business_id, get_json_object(votes, '$.funny') as 
funny, get_json_object(votes, '$.useful') as useful, get_json_object(votes, '$.cool') as cool
from (
select x.* from raw lateral view json_tuple(data, 'user_id', 'review_id', 'stars', 'date', 'text', 'type', 'business_id', 'votes') 
x as user_id, review_id, stars, dateid, text_data, type, business_id, votes
) aa;

Explanation :::::::::::::::::

There are 2 level's in this query. In the first part, I have used json_tuple to extract the multiple columns at a time. 
In the second part, I have used get_json_object function to extract the other columns. 
______________________________________________________________________________________________________________________________________

Approach 3
-----------
create table yelp_dataset
row format delimited fields terminated by ','
stored as sequencefile
as  
select
x.user_id, x.review_id, x.stars, x.dateid, x.text_data, size(split(x.text_data, ' ')) word_count, x.type, x.business_id,
get_json_object(votes, '$.funny') as funny,
get_json_object(votes, '$.useful') as useful,
get_json_object(votes, '$.cool') as cool 
from raw lateral view json_tuple(data, 'user_id', 'review_id', 'stars', 'date', 'text', 'type', 'business_id', 'votes')x
as 
user_id, review_id, stars, dateid, text_data, type, business_id, votes;

Explanation :::::::::::::::::

JSON_TUPLE

A new json_tuple() UDTF is introduced in Hive 0.7. It takes a set of names (keys) and a JSON string, 
and returns a tuple of values using one function. This is much more efficient than calling GET_JSON_OBJECT 
to retrieve more than one key from a single JSON string. In any case where a single JSON string would be parsed more than once, 
your query will be more efficient if you parse it once, which is what JSON_TUPLE is for. 
As JSON_TUPLE is a UDTF, you will need to use the LATERAL VIEW syntax in order to achieve the same goal.
I have used json_tuple to extract the multiple columns at a time along with get_json_object function to extract the other columns like funny, useful, cool, 
but it will parse the entire json object 3 times for these 3 columns. 
_____________________________________________________________________________________________________________________________________

Approach 4
-----------
create table yelp_dataset
row format delimited fields terminated by ','
stored as sequencefile
as
select user_id, review_id, stars, dateid, text_data, size(split(text_data, ' ')) word_count, type, business_id, b.* from
(
select a.* from raw lateral view json_tuple(data, 'user_id', 'review_id', 'stars', 'date', 'text', 'type', 'business_id', 'votes') 
a as user_id, review_id, stars, dateid, text_data, type, business_id, votes
) aa lateral view json_tuple(votes, 'funny', 'useful', 'cool') b as funny, useful, cool;

Explanation :::::::::::::::::::

JSON_TUPLE

A new json_tuple() UDTF is introduced in Hive 0.7. It takes a set of names (keys) and a JSON string, 
and returns a tuple of values using one function. This is much more efficient than calling GET_JSON_OBJECT 
to retrieve more than one key from a single JSON string. In any case where a single JSON string would be parsed more than once, 
your query will be more efficient if you parse it once, which is what JSON_TUPLE is for. 
As JSON_TUPLE is a UDTF, you will need to use the LATERAL VIEW syntax in order to achieve the same goal.
I have used json_tuple to extract the multiple columns at a time along with get_json_object function to extract the other columns like funny, useful, cool, 
but it will parse the entire json object 3 times for these 3 columns.

I have used json_tuple function to extract the multiple columns. 
It will parse only twice to extract all the columns where as other options will parse the json data more than twice. 
So it is the best option among the four options.
_____________________________________________________________________________________________________________________________________

Conclusion
-----------
Among the 4 approches, 4th approch is the best one.


json_tuple(jsonStr, k1, k2, ...)

Takes a set of names (keys) and a JSON string, and returns a tuple of values. 
This is a more efficient version of the get_json_object UDF because it can get multiple keys with just one call.
______________________________________________________________________________________________________________________________________






