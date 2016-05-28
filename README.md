We can work on the given dataset in 4 approaches.
I have choosen HIVE to work on the given yelp_dataset.

GET_JSON_OBJECT
---------------
Extracts json object from a json string based on json path specified, and returns json string of the extracted json object. It will return null if the input json string is invalid. 
NOTE: The json path can only have the characters [0-9a-z_], i.e., no upper-case or special characters. Also, the keys *cannot start with numbers.* This is due to restrictions on Hive column names.


JSON_TUPLE
----------
A new json_tuple() UDTF is introduced in Hive 0.7. It takes a set of names (keys) and a JSON string, and returns a tuple of values using one function. This is much more efficient than calling GET_JSON_OBJECT to retrieve more than one key from a single JSON string. In any case where a single JSON string would be parsed more than once, your query will be more efficient if you parse it once, which is what JSON_TUPLE is for. As JSON_TUPLE is a UDTF, you will need to use the LATERAL VIEW syntax in order to achieve the same goal.

json_tuple(jsonStr, k1, k2, ...)

Takes a set of names (keys) and a JSON string, and returns a tuple of values. 
This is a more efficient version of the get_json_object UDF because it can get multiple keys with just one call.

JSON SerDE
----------


Approach 1
-----------
get_json_object is used to extract the data from json, but every time it parses the json object which is not efficient.

Approach 2
----------- 
There are 2 level's in this query. In the first part, i have used json_tuple to extract the multiple columns at a time. In the second part, i have used get_json_object function to extract the other columns.
I have used json_tuple to extract the multiple columns at a time along with get_json_object function to extract the other columns like funny, useful, cool, but it will parse the entire json object 3 times for these 3 columns.

Approach 3
-----------
I have used json_tuple function to extract the multiple columns. It will parse only twice to extract all the columns where as other options will parse the json data more than twice.

Approach 4
-----------
Using JSON SerDe
 
A SerDe is a better choice than a json functions both get_json_object and json_tuple (UDF) for two reasons

1. It only has to parse each JSON record once
2. We can define the JSON schema in the Hive table schema, making it much easier to issue queries against.

Conclusion
-----------
Among the 4 approches, fourth approch is the best one.

