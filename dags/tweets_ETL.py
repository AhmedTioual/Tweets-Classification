from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import brotli
import time
from pymongo.mongo_client import MongoClient
import random
import tempfile

user_header_1 = {
                    "Accept": "*/*",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept-Language": "en-US,en;q=0.5",
                    "authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
                    "Connection": "keep-alive",
                    "content-type": "application/json",
                    "Cookie": "guest_id_marketing=v1%3A171344721728559785; guest_id_ads=v1%3A171344721728559785; personalization_id=\"v1_GHe0SFbAlPCSfxNXyPRTrw==\"; guest_id=v1%3A171344721728559785; external_referer=padhuUp37zjgzgv1mFWxJ12Ozwit7owX|0|8e8t2xd8A2w%3D; _ga=GA1.2.407514221.1713447191; g_state={\"i_l\":0}; kdt=VEcTPaQvk3prU87Ar4MM7P5bLPc66vDjmV9q2GRa; twid=u%3D1780955256953991168; ct0=252afef22d33c4b4be0b7be5d3f4f86661ed9449aa12270de42d335ee2e9b239b252a318cdf44ddd551b013096ed3f98c48f4cd2fb029b376a96c6bc12555f11a3943fc7c6878783e73ff0af9c246f04; auth_token=3b59938cd93f261ba069889f5274ee38c3fdb48f; lang=en",
                    "Host": "twitter.com",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
                    "x-client-transaction-id": "4gXEcaYkeNp8zjsQ7VO4zGVRC7FbCUxrKjnWtXmLFaD5XAA9x551gH1r8Nf/y3Sd9WUcNON1YYZbVfL/iLyr/vwPnLd54Q",
                    "X-Client-UUID": "e90c4a62-2110-4881-a9de-fa79e8f69cf0",
                    "x-csrf-token": "252afef22d33c4b4be0b7be5d3f4f86661ed9449aa12270de42d335ee2e9b239b252a318cdf44ddd551b013096ed3f98c48f4cd2fb029b376a96c6bc12555f11a3943fc7c6878783e73ff0af9c246f04",
                    "x-twitter-active-user": "yes",
                    "x-twitter-auth-type": "OAuth2Session",
                    "x-twitter-client-language": "en"
                }

user_header_2 = {
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.5",
                "authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
                "Connection": "keep-alive",
                "content-type": "application/json",
                "Cookie": "guest_id_marketing=v1%3A171379298354115241; guest_id_ads=v1%3A171379298354115241; personalization_id=\"v1_MLpslkFpbqqSPvkAp9ssag==\"; _ga=GA1.2.407514221.1713447191; g_state={\"i_l\":0}; kdt=VEcTPaQvk3prU87Ar4MM7P5bLPc66vDjmV9q2GRa; lang=en; att=1-JnETQvoehBug6emlr8wcLgCYgCSUpFSqFphk3iqO; dnt=1; guest_id=v1%3A171379298354115241; gt=1782403076642443538; _twitter_sess=BAh7BiIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7AA%253D%253D--1164b91ac812d853b877e93ddb612b7471bebc74; twid=u%3D1782403538888396800; ct0=f72cd751163e66e684bfbb66475db361b7bf2c986005bd01dd5cf048cce54c99ad6eb295b5293598156a89f2ab9a6f5a9524c7d909fc4a34b473a4dc3e8d1a5f68763676c554857c6a831e3959a91a07; auth_token=cf107966615be39f0d1f90e49f2ad2c382924f92",
                "Host": "twitter.com",
                "Referer": "https://twitter.com/search?q=fidel%20odinga&src=typeahead_click",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "TE": "trailers",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
                "x-client-transaction-id": "sX5W5M6pHp38Lf67tkovZewpwekw5IoCb+ivLGVit2Ai37tUG/dusC/5ex5/AhfGdPi0ZrDMKn0GBGFjiaT9cXxcCuHnsg",
                "X-Client-UUID": "09f12091-70b5-4b26-b277-1898d31951e6",
                "x-csrf-token": "f72cd751163e66e684bfbb66475db361b7bf2c986005bd01dd5cf048cce54c99ad6eb295b5293598156a89f2ab9a6f5a9524c7d909fc4a34b473a4dc3e8d1a5f68763676c554857c6a831e3959a91a07",
                "x-twitter-active-user": "yes",
                "x-twitter-auth-type": "OAuth2Session",
                "x-twitter-client-language": "en"
                }

user_header_3 = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.5",
            "authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
            "Connection": "keep-alive",
            "content-type": "application/json",
            "Cookie": "guest_id_marketing=v1%3A171379353291918111; guest_id_ads=v1%3A171379353291918111; personalization_id=\"v1_KWr/PP8PIQMmcxsSiI2mPg==\"; _ga=GA1.2.407514221.1713447191; g_state={\"i_l\":1,\"i_p\":1713800698044}; kdt=VEcTPaQvk3prU87Ar4MM7P5bLPc66vDjmV9q2GRa; lang=en; att=1-JnETQvoehBug6emlr8wcLgCYgCSUpFSqFphk3iqO; dnt=1; gt=1782403076642443538; guest_id=v1%3A171379353291918111; _twitter_sess=BAh7CSIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7ADoHaWQiJTgxYmE5OWJmYzBmZGYyN2QzOTIyYmYx%250AOWQyMDlhNWViOg9jcmVhdGVkX2F0bCsIdbgRBo8BOgxjc3JmX2lkIiU2YjA2%250ANWQyNjFlZmU5NjE4MDExZDk1OGJjY2YwOGUwOQ%253D%253D--1621511d0908501db888fa44fe42059a47454611; twid=u%3D1782406349592154112; ct0=6168cda4def423cf818ced997eb718681e420ee5e57a0aed5d2f1f7993fa6cd6f969bbe4f158be4346c521051d87d3c8dfc3d811061ffe9686e4da4971812171dcf604d136c58f072915bfad55af0739; auth_token=e49b4ce11ccb45e2c5878b10f5df17e20a47f159",
            "Host": "twitter.com",
            "Referer": "https://twitter.com/FD",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "TE": "trailers",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "x-client-transaction-id": "8n4ACIk9vvbQ4YzyoW5JY1p/cKT5tFM2M2tW8OOhIpmMJYjpJ0cbrzI3e/vjqRwQSn/1JfPXsvibVs7T4FfLsgK90n5q8Q",
            "X-Client-UUID": "56aa1c1d-9311-4d25-9633-c1613cab5fdd",
            "x-csrf-token": "6168cda4def423cf818ced997eb718681e420ee5e57a0aed5d2f1f7993fa6cd6f969bbe4f158be4346c521051d87d3c8dfc3d811061ffe9686e4da4971812171dcf604d136c58f072915bfad55af0739",
            "x-twitter-active-user": "yes",
            "x-twitter-auth-type": "OAuth2Session",
            "x-twitter-client-language": "en"
            }

def get_next_topic(json_file_path='dags/topics/topic.json'):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            topics = json.load(file)
            log_value = read_from_log()
            print("Log value:", log_value)
            print("Number of topics:", len(topics.keys()))

            if len(topics.keys()) == log_value:
                keep_last_line_only()
                write_to_log(new_line="0")
                print("Reset log to 0. No action needed.")
                return {}

            else:
                key_index = log_value
                key = list(topics.keys())[key_index]
                keep_last_line_only()
                write_to_log(new_line=str(log_value + 1))
                print("Returning topic:", {key: topics[key]})
                return {key: topics[key]}

    except FileNotFoundError:
        print(f"Error: File '{json_file_path}' not found.")
        return {}

def write_to_log(file_path="dags/topics/logTopics.txt", new_line="0"):
    try:
        with open(file_path, 'a', encoding='utf-8') as file:
            file.write(new_line + '\n')  # Append the new line of text followed by a newline character
            print("New line added to logTopics.txt:", new_line)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")

def read_from_log(file_path="dags/topics/logTopics.txt"):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            if lines:
                last_line = lines[-1].strip()
                return int(last_line)
            else:
                return 0
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None

def keep_last_line_only(file_path="dags/topics/logTopics.txt"):
    try:
        # Read all lines from the file
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        # Check if there are more than two lines
        if len(lines) > 1:
            # Extract the last line
            last_line = lines[-1].strip()

            # Write only the last line back to the file
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(last_line + '\n')
                print("Only the last line kept in logTopics.txt:", last_line)
        else:
            print("No action needed. File has one or zero lines.")

    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")

def save_json_to_temp_file(data):
    """
    Saves JSON data to a temporary file and returns the path of the temporary file.

    Args:
        data (dict): Dictionary containing JSON serializable data.

    Returns:
        str: Path of the temporary file where JSON data is stored.
    """
    try:
        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            # Write JSON data to the temporary file
            json.dump(data, temp_file, indent=4)
            temp_file.flush()  # Flush to ensure data is written to the file
            temp_file.seek(0)  # Move file pointer to the beginning

            # Get the temporary file path
            temp_file_path = temp_file.name

            return temp_file_path

    except Exception as e:
        print(f"Error occurred while saving JSON to temporary file: {e}")
        return None

def read_json_file(file_path):
    """
    Reads JSON data from a file and returns the parsed JSON content as a dictionary.

    Args:
        file_path (str): Path to the JSON file.

    Returns:
        dict or None: Parsed JSON content as a dictionary, or None if an error occurs.
    """
    try:
        with open(file_path, 'r') as json_file:
            json_data = json.load(json_file)
            return json_data
    except FileNotFoundError:
        print(f"Error: JSON file not found at '{file_path}'.")
    except json.JSONDecodeError as e:
        print(f"Error: Failed to decode JSON from file '{file_path}'. {e}")
    except Exception as e:
        print(f"Error occurred while reading JSON file '{file_path}': {e}")
    return None
  
last_keyword = None

def get_random_user():
    global last_keyword
    keywords = ['user_header_1', 'user_header_2', 'user_header_3']
    
    while True:
        keyword = random.choice(keywords)
        if keyword != last_keyword:
            last_keyword = keyword
            return keyword

def extract_json_data(search,next_page="",header=user_header_1):
    # Define the URL
    url = "https://twitter.com/i/api/graphql/LcI5kBN8BLC7ovF7mBEBHg/SearchTimeline"

    # Define the request parameters
    variables = {"rawQuery": search, "count": 20, "querySource": "typed_query", "product": "Latest","cursor":next_page}
    
    features = {
        "rweb_tipjar_consumption_enabled": False,
        "responsive_web_graphql_exclude_directive_enabled": True,
        "verified_phone_label_enabled": False,
        "creator_subscriptions_tweet_preview_api_enabled": True,
        "responsive_web_graphql_timeline_navigation_enabled": True,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
        "communities_web_enable_tweet_community_results_fetch": True,
        "c9s_tweet_anatomy_moderator_badge_enabled": True,
        "tweetypie_unmention_optimization_enabled": True,
        "responsive_web_edit_tweet_api_enabled": True,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
        "view_counts_everywhere_api_enabled": True,
        "longform_notetweets_consumption_enabled": True,
        "responsive_web_twitter_article_tweet_consumption_enabled": True,
        "tweet_awards_web_tipping_enabled": False,
        "creator_subscriptions_quote_tweet_preview_enabled": False,
        "freedom_of_speech_not_reach_fetch_enabled": True,
        "standardized_nudges_misinfo": True,
        "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
        "tweet_with_visibility_results_prefer_gql_media_interstitial_enabled": False,
        "rweb_video_timestamps_enabled": True,
        "longform_notetweets_rich_text_read_enabled": True,
        "longform_notetweets_inline_media_enabled": True,
        "responsive_web_enhance_cards_enabled": False
    }

    # Define the headers
    headers = header
    # Send the GET request
    response = requests.get(url, params={"variables": json.dumps(variables), "features": json.dumps(features)}, headers=headers)

    # Check if request was successful
    if response.status_code == 200:
        print("Request was successful")

        # Check if the response is compressed
        print("Content-Encoding:", response.headers.get('Content-Encoding'))

        if response.headers.get('Content-Encoding') == 'br':
            # Decompress the response content
            try:
                response_content = brotli.decompress(response.content)
            except Exception as e:
                print("Error decompressing content:", e)
                response_content = response.content
        else:
            response_content = response.content

        # Parse the JSON content
        try:
            response_json = json.loads(response_content)
            return response_json
        except json.JSONDecodeError as e:
            print("Error decoding JSON content:", e)
            return None
    else:
        print("Request failed with status code:", response.status_code)
        return None

def get_response_count(response_json):
    try:
        count = len(response_json['data']['search_by_raw_query']['search_timeline']['timeline']['instructions'][0]['entries'])
        return count
    except KeyError:
        #print("KeyError: Could not find the specified key in the response JSON.")
        return 0
    
def extract_batch_data(tweet_topics,header=user_header_1,**kwargs):
  raw_data = []
  for _, values in tweet_topics.items():
      for value in values:
          try:
            response_json = extract_json_data(value,header=header)
            if get_response_count(response_json) > 0:
                raw_data.append(response_json)
            else:
                break
            for i in range(4):
              if i == 0 :
                  next_page = len(response_json['data']['search_by_raw_query']['search_timeline']['timeline']['instructions'][0]['entries'])-1
                  next_page = response_json['data']['search_by_raw_query']['search_timeline']['timeline']['instructions'][0]['entries'][next_page]['content']['value']
                  response_json = extract_json_data(value,next_page,header=header)
                  if get_response_count(response_json) > 0:
                      raw_data.append(response_json)
                  else:
                      break
              else :
                  next_page = response_json['data']['search_by_raw_query']['search_timeline']['timeline']['instructions'][2]['entry']['content']['value']
                  response_json = extract_json_data(value,next_page,header=header)
                  if get_response_count(response_json) > 0:
                      raw_data.append(response_json)
                  else:
                      break
              time.sleep(5)
          except Exception as ex:
              print(ex)
          time.sleep(2.5)


  temp_file = save_json_to_temp_file(raw_data) 
  kwargs['ti'].xcom_push(key='raw_batch_data_key', value=temp_file) 
  
  return True,temp_file

def transform_batch_data(topic,**kwargs):
  
  temp_file = kwargs['ti'].xcom_pull(task_ids='extract_tweets_task', key='raw_batch_data_key')

  raw_data = read_json_file(file_path=temp_file)

  tweets_data = []

  for response_json in raw_data : 

      num_tweets = get_response_count(response_json)

      for i in range(num_tweets):
          try:
              data = response_json['data']['search_by_raw_query']['search_timeline']['timeline']['instructions'][0]['entries'][i]

              if 'itemContent' not in data['content']:
                break

              # Extract tweet information from the response JSON
              tweet_data = data['content']['itemContent']['tweet_results']['result']

              # Check if 'tweet' key is present
              if 'tweet' in tweet_data:
                  user_info = tweet_data['tweet']['core']
                  views_info = tweet_data['tweet']['views']
                  tweet_info = tweet_data['tweet']['legacy']
              else:
                  user_info = tweet_data['core']
                  views_info = tweet_data['views']
                  tweet_info = tweet_data['legacy']
              
              # Process tweet information here
              tweet = {
                      'followers_count': user_info['user_results']['result']['legacy']['followers_count'],
                      'friends_count': user_info['user_results']['result']['legacy']['friends_count'],
                      'location': user_info['user_results']['result']['legacy']['location'],
                      'verified': user_info['user_results']['result']['legacy']['verified'],
                      'created_at': tweet_info['created_at'],
                      'hashtags': [hashtag['text'] for hashtag in tweet_info['entities']['hashtags']],
                      'favorite_count': tweet_info['favorite_count'],
                      'full_text': tweet_info['full_text'],
                      'lang': tweet_info['lang'],
                      'quote_count': tweet_info['quote_count'],
                      'reply_count': tweet_info['reply_count'],
                      'retweet_count': tweet_info['retweet_count'],
                      'views_count': views_info.get('count', 0),
                      'topic': topic
                  }

              tweets_data.append(tweet)
                
          except KeyError as ke:
              # Handle missing keys
              print("KeyError:", ke)
              
          except Exception as ex:
              # Handle other exceptions
              print("Exception:", ex)

  temp_file = save_json_to_temp_file(tweets_data) 
  kwargs['ti'].xcom_push(key='clean_data_key', value=temp_file) 
  
  return True,temp_file

def load_to_mongodb(**kwargs):
    
    temp_file = kwargs['ti'].xcom_pull(task_ids='transform_tweets_task', key='clean_data_key')

    data_to_insert = read_json_file(file_path=temp_file)

    # MongoDB connection URL
    url = "mongodb+srv://mlteam:mlteam1234@cluster0.6y3bpz0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

    # Connect to MongoDB
    client = MongoClient(url)

    try:
        # Access the database
        db = client.get_database("TweetsDataBase")

        # Access or create the collection
        collection = db.get_collection("TweetsData")

        # Insert data into the collection
        collection.insert_many(data_to_insert)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the connection
        client.close()

# Define the DAG settings
default_args = {
    'owner': 'MLTeam',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

# Define the DAG
dag = DAG(
    'tweet_topics_processing',
    default_args=default_args,
    description='Process tweet topics data',
    schedule_interval='@daily',
)

tweets_dict = get_next_topic()
user = get_random_user()

# Define tasks for each topic
extract_task = PythonOperator(
        task_id=f'extract_tweets_task',
        python_callable=extract_batch_data,
        op_kwargs={'tweet_topics': tweets_dict,"header":user},
        dag=dag,
    )

transform_task = PythonOperator(
        task_id='transform_tweets_task',
        python_callable=transform_batch_data,
        op_kwargs={'topic': tweets_dict.keys()},
        dag=dag,
    )

load_task = PythonOperator(
        task_id=f'load_tweets_to_mongodb_task',
        python_callable=load_to_mongodb,
        op_kwargs={'clean_data': "topic"},
        dag=dag,
    )

    # Set task dependencies

extract_task >> transform_task >> load_task