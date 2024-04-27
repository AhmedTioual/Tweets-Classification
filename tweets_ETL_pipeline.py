from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import brotli
import time
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import random
import tempfile

user_header_1 = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.5",
            "Authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
            "Connection": "keep-alive",
            "Content-Length": "7451",
            "Content-Type": "application/x-www-form-urlencoded",
            "Cookie": "g_state={\"i_l\":0}; kdt=jdeWo36eab8HLeYm861t6VOrOgR2lnmeBjQm8zB0; des_opt_in=Y; dnt=1; d_prefs=MjoxLGNvbnNlbnRfdmVyc2lvbjoyLHRleHRfdmVyc2lvbjoxMDAw; lang=en; guest_id=v1%3A171422321121825927; gt=1784207582267101521; _twitter_sess=BAh7BiIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7AA%253D%253D--1164b91ac812d853b877e93ddb612b7471bebc74; twid=u%3D1784208028687847424; ct0=269784ca13153bdee5e3172baf194b537726c10071bb50be68c0c47f319fb52e04b113f601be0a40fc8d826c8751efb93c926449c5f0479d19a81ecc05f9f939152c3dccf4e685c00f73f6be1d5e22fe; auth_token=06ba5d3f9206e1337a3607e5b097329d7ad56a6c; guest_id_marketing=v1%3A171422321121825927; guest_id_ads=v1%3A171422321121825927; personalization_id=\"v1_0rxgHC4RvVBo9rfP6B7CbA==\"",
            "Host": "twitter.com",
            "Origin": "https://twitter.com",
            "Referer": "https://twitter.com/search?q=%D8%A7%D9%82%D8%AA%D8%B5%D8%A7%D8%AF&src=typed_query",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
            "x-client-transaction-id": "Mn18OgQNYBEG46Ju8k7B2x9HpCINLmMCtScAM/4E7BLgBdsMn9+KySzN9tXztH61hIen7zO6Lz8+uGA05spna4lbTRYLMQ",
            "X-Client-UUID": "e83a68c3-b047-46e3-8a9b-9ce040c23d7b",
            "x-csrf-token": "269784ca13153bdee5e3172baf194b537726c10071bb50be68c0c47f319fb52e04b113f601be0a40fc8d826c8751efb93c926449c5f0479d19a81ecc05f9f939152c3dccf4e685c00f73f6be1d5e22fe",
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

user_header_3 = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.5",
            "Authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Cookie": "g_state={\"i_l\":0}; kdt=jdeWo36eab8HLeYm861t6VOrOgR2lnmeBjQm8zB0; des_opt_in=Y; dnt=1; d_prefs=MjoxLGNvbnNlbnRfdmVyc2lvbjoyLHRleHRfdmVyc2lvbjoxMDAw; lang=en; gt=1784207582267101521; _twitter_sess=BAh7CSIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7ADoHaWQiJTZiNmUzMDhiMGZhOTIzMWY4MGFiYjlm%250AMTI1YTBjY2NhOg9jcmVhdGVkX2F0bCsI6D22H48BOgxjc3JmX2lkIiU5ZjUz%250AMWJhOWFmNzVjNDJhZjBjODQxZDBlNDIxYjdjMA%253D%253D--9837b797cddff86442fe34d8d7d3bf5394ac5ed4; guest_id_marketing=v1%3A171422408234900792; guest_id_ads=v1%3A171422408234900792; personalization_id=\"v1_FnuWqvowa2pd6gCanb7i9Q==\"; guest_id=v1%3A171422408234900792; att=1-1nezxRXReWkvKH2RIzDN9o5WJEh43tAqaUQWRP7R; twid=u%3D1784211485993897984; ct0=0987765d52578a8c3eb86c3047fb472e00f13a3465e58175bbe6e1c0721291656c87b6ef841538cc1446c16b3095152d0d5d35f3fbbd9bf289bc2e4545b25794c4142b93a308926e990afbce4758b243; auth_token=3e0c757e25a8ca12c92a25eb1c1d446ea9e83258",
            "Host": "twitter.com",
            "Referer": "https://twitter.com/search?q=rest&src=typed_query",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "TE": "trailers",
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
            "x-client-transaction-id": "TXRIy4o94QQeb6OCWOo4UXI3Wu2wBd28WyN9cO/pia5vTEDBFuTNATyMvnM2E4IknJXVkEzMqTnWPSIMo3c0b4kXDRw0Tg",
            "X-Client-UUID": "ef8cf8ec-75f1-4209-bc57-a01fc78d0562",
            "x-csrf-token": "0987765d52578a8c3eb86c3047fb472e00f13a3465e58175bbe6e1c0721291656c87b6ef841538cc1446c16b3095152d0d5d35f3fbbd9bf289bc2e4545b25794c4142b93a308926e990afbce4758b243",
            "x-twitter-active-user": "yes",
            "x-twitter-auth-type": "OAuth2Session",
            "x-twitter-client-language": "en"
            }

user_header_4 = {
  "Accept": "*/*",
  "Accept-Encoding": "gzip, deflate, br",
  "Accept-Language": "en-US,en;q=0.5",
  "Authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
  "Connection": "keep-alive",
  "Cookie": "g_state={\"i_l\":0}; kdt=jdeWo36eab8HLeYm861t6VOrOgR2lnmeBjQm8zB0; des_opt_in=Y; dnt=1; d_prefs=MjoxLGNvbnNlbnRfdmVyc2lvbjoyLHRleHRfdmVyc2lvbjoxMDAw; lang=en; gt=1784207582267101521; att=1-1nezxRXReWkvKH2RIzDN9o5WJEh43tAqaUQWRP7R; guest_id_marketing=v1%3A171422428031861532; guest_id_ads=v1%3A171422428031861532; personalization_id=\"v1_NR0cU0hm9e2ARVuMEEBPsg==\"; guest_id=v1%3A171422428031861532; _twitter_sess=BAh7CSIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7ADoPY3JlYXRlZF9hdGwrCK8kux%252BPAToMY3NyZl9p%250AZCIlY2U3ZjQ0N2I0Y2M2NmRkNDZlOGE1OTViNTk0ZDYxMDY6B2lkIiVlNGFk%250AYTQ2NzBlMzVmMzU5Y2Q2ZDBhZmIyYTg5YmI3OA%253D%253D--de20247ee93cc0a61c88a822a02a6baf5d223b9a; twid=u%3D1784212317934161920; ct0=894e367c66d8103ceb1521a60af8a929873705f1c047f1b2337ce01077dae108f32607512fba5f47b1f83185c8b8591da0f2575e9bf7049da1c2f77ee9e79bb2090add27a1288894427a9e7d22efc505; auth_token=d46d0859b8eafc3ddbac9a6b71de93660f05f16d",
  "Host": "twitter.com",
  "If-None-Match": "184876e0a8f04195126fa37c9f7065c9",
  "Referer": "https://twitter.com/search?q=test&src=typed_query",
  "Sec-Fetch-Dest": "empty",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Site": "same-origin",
  "TE": "trailers",
  "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
  "x-client-transaction-id": "MwoMw9uk2RGboWRD/gneK9YCNwIvcDsL8HZeUI/7tIVUBDC7ZjoOV0sFQ5oEcJ7wbamq7jK2yanh8tGv7Kx7Oltt5LXLMA",
  "X-Client-UUID": "db1d6502-651d-4216-8773-a7a043ac67ba",
  "x-csrf-token": "894e367c66d8103ceb1521a60af8a929873705f1c047f1b2337ce01077dae108f32607512fba5f47b1f83185c8b8591da0f2575e9bf7049da1c2f77ee9e79bb2090add27a1288894427a9e7d22efc505",
  "x-twitter-active-user": "yes",
  "x-twitter-auth-type": "OAuth2Session",
  "x-twitter-client-language": "en"
}

user_header_5 = {
  "Accept": "*/*",
  "Accept-Encoding": "gzip, deflate, br",
  "Accept-Language": "en-US,en;q=0.5",
  "Authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
  "Connection": "keep-alive",
  "Content-Type": "application/json",
  "Cookie": "g_state={\"i_l\":1,\"i_p\":1714231700304}; kdt=jdeWo36eab8HLeYm861t6VOrOgR2lnmeBjQm8zB0; des_opt_in=Y; dnt=1; d_prefs=MjoxLGNvbnNlbnRfdmVyc2lvbjoyLHRleHRfdmVyc2lvbjoxMDAw; lang=en; gt=1784207582267101521; att=1-1nezxRXReWkvKH2RIzDN9o5WJEh43tAqaUQWRP7R; guest_id=v1%3A171422448896436865; _twitter_sess=BAh7CSIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7ADoHaWQiJTBmNDQwNjUyOWQ4ODk3NTMwYjRhNjRi%250AMjA2NjUyNDY4Og9jcmVhdGVkX2F0bCsI9eq%252BH48BOgxjc3JmX2lkIiU3YzMw%250ANWFiZmIwZDg3OWZlMWQwZGQ1MzAxOGY0YWI1Yw%253D%253D--6b454dadab27cacef74b335b7ecfd152d0b953f0; twid=u%3D1784213185559465984; ct0=8effcf48e776fe74b3bc001df70213c103988d8333355eef62809d8551ac766f2bfb38dbd2ab3920d5218692f80e1282d3371aee873d5560961470a0950d1c85066efbf9a708713f6a455528be2185c8; auth_token=e8a45fc8f0a9b29d24e7a0cecad55f304cb871c9; guest_id_marketing=v1%3A171422448896436865; guest_id_ads=v1%3A171422448896436865; personalization_id=\"v1_zD7b9kCxQ3qeyBrHTVBA4g==\"",
  "Host": "twitter.com",
  "Referer": "https://twitter.com/search?q=test&src=typed_query",
  "Sec-Fetch-Dest": "empty",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Site": "same-origin",
  "TE": "trailers",
  "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
  "x-client-transaction-id": "XfAN957NpA48/zWuUmNvCrTmu8LT2sVYlOhy76xpIlr5aflxq+OS0UtNXhuHvqYw3SnHgFymHBORFFiazMNYcVf3t71sXg",
  "X-Client-UUID": "10e85e7f-9c39-4c4a-a7d2-7020d0c18b5d",
  "x-csrf-token": "8effcf48e776fe74b3bc001df70213c103988d8333355eef62809d8551ac766f2bfb38dbd2ab3920d5218692f80e1282d3371aee873d5560961470a0950d1c85066efbf9a708713f6a455528be2185c8",
  "x-twitter-active-user": "yes",
  "x-twitter-auth-type": "OAuth2Session",
  "x-twitter-client-language": "en"
}


def get_next_topic(json_file_path='dags/topics/topic.json'):
    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            topics = json.load(file)
            log_value = read_from_log()
            #print("Log value:", log_value)
            #print("Number of topics:", len(topics.keys()))

            if len(topics.keys()) == log_value:
                write_to_log(new_line="1")
                keep_last_line_only()
                return {}

            else:
                key_index = log_value
                key = list(topics.keys())[key_index]
                #keep_last_line_only()
                write_to_log(new_line=str(log_value + 1))
                #print("Returning topic:", {key: topics[key]})
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

last_keyword = None

def get_random_user():
    global last_keyword
    keywords = [user_header_1,user_header_2,user_header_3,user_header_4,user_header_5]
    
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
        #print("Content-Encoding:", response.headers.get('Content-Encoding'))

        if response.headers.get('Content-Encoding') == 'br':
            # Decompress the response content
            try:
                response_content = brotli.decompress(response.content)
            except Exception as e:
                #print("Error decompressing content:", e)
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
    
def extract_batch_data(**kwargs):
  tweet_topics = get_next_topic()
  header = get_random_user()
  raw_data = []
  for _, values in tweet_topics.items():
      for value in values:
          try:
            response_json = extract_json_data(value,header=header)
            if get_response_count(response_json) > 0:
                raw_data.append(response_json)
            else:
                break
            for i in range(10):
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
  kwargs['ti'].xcom_push(key='topic_key', value= list(tweet_topics.keys())[0]) 
  
  return True,temp_file

def transform_batch_data(**kwargs):
  
  temp_file = kwargs['ti'].xcom_pull(task_ids='extract_tweets_task', key='raw_batch_data_key')
  topic = kwargs['ti'].xcom_pull(task_ids='extract_tweets_task', key='topic_key')

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
  
  return True,temp_file,len(tweets_data)

def load_to_mongodb(**kwargs):
    
    temp_file = kwargs['ti'].xcom_pull(task_ids='transform_tweets_task', key='clean_data_key')

    data_to_insert = read_json_file(file_path=temp_file)

    #with open("dags/topics/data.json", 'w') as json_file:
    #    json.dump(data_to_insert, json_file, indent=4)

    # MongoDB connection URL
    url = "mongodb+srv://mlteam:mlteam1234@cluster0.6y3bpz0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

    # Connect to MongoDB
    #client = MongoClient(url)
    client = MongoClient(url, server_api=ServerApi('1'))
    print("test0")
    try:
        #client.admin.command('ping') 
        #print("Pinged your deployment. You successfully connected to MongoDB!")
        # Access the database
        db = client.get_database("TweetsDataBase")
        print("test1")
        # Access or create the collection
        collection = db.get_collection("TweetsData")
        print("test2",collection)
        # Insert data into the collection
        collection.insert_many(data_to_insert)
        print("test3")
        return True,len(data_to_insert)
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return False,len(data_to_insert)
    finally:
        # Close the connection
        client.close()
        
default_args = {
    'owner' : 'MLTeam',
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5)
}

with DAG(
    dag_id='tweets_ETL',
    default_args=default_args,
    description= 'This an ETL pipeline',
    start_date=datetime(2024,4,27),
    schedule_interval='@hourly'
) as dag: 
    
    # Define tasks for each topic
    extract_task = PythonOperator(
        task_id='extract_tweets_task',
        python_callable=extract_batch_data
    )

    transform_task = PythonOperator(
            task_id='transform_tweets_task',
            python_callable=transform_batch_data
        )

    load_task = PythonOperator(
            task_id='load_tweets_to_mongodb_task',
            python_callable=load_to_mongodb
        )

    # Set task dependencies
    extract_task >> transform_task >> load_task