from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import brotli
import time
from pymongo.mongo_client import MongoClient

def extract_json_data(search,next_page=""):
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
    headers = {
          "Accept": "*/*",
          "Accept-Encoding": "gzip, deflate, br",
          "Accept-Language": "en-US,en;q=0.5",
          "authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
          "Connection": "keep-alive",
          "content-type": "application/json",
          "Cookie": "g_state={\"i_l\":0}; kdt=jdeWo36eab8HLeYm861t6VOrOgR2lnmeBjQm8zB0; des_opt_in=Y; dnt=1; lang=en; d_prefs=MjoxLGNvbnNlbnRfdmVyc2lvbjoyLHRleHRfdmVyc2lvbjoxMDAw; guest_id=v1%3A171331275986956815; gt=1780389843245154599; twid=u%3D1780389902699401217; ct0=a4340c3c75d5eee745277932bae7d0c01fb3a203e1a1dc33212faca210fd1041c57850e05c5cf8f2263e0ee855199b9c77ac1cb5b6af2d8d63981c9c666a2cb93d20da68c93b1b593e621118ce1319c1; auth_token=d788596838a1244ba1a651572277b9fb7ce139a1; guest_id_marketing=v1%3A171331275986956815; guest_id_ads=v1%3A171331275986956815; personalization_id=\"v1_7F3bjEu2h8DWBNcQjX5VKw==\"",
          "Host": "twitter.com",
          "Sec-Fetch-Dest": "empty",
          "Sec-Fetch-Mode": "cors",
          "Sec-Fetch-Site": "same-origin",
          "TE": "trailers",
          "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
          "x-client-transaction-id": "mtKxi8974foGRAMPFY44merEYKx1ma55xqiE8UJ0ndomVtcGbOVBqVVIbn1D//MSeLMoVZsaN1bKGDLFTSRUWkHVdvg6mQ",
          "X-Client-UUID": "4198c768-ae7c-4f08-b296-628102e0ac82",
          "x-csrf-token": "a4340c3c75d5eee745277932bae7d0c01fb3a203e1a1dc33212faca210fd1041c57850e05c5cf8f2263e0ee855199b9c77ac1cb5b6af2d8d63981c9c666a2cb93d20da68c93b1b593e621118ce1319c1",
          "x-twitter-active-user": "yes",
          "x-twitter-auth-type": "OAuth2Session",
          "x-twitter-client-language": "en"
      }

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
    
def extract_batch_data(tweet_topics):
  raw_data = []
  for topic, values in tweet_topics.items():
      for value in values:
          try:
            response_json = extract_json_data(value)
            if get_response_count(response_json) > 0:
                raw_data.append(response_json)
            else:
                break
            for i in range(10):
              if i == 0 :
                  next_page = len(response_json['data']['search_by_raw_query']['search_timeline']['timeline']['instructions'][0]['entries'])-1
                  next_page = response_json['data']['search_by_raw_query']['search_timeline']['timeline']['instructions'][0]['entries'][next_page]['content']['value']
                  response_json = extract_json_data(value,next_page)
                  if get_response_count(response_json) > 0:
                      raw_data.append(response_json)
                  else:
                      break
              else :
                  next_page = response_json['data']['search_by_raw_query']['search_timeline']['timeline']['instructions'][2]['entry']['content']['value']
                  response_json = extract_json_data(value,next_page)
                  if get_response_count(response_json) > 0:
                      raw_data.append(response_json)
                  else:
                      break
              time.sleep(1.5)
          except Exception as ex:
              print(ex)
          time.sleep(0.7)
  return raw_data

def transform_batch_data(raw_data,topic):
  
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

  return tweets_data

def load_to_mongodb(data_to_insert):
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

tweet_topics = {
    'Politics': [
        'الحكومة المغربية', 'gouvernement marocain', 'Moroccan government',
        'الانتخابات في المغرب', 'élections au Maroc', 'elections in Morocco',
        'السياسة الخارجية للمغرب', 'politique étrangère du Maroc', "Morocco's foreign policy"
    ],
    'Economy': [
        'النمو الاقتصادي في المغرب', 'croissance économique au Maroc', 'economic growth in Morocco',
        'سوق العمل المغربي', 'marché du travail marocain', 'Moroccan job market',
        'الاستثمار في المغرب', 'investissement au Maroc', 'investment in Morocco'
    ],
    'Culture': [
        'الفن المغربي', 'art marocain', 'Moroccan art',
        'التراث المغربي', 'patrimoine marocain', 'Moroccan heritage',
        'الأدب المغربي', 'littérature marocaine', 'Moroccan literature'
    ],
    'Social Issues': [
        'الفقر في المغرب', 'pauvreté au Maroc', 'poverty in Morocco',
        'التعليم في المغرب', 'éducation au Maroc', 'education in Morocco',
        'الصحة في المغرب', 'santé au Maroc', 'healthcare in Morocco'
    ],
    'Technology': [
        'الابتكار التكنولوجي في المغرب', 'innovation technologique au Maroc', 'technological innovation in Morocco',
        'شركات التكنولوجيا في المغرب', 'entreprises technologiques au Maroc', 'technology companies in Morocco',
        'تطور التكنولوجيا في المغرب', 'évolution technologique au Maroc', 'technological advancement in Morocco'
    ],
    'Environment': [
        'التلوث في المغرب', 'pollution au Maroc', 'pollution in Morocco',
        'حماية البيئة في المغرب', 'protection de l\'environnement au Maroc', 'environmental protection in Morocco',
        'التغير المناخي في المغرب', 'changement climatique au Maroc', 'climate change in Morocco'
    ],
    'Sport': ['رياضة في المغرب', 'sport au Maroc', 'sport in Morocco',
    'كرة القدم المغربية', 'football marocain', 'Moroccan football',
    'الألعاب الأولمبية في المغرب', 'Jeux olympiques au Maroc', 'Olympic Games in Morocco'
    ],
    "Education Reform": [
    "إصلاح التعليم في المغرب",
    "réforme de l'éducation au Maroc",
    "education reform in Morocco"
  ],
  "Youth Empowerment": [
    "تمكين الشباب في المغرب",
    "autonomisation des jeunes au Maroc",
    "youth empowerment in Morocco"
  ],
  "Digital Transformation": [
    "التحول الرقمي في المغرب",
    "transformation numérique au Maroc",
    "digital transformation in Morocco"
  ],
  "Gender Equality": [
    "المساواة بين الجنسين في المغرب",
    "égalité des genres au Maroc",
    "gender equality in Morocco"
  ],
  "Infrastructure Development": [
    "تطوير البنية التحتية في المغرب",
    "développement des infrastructures au Maroc",
    "infrastructure development in Morocco"
  ],
  "Cultural Exchange": [
    "تبادل ثقافي بين المغرب والعالم",
    "échange culturel entre le Maroc et le monde",
    "cultural exchange between Morocco and the world"
  ],
  "Entrepreneurship": [
    "ريادة الأعمال في المغرب",
    "entrepreneuriat au Maroc",
    "entrepreneurship in Morocco"
  ],
  "Tourism": [
    "السياحة في المغرب",
    "tourisme au Maroc",
    "tourism in Morocco"
  ],
  "Public Health Policies": [
    "سياسات الصحة العامة في المغرب",
    "politiques de santé publique au Maroc",
    "public health policies in Morocco"
  ],
  "Rural Development": [
    "تنمية الريف في المغرب",
    "développement rural au Maroc",
    "rural development in Morocco"
  ]
}

# Define the DAG settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'tweet_topics_processing',
    default_args=default_args,
    description='Process tweet topics data',
    schedule_interval='@hourly',
)

# Define and chain tasks for each topic
for topic, keywords in tweet_topics.items():
    # Define tasks for extract, transform, and load
    extract_task = PythonOperator(
        task_id=f'extract_{topic.lower()}',
        python_callable=extract_batch_data,
        op_args=[tweet_topics, topic],
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id=f'transform_{topic.lower()}',
        python_callable=transform_batch_data,
        op_args=[topic],
        dag=dag,
    )

    load_task = PythonOperator(
        task_id=f'load_{topic.lower()}',
        python_callable=load_to_mongodb,
        op_args=[topic],
        dag=dag,
    )

    # Chain tasks
    extract_task >> transform_task >> load_task
