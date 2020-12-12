# source: https://pythondata.com/collecting-storing-tweets-with-python-and-mongodb/
# import libraries

from __future__ import print_function
import tweepy
import json
from pymongo import MongoClient

# Define mongo db host

MONGO_HOST = 'mongodb://35.234.63.96/db'  # your ext-ip address, localhost

# assuming you have mongoDB installed locally
# and a database called 'twitterbnk48'

# Your words that you want to collect

WORDS = ['cyberpunk',' ']

# Twitter Application Management
# https://apps.twitter.com/

CONSUMER_KEY = "MX0bQ0au37THr1LVK0iPob2iG"
CONSUMER_SECRET = "8eiVLnLJlfbgEi0tYzUT4Fj9R5ZWbiCXe94lrelnoL8P4tzZay"
ACCESS_TOKEN = "176797692-I8kg44mU6ffpMWgRVxPapFJOxUMIavhPc0ECE690"
ACCESS_TOKEN_SECRET = "RVRILBTJzb73d5irF5Atpwwq292BLBeZEn1OF3jdlA7Vk"

# Collect your word from twiiter

class StreamListener(tweepy.StreamListener):    
    #This is a class provided by tweepy to access the Twitter Streaming API. 
 
    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")
 
    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False
 
    def on_data(self, data):
        #This is the meat of the script...it connects to your mongoDB and stores the tweet
        try:
            client = MongoClient(MONGO_HOST)
            
            # Use twitterbnk48 database. If it doesn't exist, it will be created.
            db = client.db
    
            # Decode the JSON from Twitter
            datajson = json.loads(data)
            
            #grab the 'created_at' data from the Tweet to use for display
            created_at = datajson['created_at']
 
            #print out a message to the screen that we have collected a tweet
            print("Tweet collected at " + str(created_at))
            
            #insert the data into the mongoDB into a collection called twitter_search
            #if twitter_search doesn't exist, it will be created.
            db.twitter_search.insert(datajson)
        except Exception as e:
           print(e)
 
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
#Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True)) 
streamer = tweepy.Stream(auth=auth, listener=listener)
print("Tracking: " + str(WORDS))
streamer.filter(track=WORDS)
