#import os
import tweepy 
import re
from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer
import matplotlib.pyplot as plt

api_key = "BAbbm3LZTdxsvzDKocfLBlSGb"
api_key_secret = "IZTYrFUUXJhNRauRxP95NkDgn7TsVymBPhsIICQFH2ZTulHHho"
access_token = "1527657951317610504-7jMeN9wf8beUePhxNBfqojSa5k7wUl"
access_token_secret = "Mmm1yQDKNDW2L09Ut4Rgvgyzs4lnvJzZuay7ga8vhDAG6"

# Authentification pour utiliser API twitter

def auth_cpt_twtr(consumer_key,consumer_secret,access_token,access_secret):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)
    try:
        api.verify_credentials()
        print('Successful Authentication')
        return api
    except:
        print('Failed authentication')
        exit()

api = auth_cpt_twtr(api_key, api_key_secret, access_token, access_token_secret)

#chercher les tweets par les noms des équipes(j'ai fait le choix de 1000 tweets)

requete = '"AS MONACO"'

tweets = tweepy.Cursor(api.search_tweets,
                   q = requete,
                   lang = "fr").items(50)

all_tweets = [tweet.text for tweet in tweets]

print(all_tweets)

#pipeline de nettoyage : enlever les ponctuations, emojis, mettre en miniscule...

def nlp_pipeline(tweets:list):
    clean_tweets = []
    for text in tweets:


        text = text.lower()
        text = text.replace('\n', ' ').replace('\r', '')
        text = ' '.join(text.split())
        text = re.sub(r"[A-Za-z\.]*[0-9]+[A-Za-z%°\.]*", "", text)
        text = re.sub(r"(\s\-\s|-$)", "", text)
        text = re.sub(r"[,\!\?\%\(\)\/\"]", "", text)
        text = re.sub(r"\&\S*\s", "", text)
        text = re.sub(r"\&", "", text)
        text = re.sub(r"\+", "", text)
        text = re.sub(r"\#", "", text)
        text = re.sub(r"\$", "", text)
        text = re.sub(r"\£", "", text)
        text = re.sub(r"\%", "", text)
        text = re.sub(r"\:", "", text)
        text = re.sub(r"\@", "", text)
        text = re.sub(r"\-", "", text)
        text = re.compile("["
                u"\U0001F600-\U0001F64F" 
                u"\U0001F300-\U0001F5FF" 
                u"\U0001F680-\U0001F6FF"  
                u"\U0001F1E0-\U0001F1FF" 
                u"\U00002702-\U000027B0"
                u"\U000024C2-\U0001F251"
                u"\U0001f926-\U0001f937"
                u'\U00010000-\U0010ffff'
                u"\u200d"
                u"\u2640-\u2642"
                u"\u2600-\u2B55"
                u"\u23cf"
                u"\u23e9"
                u"\u231a"
                u"\u3030"
                u"\ufe0f" 
                           "]+", flags=re.UNICODE).sub(r'', text)
        clean_tweets.append(text)

    return clean_tweets
tweets_clean = nlp_pipeline(all_tweets)
#print(tweets)

# calculer la polarité des tweets

polarity = []
somme_pol = 0
nb_pol = 0
for tweet in tweets_clean:
  polarity.append(TextBlob(tweet,pos_tagger=PatternTagger(),analyzer=PatternAnalyzer()).sentiment[0])
  somme_pol += polarity[nb_pol]
  nb_pol += 1

#print(polarity)

moyenne_pol = somme_pol / nb_pol

print(moyenne_pol)
print(requete)