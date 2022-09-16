import sys
import tweepy 
import re
from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer
import csv
#import matplotlib.pyplot as plt

api_key = "BAbbm3LZTdxsvzDKocfLBlSGb"
api_key_secret = "IZTYrFUUXJhNRauRxP95NkDgn7TsVymBPhsIICQFH2ZTulHHho"
access_token = "1527657951317610504-7jMeN9wf8beUePhxNBfqojSa5k7wUl"
access_token_secret = "Mmm1yQDKNDW2L09Ut4Rgvgyzs4lnvJzZuay7ga8vhDAG6"

# Authentification pour utiliser API twitter
def auth_cpt_twtr(consumer_key,consumer_secret,access_token,access_secret):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    try:
        api.verify_credentials()
        print('Successful Authentication')
        return api
    except:
        print('Failed authentication')
        exit()

def nlp_pipeline(dict_equipes:dict):
    clean_tweets_equipes = {}
    for equipe, tweets_equipe in dict_equipes.items():
        print(equipe)
        clean_tweets_equipe = []
        for text in tweets_equipe:
            print(text)
            text = re.sub(r"https?:\/\/\S+", "", text)
            text = text.lower()
            text = text.replace('\n', ' ').replace('\r', ' ')
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
            text = re.sub(r"\-", " ", text)
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
            print(text)
            clean_tweets_equipe.append(text)
        clean_tweets_equipes[equipe] = clean_tweets_equipe

    return clean_tweets_equipes

#chercher les tweets par les noms des équipes et par comptes
def liste_equipes(path) -> set():
    lst_equipes = set()
    with open(path,mode='r') as clndr:
        csv_reader = csv.reader(clndr,delimiter=',',lineterminator='\n')
        next(csv_reader)
        for line in csv_reader:
            lst_equipes.add(line[1])
        return lst_equipes

def nom_normalizer_q_twtr(equipe_a_normaliser):
    teams = {
        '#OL': ['OLYMPIQUE LYONNAIS', 'LYON', 'OL', 'O.L.'], 
        '#ACAjaccio': ['AC AJACCIO', 'AJACCIO', 'ACA', 'A.C.A', 'ATHLETIC CLUB AJACCIEN','A-C AJACCIO','A.C AJACCIO'],
        '#RCSA' : ['STRASBOURG','RACING CLUB DE STRASBOURG ALSACE', 'RC STRASBOURG','R.C. STRASBOURG','R.C.S.','RCS', 'R.C.S.A.', 'RC STRASBOURG ALSACE', 'R.C. STRASBOURG ALSACE'], 
        '#ASMonaco OR #ASM': ['AS MONACO', 'MONACO', 'ASSOCIATION SPORTIVE DE MONACO FOOTBALL CLUB', 'AS MONACO FC', 'A.S MONACO F.C', 'ASM'], 
        '#CF63': ['CLERMONT FOOT 63', 'CLERMONT-FERRAND', 'CF63', 'C.F.63', 'C-F 63', 'CLERMONT', 'CLERMONT FOOT'], 
        '#PSG': ['PARIS SAINT GERMAIN', 'PARIS', 'PARIS-SAINT-GERMAIN FOOTBALL CLUB', 'PSG', 'P.S.G.', 'PARIS SG', 'PARIS-SG', 'PARIS SAINT-GERMAIN', 'PARIS SAINT-GERMAIN FOOTBALL CLUB', 'PARIS SAINT-GERMAIN FC'], 
        '#TFC' : ['TOULOUSE', 'TOULOUSE FOOTBALL CLUB', 'TFC', 'T.F.C.', 'TOULOUSE FC', 'TOULOUSE F.C.'], 
        '#OGCNice': ['NICE', "OLYMPIQUE GYMNASTE CLUB NICE COTE D'AZUR", 'OGC NICE', 'O.G.C. NICE', "OGC NICE COTE D'AZUR", "O.G.C. NICE COTE D'AZUR", 'OLYMPIQUE GYMNASTE CLUB NICE'], 
        '#SCO': ['ANGERS SCO', 'ANGERS', "SCO D'ANGERS", 'SCOA', "ANGERS SPORTING CLUB DE L'OUEST", 'SCO ANGERS', 'ANGERS-SCO', 'ANGERS S.C.O.', 'S.C.O. ANGERS'], 
        '#FCNantes OR #FCN' : ['FC NANTES', 'NANTES', 'FOOTBALL CLUB DE NANTES', 'F.C. NANTES', 'FCN', 'F.C.N.'], 
        '#RCLens OR #RCL': ['RC LENS', 'LENS', 'RACING CLUB DE LENS', 'RCL', 'R.C.L.', 'R.C. LENS'], 
        '#SB29': ['STADE BRESTOIS', 'BREST', 'STADE BRESTOIS 29', 'SB29', 'S.B.29', 'S.B. 29'], 
        '#LOSC': ['LOSC LILLE', 'LILLE', 'LOSC', 'LILLE OLYMPIQUE SPORTING CLUB', 'L.O.S.C.', 'L.O.S.C. LILLE'], 
        '#TeamAJA': ['AJ AUXERRE', 'AUXERRE', 'AJA', 'ASSOCIATION DE LA JEUNESSE AUXERROISE', 'A.J.AUXERRE', 'A.J AUXERRE'], 
        '#MHSC': ['MONTPELLIER HÃ‰RAULT SC', 'MONTPELLIER HÉRAULT SC','MONTPELLIER HÉRAULT', 'MONTPELLIER', 'MONTPELLIER-HERAULT SPORT CLUB', 'MONTPELLIER HERAULT SPORT CLUB', 'MONTPELLIER HSC', 'MHSC', 'MONTPELLIER-HERAULT SC', 'MONTPELLIER-HÉRAULT SC', 'MONTPELLIER-HERAULT S.C.', 'M.H.S.C.', 'MONTPELLIER HERAULT SC', 'MONTPELLIER HÉRAULT SC', 'MONTPELLIER HERAULT S.C.'], 
        '#ESTAC': ['TROYES', 'ESPERANCE SPORTIVE TROYES AUBE CHAMPAGNE', 'ESTAC', 'E.S.T.A.C.', 'ESTAC TROYES', 'E.S.T.A.C. TROYES'], 
        '#SRFC': ['RENNES', 'STADE RENNAIS FOOTBALL CLUB', 'STADE RENNAIS', 'STADE RENNAIS FC', 'S.R.F.C.', 'STADE RENNAIS F.C.'], 
        '#FCLorient': ['FC LORIENT', 'LORIENT', 'FOOTBALL CLUB LORIENT BRETAGNE SUD', 'FCL', 'F.C.L.', 'F.C. LORIENT', 'FC LORIENT BRETAGNE SUD', 'FOOTBALL CLUB LORIENTAIS'], 
        '#OM OR #teamOM': ['OLYMPIQUE DE MARSEILLE', 'MARSEILLE', 'OM', 'O.M.'],
        '#GoSDR': ['STADE DE REIMS', 'REIMS', 'STADE REIMS']}
    resultat = equipe_a_normaliser
    for equipe_name in teams:
        if equipe_a_normaliser in teams[equipe_name]:
            return equipe_name
    return resultat

sys.stdout.reconfigure(encoding='utf-8')
api = auth_cpt_twtr(api_key, api_key_secret, access_token, access_token_secret)

equipes = list(liste_equipes('./DB_golden_merging_calendrier_6.csv'))

all_tweets = {}
for equipe in equipes:
    tweets_equipe = []
    requete = nom_normalizer_q_twtr(equipe)
    print(requete)
    tweets = tweepy.Cursor(api.search_tweets,
                    q= requete,
                    lang= "fr").items(10)
    for tweet in tweets:
        tweets_equipe.append(tweet.text)
    all_tweets[equipe] = tweets_equipe
#print(all_tweets)

#pipeline de nettoyage : enlever les ponctuations, emojis, mettre en miniscule...
tweets_clean = nlp_pipeline(all_tweets)
#print('all tweets is '+str(all_tweets))
print('clean tweets is '+str(tweets_clean))

#calculer la polarité des tweets
with open('./polarity.csv', mode='w') as plt:
    csv_writer = csv.writer(plt)
    for equipe, tweets_clean_eq in tweets_clean.items():
        i=0
        polarity=0
        for tweet in tweets_clean_eq:
            polarity+=TextBlob(tweet,pos_tagger=PatternTagger(),analyzer=PatternAnalyzer()).sentiment[0]
            i+=1
        print(polarity)
        avg_polarity = polarity/i
        print(avg_polarity)
        plt.writelines(str(equipe)+','+str(avg_polarity)+'\n')
plt.close()
