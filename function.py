import os
import tweepy
import pandas as pd
import json

def nom_normalizer(equipe_a_normaliser):
    teams = {
        'OLYMPIQUE LYONNAIS': ['OLYMPIQUE LYONNAIS', 'LYON', 'OL', 'O.L.'], 
        'AC AJACCIO': ['AC AJACCIO', 'AJACCIO', 'ACA', 'A.C.A', 'ATHLETIC CLUB AJACCIEN','A-C AJACCIO','A.C AJACCIO'],
        'RC STRASBOURG ALSACE' : ['STRASBOURG','RACING CLUB DE STRASBOURG ALSACE', 'RC STRASBOURG','R.C. STRASBOURG','R.C.S.','RCS', 'R.C.S.A.', 'RC STRASBOURG ALSACE', 'R.C. STRASBOURG ALSACE'], 
        'AS MONACO': ['AS MONACO', 'MONACO', 'ASSOCIATION SPORTIVE DE MONACO FOOTBALL CLUB', 'AS MONACO FC', 'A.S MONACO F.C', 'ASM'], 
        'CLERMONT FOOT 63': ['CLERMONT FOOT 63', 'CLERMONT-FERRAND', 'CF63', 'C.F.63', 'C-F 63', 'CLERMONT FOOT'], 
        'PARIS SAINT GERMAIN': ['PARIS SAINT GERMAIN', 'PARIS', 'PARIS-SAINT-GERMAIN FOOTBALL CLUB', 'PSG', 'P.S.G.', 'PARIS SG', 'PARIS-SG', 'PARIS SAINT-GERMAIN', 'PARIS SAINT-GERMAIN FOOTBALL CLUB', 'PARIS SAINT-GERMAIN FC'], 
        'TOULOUSE FC' : ['TOULOUSE', 'TOULOUSE FOOTBALL CLUB', 'TFC', 'T.F.C.', 'TOULOUSE FC', 'TOULOUSE F.C.'], 
        'OGC NICE': ['NICE', "OLYMPIQUE GYMNASTE CLUB NICE COTE D'AZUR", 'OGC NICE', 'O.G.C. NICE', "OGC NICE COTE D'AZUR", "O.G.C. NICE COTE D'AZUR", 'OLYMPIQUE GYMNASTE CLUB NICE'], 
        'ANGERS SCO': ['ANGERS SCO', 'ANGERS', "SCO D'ANGERS", 'SCOA', "ANGERS SPORTING CLUB DE L'OUEST", 'SCO ANGERS', 'ANGERS-SCO', 'ANGERS S.C.O.', 'S.C.O. ANGERS'], 
        'FC NANTES' : ['FC NANTES', 'NANTES', 'FOOTBALL CLUB DE NANTES', 'F.C. NANTES', 'FCN', 'F.C.N.'], 
        'RC LENS': ['RC LENS', 'LENS', 'RACING CLUB DE LENS', 'RCL', 'R.C.L.', 'R.C. LENS'], 
        'STADE BRESTOIS': ['STADE BRESTOIS', 'BREST', 'STADE BRESTOIS 29', 'SB29', 'S.B.29', 'S.B. 29'], 
        'LOSC LILLE': ['LOSC LILLE', 'LILLE', 'LOSC', 'LILLE OLYMPIQUE SPORTING CLUB', 'L.O.S.C.', 'L.O.S.C. LILLE'], 
        'AJ AUXERRE': ['AJ AUXERRE', 'AUXERRE', 'AJA', 'ASSOCIATION DE LA JEUNESSE AUXERROISE', 'A.J.AUXERRE', 'A.J AUXERRE'], 
        'MONTPELLIER HÉRAULT SC': ['MONTPELLIER HÉRAULT SC','MONTPELLIER HÉRAULT', 'MONTPELLIER', 'MONTPELLIER-HERAULT SPORT CLUB', 'MONTPELLIER HERAULT SPORT CLUB', 'MONTPELLIER HSC', 'MHSC', 'MONTPELLIER-HERAULT SC', 'MONTPELLIER-HÉRAULT SC', 'MONTPELLIER-HERAULT S.C.', 'M.H.S.C.', 'MONTPELLIER HERAULT SC', 'MONTPELLIER HÉRAULT SC', 'MONTPELLIER HERAULT S.C.'], 
        'ESTAC TROYES': ['TROYES', 'ESPERANCE SPORTIVE TROYES AUBE CHAMPAGNE', 'ESTAC', 'E.S.T.A.C.', 'ESTAC TROYES', 'E.S.T.A.C. TROYES'], 
        'STADE RENNAIS FC': ['RENNES', 'STADE RENNAIS FOOTBALL CLUB', 'STADE RENNAIS', 'STADE RENNAIS FC', 'S.R.F.C.', 'STADE RENNAIS F.C.'], 
        'FC LORIENT': ['FC LORIENT', 'LORIENT', 'FOOTBALL CLUB LORIENT BRETAGNE SUD', 'FCL', 'F.C.L.', 'F.C. LORIENT', 'FC LORIENT BRETAGNE SUD', 'FOOTBALL CLUB LORIENTAIS'], 
        'OLYMPIQUE DE MARSEILLE': ['OLYMPIQUE DE MARSEILLE', 'MARSEILLE', 'OM', 'O.M.'],
        'STADE DE REIMS': ['STADE DE REIMS', 'REIMS', 'STADE REIMS'],
        'DIJON': ['DIJON', 'DIJON FCO', 'DFCO', "DIJON FOOTBALL COTE-D'OR", "DIJON FOOTBALL CÔTE-D'OR"],
        'SAINT ETIENNE': ['ST ETIENNE', 'ST-ETIENNE', 'ST ÉTIENNE', 'ST-ÉTIENNE', 'SAINT ETIENNE', 'SAINT-ETIENNE', 'SAINT ÉTIENNE', 'SAINT-ÉTIENNE', 'ASSE','AS SAINT ETIENNE', 'AS SAINT-ETIENNE', 'AS SAINT-ÉTIENNE', 'AS SAINT-ÉTIENNE'],
        'BORDEAUX': ['BORDEAUX', 'FOOTBALL CLUB DES GIRONDINS DE BORDEAUX', 'FC GIRONDINS DE BORDEAUX', 'GIRONDINS DE BORDEAUX', 'FCG BORDEAUX', 'FOOTBALL CLUB GIRONDINS DE BORDEAUX'],
        'METZ': ['METZ', 'FC METZ', 'FOOTBALL CLUB DE METZ', 'FOOTBALL CLUB METZ']}
    resultat = equipe_a_normaliser
    for equipe_name in teams:
        if equipe_a_normaliser in teams[equipe_name]:
            return equipe_name
    return resultat

def merge_season(season, path):
    directory = f"./Data_final/"
    if not os.path.exists(directory):
        os.mkdir(directory)
    merged =open(directory+f"season_{season}_{season+1}.csv","a")    
    merged.write('Journee,HomeTeam,AwayTeam,FTHG,FTAG,URL\n')  
    for num in range(1,39):
        f = open(path+"/season_"+str(season)+"_"+str(season+1)+"_journee_"+str(num)+".csv")
        next(f) # skip the header
        for line in f:
            merged.write(f'{num},'+line)
        f.close() # not really needed
    merged.close()

def merge_ligue1(season1,season2):
    merged_F =open("./Data_final/data_ligue1.csv","a")    
    merged_F.write('Season,Journee,HomeTeam,AwayTeam,FTHG,FTAG,URL\n')  
    for num in range(season1,season2+1):
        ff = open("season_"+str(season1)+"_"+str(season1+1)+".csv")
        next(ff) # skip the header
        for line in ff:
            merged_F.write(f'{num}-{num+1},'+line)
        ff.close() # not really needed
    merged_F.close()

def scrap_to_list(tree,xpth,scrape_type):
    """scrape type: 1 .text, 2 .attrib["alt"]"""
    scrape_list = tree.xpath(xpth)
    list_out = []
    if scrape_type == 1:
        for each in scrape_list:
            list_out.append(each.text.strip())
    if scrape_type == 2:
        for each in scrape_list:
            list_out.append(each.attrib["alt"])        
    return list_out

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

# Recuperer les tweets par un requette (query)
def get_tweets(query, api, bucket_url, count = 200):
    q=str(query)
    fetched_tweets = api.user_timeline(screen_name=q, count = count)
    clmns = ['Tweet', 'Date de creation', 'hashtags']
    data_tweets=[]
    # analyser tweets une par une
    for tweet in fetched_tweets:
        # Ajouter les tweets à la dataframe
        oneline_text = str.join(" ", tweet.text.splitlines())
        data_tweets.append([oneline_text, tweet.created_at, tweet.entities["hashtags"]])
    df = pd.DataFrame(data_tweets, columns=clmns)
    df.to_csv(r'{}{}.csv'.format(bucket_url,query), index=False)
    
def images_downloader(media_files, directory):
    path_downloaded= set()
    for media_file in media_files:
        path_downloaded.add(wget.download(media_file, directory))
    return path_downloaded

def get_photo_user(at_user,api):
    # Definir un class à partir d'une function qui
    @classmethod
    def parse(cls, api, raw):
        status = cls.first_parse(api, raw)
        setattr(status, 'json', json.dumps(raw))
        return status

    # You need to do it for all the models you need
    # Status() is the data model for a tweet
    tweepy.models.Status.first_parse = tweepy.models.Status.parse
    tweepy.models.Status.parse = parse
    # User() is the data model for a user profil
    tweepy.models.User.first_parse = tweepy.models.User.parse
    tweepy.models.User.parse = parse
    
    # This code stores all the tweets by a specific user in the variable tweets. 
    # Now, we are ready to filter those with images. In our case, we want 
    # 200 tweets which are directly created by the user (i.e., No retweets nor replies)
    tweets = api.user_timeline(screen_name=at_user,
    count=1, include_rts=False,
    exclude_replies=True)
    last_id = tweets[-1].id
    while (True):
        more_tweets = api.user_timeline(screen_name=at_user,
        since_id="2022-06-01",
        count=1, include_rts=False,
        exclude_replies=True,
        max_id=last_id-1)
        # There are no more tweets
        if (len(more_tweets) == 0):
            break
        else:
            last_id = more_tweets[-1].id-1
            tweets = tweets + more_tweets

    # Trouver les liens pour toutes les images dans chaque tweet
    media_files = set()
    for status in tweets:
        media = status.entities.get('media', [])
        if(len(media) > 0):
            media_files.add(media[0]['media_url'])
    return media_files
