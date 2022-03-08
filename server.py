from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from flask import Flask, jsonify
import requests
import pymongo
import ssl
import pandas as pd
from datetime import datetime
import pytz
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("API_KEY")

class DataStore():
    test = 0

data = DataStore()

def sensor():
    if data.test == 0:
        data.test = 1
        print("0")
        bestPlayers(0)
    elif data.test == 1:
        data.test = 0
        print("1")
        bestPlayers(1)

def bestPlayers(choice):
    data = pd.read_excel (r'bestPlayers.xlsx') 
    df = pd.DataFrame(data, columns= ['Name', 'Brawlhalla ID'])
    df['Brawlhalla ID'] = df['Brawlhalla ID'].astype(str)          
    df['Brawlhalla ID'] = df['Brawlhalla ID'].str.split('.').str[0]
    print(df)
    players = df.to_dict()
    print(players)

    ''' To Update the players with the excel
    collectionBestPlayers.delete_many({})
    cmp = 0
    for i in range(0, len(df)):
        print(players['Brawlhalla ID'][i])
        if (players['Brawlhalla ID'][i] != "nan"):
            post = {"_id":cmp , "name": players['Name'][i], "brawlID":players['Brawlhalla ID'][i]}
            cmp += 1
            collectionBestPlayers.insert_one(post)
    '''
    numID = collectionBestPlayers.count_documents({})
    print(numID)

    country = "France"
    
    if (choice == 0):
        start = 0
        end = int(numID/2)
    elif (choice == 1):
        start = int(numID/2)
        end = numID

    for i in range(start, end):
        print(collectionBestPlayers.find_one({"_id":i}))
        name = collectionBestPlayers.find_one({"_id":i})['name']
        brawlID = collectionBestPlayers.find_one({"_id":i})['brawlID']
        print(name)
        print(brawlID)

        player = requests.get(os.getenv("MY_API_URL") + str(brawlID))

        playerName = player.json()["dataClientJSON"]["playerName"]
        level = player.json()["dataClientJSON"]["level"]
        region = player.json()["dataClientJSON"]["region"]
        rating = player.json()["dataClientJSON"]["rating"]
        peakRating = player.json()["dataClientJSON"]["peakRating"]
        globalRank = player.json()["dataClientJSON"]["globalRank"]
        regionRank = player.json()["dataClientJSON"]["regionRank"]
        mainLevelCharacter = player.json()["dataClientJSON"]["mainLevelCharacter"]
        mainRankedCharacter = player.json()["dataClientJSON"]["mainRankedCharacter"]
        pictureMainLevelCharacter = player.json()["dataClientJSON"]["pictureMainLevelCharacter"]
        pictureMainRankedCharacter = player.json()["dataClientJSON"]["pictureMainRankedCharacter"]
        mainWeapon = player.json()["dataClientJSON"]["mainWeapon"]
        trueLevel = player.json()["dataClientJSON"]["trueLevel"]
        passiveAgressive = player.json()["dataClientJSON"]["passiveAgressive"]
        timePlayed = player.json()["dataClientJSON"]["timePlayed"]

        post = {"_id":i , "name": name, "inGameName": playerName,"brawlID":brawlID, "level":level, "region":region, "rating":rating, "peakRating":peakRating, "globalRank":globalRank, "regionRank":regionRank, "mainLevelCharacter":mainLevelCharacter, "mainRankedCharacter":mainRankedCharacter, "pictureMainLevelCharacter":pictureMainLevelCharacter, "pictureMainRankedCharacter":pictureMainRankedCharacter, "mainWeapon":mainWeapon, "trueLevel":trueLevel, "passiveAgressive":passiveAgressive, "timePlayed":timePlayed, "country":country} 
        collectionBestPlayers.delete_one({"_id":i})
        collectionBestPlayers.insert_one(post)


app = Flask(__name__)

cluster = pymongo.MongoClient(os.getenv("MONGODB_URL"), ssl_cert_reqs=ssl.CERT_NONE)
db = cluster["brawlData"]
collectionBestPlayers = db["bestPlayers"]

# sched = BackgroundScheduler(next_run_time=datetime.now)
# sched = BackgroundScheduler()
sched = BlockingScheduler()
sched.configure(timezone=pytz.timezone('Europe/Paris'))
sched.add_job(sensor,'interval',minutes=20, next_run_time=datetime.now())
# sched.add_job(sensor,'interval',minutes=20)
sched.start()

@app.route("/")
def home():
    return "Welcome to Brawlhalla Best Country Players Ranks API :) !"

@app.route("/api")
def api():
    return jsonify(list(collectionBestPlayers.find({})))

app.run()