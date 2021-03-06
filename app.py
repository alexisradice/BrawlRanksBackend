from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, request, send_file, send_from_directory
import json
import requests
import pymongo
import ssl
import pandas as pd
from datetime import datetime
import pytz
from dotenv import load_dotenv
import os
from flask_cors import CORS

load_dotenv()

API_KEY = os.getenv("API_KEY")

class DataStore():
	test = 0

data = DataStore()

def sensor():
	requests.get(os.getenv("FLASK_API")).content
	if data.test == 0:
		data.test = 1
		print("0")
		bestPlayers(0)
	elif data.test == 1:
		data.test = 2
		print("1")
		bestPlayers(1)
	elif data.test == 2:
		data.test = 0
		print("2")
		bestPlayers(2)

def bestPlayers(choice):
	data = pd.read_excel (r'bestPlayers.xlsx') 
	df = pd.DataFrame(data, columns= ['Name', 'Brawlhalla ID', 'Earnings'])
	df['Brawlhalla ID'] = df['Brawlhalla ID'].astype(str)          
	df['Brawlhalla ID'] = df['Brawlhalla ID'].str.split('.').str[0]
	print(df)
	players = df.to_dict()
	print(players)

	'''
	collectionBestPlayers.delete_many({})
	cmp = 0
	for i in range(0, len(df)):
		print(players['Brawlhalla ID'][i])
		if (players['Brawlhalla ID'][i] != "nan"):
			post = {"_id":cmp , "name": players['Name'][i], "brawlID":players['Brawlhalla ID'][i], "earnings":players['Earnings'][i]}
			cmp += 1
			collectionBestPlayers.insert_one(post)
	'''

	numID = collectionBestPlayers.count_documents({})
	print(numID)

	country = "France"

	if (choice == 0):
		start = 0
		end = int(numID/3)
	elif (choice == 1):
		start = int(numID/3)
		end = int((numID/3) * 2)
	elif (choice == 2):
		start = int((numID/3) * 2)
		end = numID

	for i in range(start, end):
		print(collectionBestPlayers.find_one({"_id":i}))
		name = collectionBestPlayers.find_one({"_id":i})['name']
		brawlID = collectionBestPlayers.find_one({"_id":i})['brawlID']
		earnings = collectionBestPlayers.find_one({"_id":i})['earnings']
		print(name)
		print(brawlID)

		try:
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
			winrate = player.json()["dataClientJSON"]["winrate"]
			clan = player.json()["dataClientJSON"]["clan"]
			totalCharactersLevels = player.json()["dataClientJSON"]["totalCharactersLevels"]

			post = {"_id":i , "name": name, "inGameName": playerName,"brawlID":brawlID, "level":level, "region":region, "rating":rating, "peakRating":peakRating, "globalRank":globalRank, "regionRank":regionRank, "mainLevelCharacter":mainLevelCharacter, "mainRankedCharacter":mainRankedCharacter, "pictureMainLevelCharacter":pictureMainLevelCharacter, "pictureMainRankedCharacter":pictureMainRankedCharacter, "mainWeapon":mainWeapon, "trueLevel":trueLevel, "passiveAgressive":passiveAgressive, "timePlayed":timePlayed, "earnings":earnings, "country":country, "winrate":winrate, "clan":clan, "totalCharactersLevels":totalCharactersLevels} 
			collectionBestPlayers.delete_one({"_id":i})
			collectionBestPlayers.insert_one(post)
		except:
			print("Time Out / No Data : Next")
			pass


app = Flask(__name__)

CORS(app)

cluster = pymongo.MongoClient(os.getenv("MONGODB_URL"), ssl_cert_reqs=ssl.CERT_NONE)
db = cluster["brawlData"]
collectionBestPlayers = db["bestPlayers"]

sched = BackgroundScheduler(next_run_time=datetime.now)
sched.configure(timezone=pytz.timezone('Europe/Paris'))
sched.add_job(sensor,'interval',minutes=20, next_run_time=datetime.now())
sched.start()

@app.route("/")
def home():
	return "Welcome to BrawlRanks API :) !"

@app.route("/api/france/currentSeason")
def currentSeason():
	return jsonify(list(collectionBestPlayers.find().sort("rating", -1)))

@app.route("/api/france/season23")
def season23():
	return send_from_directory("json/", "bestFrancePlayersSeason23.json")

@app.route("/api/france/season24")
def season24():
	return send_from_directory("json/", "bestFrancePlayersSeason24.json")

@app.route("/api/legends/<string:legend>", methods=["GET"])
def legends(legend):
	return send_file("img/legends/" + str(legend) + ".png")

@app.route("/api/imgLoading")
def imgLoading():
	return send_file("img/imgLoading.jpg")

if __name__ == "__main__":
	app.run()