from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, request, send_file, send_from_directory
import requests
import pymongo
import pandas as pd
from datetime import datetime
import pytz
from dotenv import load_dotenv
import os
from flask_cors import CORS
import certifi
import logging

# Load environment variables
load_dotenv()

API_KEY = os.getenv("API_KEY")
FLASK_API = os.getenv("FLASK_API")
MY_API_URL = os.getenv("MY_API_URL")
MONGODB_URL = os.getenv("MONGODB_URL")
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
URL_UPLOAD = os.getenv("URL_UPLOAD")

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Initialize MongoDB client
cluster = pymongo.MongoClient(MONGODB_URL, tlsCAFile=certifi.where())
db = cluster["brawlData"]
collectionBestPlayers = db["bestPlayers"]

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Data store for keeping track of choices
class DataStore():
	test = 0

data = DataStore()

# Sensor function to alternate between different data sets
def sensor():
	try:
		response = requests.get(FLASK_API)
		response.raise_for_status()
	except requests.RequestException as e:
		logger.error(f"Failed to reach {FLASK_API}: {e}")
		return
	
	if data.test == 0:
		data.test = 1
		logger.info("Processing choice 0")
		bestPlayers(0)
	elif data.test == 1:
		data.test = 2
		logger.info("Processing choice 1")
		bestPlayers(1)
	elif data.test == 2:
		data.test = 0
		logger.info("Processing choice 2")
		bestPlayers(2)

	players = list(collectionBestPlayers.find().sort("rating", -1))
	headers = {'Authorization': f'Bearer {BEARER_TOKEN}'}
	payload = {
		'filename': 'currentSeason',
		'content': players
	}
	response = requests.post(URL_UPLOAD, json=payload, headers=headers)
	logger.info(response.json())

# Function to update the best players in the database
def bestPlayers(choice):
	try:
		df = pd.read_excel('bestPlayers.xlsx')
		df['Brawlhalla ID'] = df['Brawlhalla ID'].astype(str).str.split('.').str[0]
		players = df.to_dict()
		print(players)
	except Exception as e:
		logger.error(f"Failed to process Excel file: {e}")
		return
	
# If no players in the database or add new players
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

	ranges = {
		0: (0, int(numID/3)),
		1: (int(numID/3), int(2*numID/3)),
		2: (int(2*numID/3), numID)
	}
	
	start, end = ranges.get(choice, (0, numID))

	for i in range(start, end):
		player_data = collectionBestPlayers.find_one({"_id": i})
		print(player_data)
		if player_data is None:
			logger.warning(f"No player found with _id: {i}")
			continue

		name = player_data['name']
		brawlID = player_data['brawlID']
		earnings = player_data['earnings']
		print(name)
		print(brawlID)
		try:
			player = requests.get(f"{MY_API_URL}{brawlID}")
			player.raise_for_status()
			player_json = player.json()["dataClientJSON"]

			post = {
				"_id": i,
				"name": name,
				"inGameName": player_json["playerName"],
				"brawlID": brawlID,
				"level": player_json["level"],
				"region": player_json["region"],
				"rating": player_json["rating"],
				"peakRating": player_json["peakRating"],
				"globalRank": player_json["globalRank"],
				"regionRank": player_json["regionRank"],
				"mainLevelCharacter": player_json["mainLevelCharacter"],
				"mainRankedCharacter": player_json["mainRankedCharacter"],
				"pictureMainLevelCharacter": player_json["pictureMainLevelCharacter"],
				"pictureMainRankedCharacter": player_json["pictureMainRankedCharacter"],
				"mainWeapon": player_json["mainWeapon"],
				"trueLevel": player_json["trueLevel"],
				"passiveAgressive": player_json["passiveAgressive"],
				"timePlayed": player_json["timePlayed"],
				"earnings": earnings,
				"country": country,
				"winrate": player_json["winrate"],
				"clan": player_json["clan"],
				"totalCharactersLevels": player_json["totalCharactersLevels"]
			}
			collectionBestPlayers.replace_one({"_id": i}, post, upsert=True)
		except requests.RequestException as e:
			logger.error(f"Failed to fetch data for player {brawlID}: {e}")
		except KeyError as e:
			logger.error(f"Missing key in player data for {brawlID}: {e}")
		except Exception as e:
			logger.error(f"Unexpected error for player {brawlID}: {e}")

# Flask routes
@app.route("/")
def home():
	return "Welcome to BrawlRanks API :) !"

@app.route("/api/france/currentSeason")
def currentSeason():
	try:
		players = list(collectionBestPlayers.find().sort("rating", -1))
		return jsonify(players)
	except Exception as e:
		logger.error(f"Error retrieving current season data: {e}")
		return jsonify({"error": "Failed to retrieve data"}), 500

@app.route("/api/france/season23")
def season23():
	return send_from_directory("json/", "bestFrancePlayersSeason23.json")

@app.route("/api/france/season24")
def season24():
	return send_from_directory("json/", "bestFrancePlayersSeason24.json")

@app.route("/api/legends/<string:legend>", methods=["GET"])
def legends(legend):
	try:
		return send_file(f"img/legends/{legend}.png")
	except Exception as e:
		logger.error(f"Error retrieving image for legend {legend}: {e}")
		return jsonify({"error": "Image not found"}), 404

@app.route("/api/imgLoading")
def imgLoading():
	return send_file("img/imgLoading.jpg")

# Schedule the sensor function to run every 20 minutes
sched = BackgroundScheduler(timezone=pytz.timezone('Europe/Paris'))
sched.add_job(sensor, 'interval', minutes=20, next_run_time=datetime.now())
sched.start()

if __name__ == "__main__":
	app.run()
