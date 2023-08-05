import time
import json
import configparser
import csv
import boto3
import requests
import boto3


# Fonction pour effectuer l'appel API et récupérer les coordonnées
def get_position():
    url = "http://api.open-notify.org/iss-now.json"
    response = requests.get(url)
    data = response.json()
    return data["iss_position"]["latitude"], data["iss_position"]["longitude"]

# Nombre d'itérations
iterations = 2

# Attendre 5 secondes entre chaque appel
sleep_time = 2

# Liste pour stocker les coordonnées
coordinates_list = []

# Boucle pour effectuer les appels API et enregistrer les coordonnées
for _ in range(iterations):
    latitude, longitude = get_position()
    coordinates_list.append((latitude, longitude))
    time.sleep(sleep_time)

# Enregistrement dans le fichier CSV
export_file = "export_file.csv"

with open(export_file, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["Latitude", "Longitude"])
    csvwriter.writerows(coordinates_list)

csvfile.close()

print(f"{iterations} enregistrements ont été ajoutés dans le fichier export.csv.")

# load the aws_boto_credentials values
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials",
                "access_key")
secret_key = parser.get("aws_boto_credentials",
                "secret_key")
bucket_name = parser.get("aws_boto_credentials",
                "bucket_name")

s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key)

s3.upload_file(
    export_file,
    bucket_name,
    export_file)
