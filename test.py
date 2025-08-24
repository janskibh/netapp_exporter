import requests
import base64

# Informations de connexion
url = "https://it-eqx-sto.aldebaran.lan/api/storage/disks"
username = "admin"
password = "VGVyc2VkaWEyMDIxIQo="

# Requête API
response = requests.get(url, auth=(username, base64.b64decode(password.encode())), verify=False)

if response.status_code == 200:
    disks = response.json().get('records', [])
    for disk in disks:
        print(f"Nom du disque: {disk['name']}")
        print(f"Capacité totale: {disk['capacity']} octets")
        print(f"Statut: {disk['status']}")
else:
    print(f"Erreur: {response.status_code} - {response.text}")