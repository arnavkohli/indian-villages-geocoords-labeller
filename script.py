import requests
import json
import db

CONFIG = json.loads(open("./config.json", "r").read())
db = db.MySQLDB(**CONFIG["MySQL"])

def refresh_token(client_id, client_secret):
	print (f"[INFO] Refreshing Token")
	url = f"https://outpost.mapmyindia.com/api/security/oauth/token?grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}"
	response = requests.post(url)
	if response.status_code == 200:
		access_token = response.json().get("access_token")
		CONFIG['MyMap']['access_token'] = access_token
		with open("./config.json", "w") as f:
			json.dump(CONFIG, f, indent=4)
		return True, "success"
	else:
		return False, str(response.content)

def get_state_data(state_code):
	url = f"https://raw.githubusercontent.com/datameet/indian_village_boundaries/master/{state_code}/{state_code}.geojson"
	return requests.get(url).json()

def get_coordinates(sub_dist, district, state, name, retries=0):
	if retries == 5:
		exit("[ERROR] Retried to get coordinates 5 times before exiting!")

	url = "https://atlas.mapmyindia.com/api/places/geocode?address="
	url += f"{sub_dist} {district} {state} {name}"
	headers = {"Authorization" : f"Bearer {CONFIG['MyMap']['access_token']}"}

	response = requests.get(url, headers=headers)
	data = response.json()

	try:
		return {"latitude" : data.get("copResults").get("latitude"), "longitude" : data.get("copResults").get("longitude")}
	except:
		refreshed, msg = refresh_token(client_id=CONFIG['MyMap']['client_id'], client_secret=CONFIG['MyMap']['client_secret'])
		if not refreshed:
			exit(f"[ERROR] Failed to refresh token!")
		return get_coordinates(sub_dist, district, state, retries=retries + 1)

def parse_label_and_save_data(data):
	feature_collection = data.get("features")

	all_data = []
	total = len(feature_collection)
	for index, record in enumerate(feature_collection):
		print (f"Status: {index + 1} / {total}")
		data = {}
		data["coordinates"] = record.get("geometry").get("coordinates")
		data["sub_district"] = record.get("properties").get("SUB_DIST")
		data["type"] = record.get("properties").get("TYPE")
		data["state"] = record.get("properties").get("STATE")
		data["name"] = record.get("properties").get("NAME")
		data["district"] = record.get("properties").get("DISTRICT")

		try:
			geo_data = get_coordinates(sub_dist=data["sub_district"], district=data["district"], state=data["state"], name=data["name"])
			data["latitude"] = geo_data["latitude"]
			data["longitude"] = geo_data["longitude"]
		except:
			print (f"[ERROR] There was an error getting lat long for record {index + 1}")
		all_data.append(data)
		db.insert_data(data, table=CONFIG['MySQL']['table_name'])
	return all_data


if __name__ == '__main__':
	state_code = "br"
	data = get_state_data(state_code)
	labelled_data = parse_label_and_save_data(data)





