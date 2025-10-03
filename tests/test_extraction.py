# import socket
# print(socket.gethostbyname("api.openweathermap.org"))

# import requests

# try:
#     r = requests.get("https://api.openweathermap.org")
#     print("Status:", r.status_code)
#     print("Headers:", r.headers)
# except Exception as e:
#     print("Connection error:", e)

import requests

API_KEY = "e5e294b017f5da0c02a935d8a6122573"
cities = ["London,GB", "Paris,FR", "Chennai,IN"]

for city in cities:
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(city, "->", data)
    else:
        print(city, "-> Error", response.status_code, response.text)
