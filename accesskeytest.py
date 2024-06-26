


import requests

# Replace 'YOUR_ACCESS_KEY' with your actual API key from Weatherstack
api_key = 'f0b63c15afb6890c42cce7254986db0f'
url = f'http://api.weatherstack.com/current?access_key={api_key}&query=New York'

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print(f"Error accessing API: {response.status_code} - {response.text}")
