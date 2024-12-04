import requests

api_url1 = f'http://openapi.seoul.go.kr:8088/5457776d5073657938315043486944/json/tbCycleStationInfo/1/2/'

response = requests.get(api_url1)
print(response.content)

api_url2 = f'http://openapi.seoul.go.kr:8088/4b597a696d7365793832456245614a/json/bikeList/1/2/'
print("\napi2\n")
response = requests.get(api_url2)
print(response.content)
