import requests

page_number = 0
# req1 = requests.get(url=f"https://opendata.utah.gov/resource/s9t3-bccv.json?$limit=20&$offset={page_number}")
req2 = requests.get(url=f"https://opendata.utah.gov/resource/s9t3-bccv.json?$limit=20&$offset={page_number + 1}")

# res1 = req1.json()
# res2 = req2.json()
# print(res1[0])
print(req2.headers.get("X-SODA2-Secondary-Last-Modified"))

req = requests.request(method="GET", url="", data={
    "limit": 5000,
})
print(req2.headers.get("Last-Modified"))
# print(req.json())
# print(len(req.json()))
