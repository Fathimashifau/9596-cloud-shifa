import requests
api_key="pub_379971d421944e53a2cf482947f8a633"
url=f"https://newsdata.io/api/1/news?apikey={api_key}&q=technology&language=en"

response=requests.get(url)
data=response.json()
print(data)