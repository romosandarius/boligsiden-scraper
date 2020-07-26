import re
import requests
from requests_html import HTMLSession
from bs4 import BeautifulSoup

url = 'https://www.boligsiden.dk/resultat/45b329d437d14960aeeed2e6398f83ce?s=12&sd=false&d=1&p={}&i=60'

session = HTMLSession()
r = session.get(url)
r.html.render()
data = r.content
#print(r.html.find('.info > .name', first=True).text)


# Get soup
soup = BeautifulSoup(data, "html.parser")

soup.find_all('a')