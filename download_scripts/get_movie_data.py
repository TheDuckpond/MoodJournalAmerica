"""
This script retrieves the most popular Netflix titles in the last 24 hours from instantwatcher.com which has been 
granted exclusive access to the Netflix API which will soon deprecate.

@authored malam, 18 July 2014
@modified habdulkafi, 20 July 2014
Changelog:

"""

import bs4
import requests
from rottentomatoes import RT
import re
import pandas
import math
import csv

reg1 = re.compile("\$\(function.+")
reg2 = re.compile('\s')

r = requests.get("http://instantwatcher.com/") 
soup = bs4.BeautifulSoup(r.content)
pop_titles = soup.findAll('div', {'class': 'span-8 homepage-most-popular'})

auth_key = ''

# adapted from http://stackoverflow.com/a/989920 for dealing with bs4 resultsets
def parse_string(el):
	text = ''.join(el.findAll(text=True))
	return text.strip()

for title in pop_titles:
	titles = map(parse_string,title.findAll('a')) # titles contained within ahref tags

synop = []

for i in titles: # needs fixing to deal with dict keys
	try:
		wholedict = RT(auth_key).search(i)
		allinks = wholedict[0]['links']['alternate']
		characters = []
		for j in wholedict[0]['abridged_cast']:
			characters.append(j['name'].encode('utf8'))
		r = requests.get(allinks)
		soup = bs4.BeautifulSoup(r.content)
		tempdict = {}
		tempdict['title'] = i
		tempdict['synopsis'] = re.sub(reg1,'',re.sub(reg2,' ',soup.findAll('p', {'id':"movieSynopsis"})[0].text)).encode('utf8')
		for actor in characters:
			tempdict['actor_' + str(characters.index(actor))] = actor
		tempdict['rating'] = wholedict[0]['mpaa_rating']
		synop.append(tempdict)

	except Exception, e:
		print i,e

df = pandas.DataFrame(synop)

all_actors = df['actor_0'].tolist() + df['actor_1'].tolist() + df['actor_2'].tolist() + df['actor_3'].tolist() + df['actor_4'].tolist()

top_actors = {}
for actor in all_actors:
	if type(actor) == type(0.0):
		all_actors.remove(actor)
	elif actor in top_actors.keys():
		top_actors[actor] += 1
		# print type(actor)
	else:
		top_actors[actor] = 1


ratings = df['rating']

top_rating = {}
for rating in ratings:
	if type(rating) == type(0.0):
		ratings.remove(rating)
	elif rating in top_rating.keys():
		top_rating[rating] += 1
	else:
		top_rating[rating] = 1


sorted_actors = sorted(top_actors, key=top_actors.get,reverse=True)
sorted_ratings = sorted(top_rating, key=top_rating.get,reverse=True)

with open('daily_movies.csv','wb') as f:
	wr = csv.writer(f)
	wr.writerow(['actors','rating'])
	for ac in sorted_actors:
		wr.writerow([ac,sorted_ratings[0]])
	f.close()

df.to_csv('movie_synopses.csv',index=False)