"""

Update data underlying visualizations on Mood Journal America
Some copy & paste from original qacprojects script

Runs on crontab, daily_download -> topicmoddeling.py -> << this >>

@authored malam,habdulkafi 31 June 2014
I/O,query functions originally by rpetchler,malam from qacprojects query script
Changelog:
@updated: malam, 7 July 2014 - added wordcloud query
@updated: habdulkafi, 21 July 2014 - added gtrends,heatmap,movie queries

"""


import codecs
import json
import os
import string
import urlparse
import re
import requests
import bs4

import nltk
import us
import multiprocessing
import MySQLdb
import numpy as np
import pandas as pd
from pandas.io import sql
from numpy import round

from rottentomatoes import RT


static_dir = '/home/twitter-data/website/qacprojects/static/data/twitter_project/tmp_data_test'

# dict based directly on official FIPS code
fipsdict = {'Northeast':{'New England':['09','23','25','33','44','50'],'Middle Atlantic':['34','36','42']},'Midwest':{'East North Central':['18','17','26','39','55'],'West North Central':['19','20','27','29','31','38','46']},'South':{'South Atlantic':['10','11','12','13','24','37','45','51','54'],'East South Central':['01','21','28','47'],'West South Central':['05','22','40','48']},'West':{'Mountain':['04','08','16','35','30','49','32','56'],'Pacific':['02','06','15','41','53']}}
subregions = []
for region in fipsdict.keys():
    subregions = subregions + fipsdict[region].keys()

# because we're uploading to two separate databases

with open('p_param1.json') as f:
    p = json.load(f)

with open('z_param2.json') as q:
    z = json.load(q)


conn = MySQLdb.connect(host=p['mysql']['host'],
                       user=p['mysql']['user'],
                       passwd=p['mysql']['passwd'],
                       db=p['mysql']['db'],
                       charset='utf8')


# twitter database
conn2 = MySQLdb.connect(host=z['mysql']['host'],
                       user=z['mysql']['user'],
                       passwd=z['mysql']['passwd'],
                       db=z['mysql']['db'],
                       charset='utf8')

# a bit irrelevant here, inherited from qacprojects script

categories = {1: 'Republican', 2: 'Democrat'}
chambers={0: 'House', 1: 'Senate'}
chambercolors={0: '#F8DC3',1: '#7FBF7B'}
colors = {1: '#D62728', 2: '#1F77B4'}


######################################################################
# I/O Functions


def decorator(func):
    """Decorate I/O functions with filesystem operations."""
    def wrapper(filename, data):
        filename = os.path.join(static_dir, filename)
        func(filename, data)
        # os.chmod(filename, 0664)
    return wrapper


@decorator
def write_json(filename, data):
    """Write JSON data to a file."""
    with codecs.open(filename, 'w', 'utf8') as f:
        json.dump(data, f, separators=(',', ':'))


@decorator
def write_csv(filename, df):
    """Write a Pandas DataFrame to CSV format."""
    df.to_csv(filename, index=False, encoding='utf8')


@decorator
def write_html(filename, df):
    """Write a Pandas DataFrame to a Boostrap-classed HTML table."""
    html = df.to_html(index=False, classes=['table', 'table-condensed'])
    html = html.replace('border="1" ', '')
    html = html.replace('dataframe ', '')
    html = html.replace(' style="text-align: right;"', '')
    with codecs.open(filename, 'w', 'utf8') as f:
        f.write(html)


######################################################################
# Query Functions


def nest(df, key=None, **kwargs):
    """Nest a series into JSON format for NVD3.

    The JavaScript data structure has at least two item: `key`, a
    string or integer used as the series label, and `values`, an array
    of two-element arrays which contain the series indices and values.
    Use kwargs to pass additionally items to the series, such as the
    series color.

    Args:
        df: A Pandas Series.
        key: The name of the matching key in the JSON data structure.

    Returns:
        A nested dictionary which, when appended to a list, is
            a suitable data set for visualization with NVD3.
    """
    df = [(k, int(v)) for k, v in df.iteritems()]
    df = {'key': key, 'values': df}
    for k, v in kwargs.items():
        df[k] = v
    return df


def group_nest(df, key=None, **kwargs):
    """Nest a data frame into JSON format for NVD3.
    """
    top = df.groupby('label').agg({'value': np.sum})
    top = top.sort('value', ascending=False).head(20).index

    nested = []
    for name, group in df.groupby('category'):
        group = group[['label', 'value']].set_index('label')
        group = group.ix[top].fillna(0)
        group = nest(group.value, key=categories[name], color=colors[name])
        nested.append(group)

    return nested


def query_urls(q, params=None, n=20):
    """Counts the number of URLs in a query.

    This wraps the URL parsing routine. Ensure that the SQL query
    returns a column of URLs.

    Args:
        q: A string SQL query.
        params: An optional list or tuple of query parameters.
        n: The number of top records to retrieve.

    Returns:
        An indexed, one-column Pandas DataFrame. The index contains
            URLs and the column contains frequencies.
    """
    df = sql.read_frame(q, conn, params=params)
    df = df['url'].apply(lambda x: urlparse.urlparse(x).netloc)
    df = df.value_counts()
    df = df.head(n)
    df = pd.DataFrame({'label': df.index, 'value': df}).set_index('label')
    return df

# CREATES THE HTML TABLE FOR THE GOOGLE TRENDS
def write_google_html(filename, df):
    """Write a Pandas DataFrame to a Boostrap-classed HTML table."""
    html = df.to_html(index=False, columns = ['America\'s Priorities','explained...','SEE FOR YOURSELF'], classes=['table','table-striped'],escape = False)
    html = html.replace('border="1" ', '')
    html = html.replace('dataframe ', '')
    # html = html.replace(' style="text-align: left;"', '')
    html = html.replace('style="text-align: right;"','style="text-align: right; margin-right:15px;"')
    with codecs.open(filename, 'w', 'UTF-8') as f:
        f.write(html)

######################################################################
# Sentiment / Geo

cur = conn.cursor()

q='''SELECT g.fips AS id, g.region, g.sub_region, g.state, CAST( p.pos AS SIGNED INT ) - CAST( p.neg AS SIGNED INT ) AS rate
FROM daily_download AS p
INNER JOIN geo AS g ON g.state = p.state
WHERE p.timestamp = CURDATE()'''
df = sql.read_frame(q, conn)
write_csv('news_sent_overall.csv', df)

# Query for overall topic modelling
q='''SELECT topics FROM daily_overall
WHERE date = CURDATE()'''
df = sql.read_frame(q,conn)
write_csv('topics_overall.txt',df)

# Query for overall topic sentiment rate
q='''SELECT sentiment FROM daily_overall
WHERE date = CURDATE()'''
df = sql.read_frame(q,conn)
write_csv('news-sent-single.csv',df)


# Text for WordCloud
q='''SELECT text,timestamp AS date FROM daily_download
WHERE timestamp = CURDATE()'''
df = sql.read_frame(q,conn)
write_csv('news_text_all.csv',df)

# this query joins daily_download with daily_geo tables
q = u'SELECT d.pos, d.neg, g.fips, g.region, g.sub_region, g.state FROM geo as g INNER JOIN daily_download as d on d.state=g.state WHERE timestamp = CURDATE();'
# creates a pandas dataframe from that query
df = sql.read_frame(q ,conn)
df['rate'] = df['pos'] - df['neg']

sentiment_reg1 = (df[df['region'] == 'Northeast']['pos'].sum() - df[df['region'] == 'Northeast']['neg'].sum() + 0.0) / len(df[df['region'] == 'Northeast'].index)
sentiment_reg2 = (df[df['region'] == 'Midwest']['pos'].sum() - df[df['region'] == 'Midwest']['neg'].sum() + 0.0) / len(df[df['region'] == 'Midwest'].index)
sentiment_reg3 = (df[df['region'] == 'South']['pos'].sum() - df[df['region'] == 'South']['neg'].sum() + 0.0) / len(df[df['region'] == 'South'].index)
sentiment_reg4 = (df[df['region'] == 'West']['pos'].sum() - df[df['region'] == 'West']['neg'].sum() + 0.0) / len(df[df['region'] == 'West'].index)

df.loc[df['region'] == 'Northeast','rrate'] = sentiment_reg1
df.loc[df['region'] == 'Midwest','rrate'] = sentiment_reg2
df.loc[df['region'] == 'South','rrate'] = sentiment_reg3
df.loc[df['region'] == 'West','rrate'] = sentiment_reg4

for subregion in subregions:
    df.loc[df['sub_region'] == subregion,'srate'] = (df[df['sub_region'] == subregion]['pos'].sum() - df[df['sub_region'] == subregion]['neg'].sum() + 0.0) / len(df[df['sub_region'] == subregion].index)

df = df.rename(columns = {'fips':'id'})
df = df[['id','region','sub_region','state','rate','rrate','srate']]
df['rrate'] = round(df['rrate'],7)
df['srate'] = round(df['srate'],7)
df.to_csv('/home/twitter-data/website/qacprojects/static/data/twitter_project/tmp_data_test/news_sent_total.csv',index = False)

######### Issue Queries ##############
# Requires a separate connection to twitter database #
##### Human Rights ######

# Per Hour
q='''SELECT UNIX_TIMESTAMP( DATE_ADD( DATE_FORMAT( CONVERT_TZ( s.created_at,  '+00:00',  '-04:00' ) ,  "%Y-%m-%d %H:00:00" ) , INTERVAL IF( MINUTE( s.created_at ) <30, 0, 1 ) HOUR ) ) *1000 AS label, 
CAST( p.positive AS SIGNED INT ) - CAST( p.negative AS SIGNED INT ) AS value
FROM status AS s
INNER JOIN sentiment AS p ON p.status_id = s.status_id
WHERE  `issue_key` = 1
GROUP BY label;'''
df = sql.read_frame(q, conn2)
write_csv('hr-sent-hour.csv',df)

# Per Day
q='''SELECT UNIX_TIMESTAMP( DATE_FORMAT( CONVERT_TZ( s.created_at,  '+00:00',  '-04:00' ) ,  "%Y-%m-%d" ) ) *1000 AS label, CAST( p.positive AS SIGNED ) - CAST( p.negative AS SIGNED ) AS value
FROM status AS s
INNER JOIN sentiment AS p ON p.status_id = s.status_id
WHERE  `issue_key` = 1
GROUP BY label;'''
df = sql.read_frame(q, conn2)
write_csv('hr-sent-day.csv', df)

#### Health Care ######

# Overall
q='''SELECT created_at AS label, 
CAST( p.positive AS SIGNED INT ) - CAST( p.negative AS SIGNED INT ) AS value
FROM status AS s
INNER JOIN sentiment AS p ON p.status_id = s.status_id
WHERE  `issue_key` = 2
GROUP BY label;'''
df = sql.read_frame(q, conn2)
write_csv('hc-sent-overall.csv', df)

# Per Hour
q='''SELECT UNIX_TIMESTAMP( DATE_ADD( DATE_FORMAT( CONVERT_TZ( s.created_at,  '+00:00',  '-04:00' ) ,  "%Y-%m-%d %H:00:00" ) , INTERVAL IF( MINUTE( s.created_at ) <30, 0, 1 ) HOUR ) ) *1000 AS label, 
CAST( p.positive AS SIGNED INT ) - CAST( p.negative AS SIGNED INT ) AS value
FROM status AS s
INNER JOIN sentiment AS p ON p.status_id = s.status_id
WHERE  `issue_key` = 2
GROUP BY label;'''
df = sql.read_frame(q, conn2)
write_csv('hc-sent-hour.csv', df)

# Per Day
q='''SELECT UNIX_TIMESTAMP( DATE_FORMAT( CONVERT_TZ( s.created_at,  '+00:00',  '-04:00' ) ,  "%Y-%m-%d" ) ) *1000 AS label, CAST( p.positive AS SIGNED ) - CAST( p.negative AS SIGNED ) AS value
FROM status AS s
INNER JOIN sentiment AS p ON p.status_id = s.status_id
WHERE  `issue_key` = 2
GROUP BY label;'''
df = sql.read_frame(q, conn2)
write_csv('hc-sent-day.csv', df)

#### Environment ######

# Per Hour
q='''SELECT UNIX_TIMESTAMP( DATE_ADD( DATE_FORMAT( CONVERT_TZ( s.created_at,  '+00:00',  '-04:00' ) ,  "%Y-%m-%d %H:00:00" ) , INTERVAL IF( MINUTE( s.created_at ) <30, 0, 1 ) HOUR ) ) *1000 AS label, 
CAST( p.positive AS SIGNED INT ) - CAST( p.negative AS SIGNED INT ) AS value
FROM status AS s
INNER JOIN sentiment AS p ON p.status_id = s.status_id
WHERE  `issue_key` = 3
GROUP BY label;'''
df = sql.read_frame(q, conn2)
write_csv('env-sent-hour.csv', df)

# Per Day
q='''SELECT UNIX_TIMESTAMP( DATE_FORMAT( CONVERT_TZ( s.created_at,  '+00:00',  '-04:00' ) ,  "%Y-%m-%d" ) ) *1000 AS label, CAST( p.positive AS SIGNED ) - CAST( p.negative AS SIGNED ) AS value
FROM status AS s
INNER JOIN sentiment AS p ON p.status_id = s.status_id
WHERE  `issue_key` = 3
GROUP BY label;'''
df = sql.read_frame(q, conn2)
write_csv('env-sent-day.csv', df)

#### Education   ######

# Per Hour
q='''SELECT UNIX_TIMESTAMP( DATE_ADD( DATE_FORMAT( CONVERT_TZ( s.created_at,  '+00:00',  '-04:00' ) ,  "%Y-%m-%d %H:00:00" ) , INTERVAL IF( MINUTE( s.created_at ) <30, 0, 1 ) HOUR ) ) *1000 AS label, 
CAST( p.positive AS SIGNED INT ) - CAST( p.negative AS SIGNED INT ) AS value
FROM status AS s
INNER JOIN sentiment AS p ON p.status_id = s.status_id
WHERE  `issue_key` = 4
GROUP BY label;'''
df = sql.read_frame(q, conn2)
write_csv('edu-sent-hour.csv', df)

# Per Day
q='''SELECT UNIX_TIMESTAMP( DATE_FORMAT( CONVERT_TZ( s.created_at,  '+00:00',  '-04:00' ) ,  "%Y-%m-%d" ) ) *1000 AS label, CAST( p.positive AS SIGNED ) - CAST( p.negative AS SIGNED ) AS value
FROM status AS s
INNER JOIN sentiment AS p ON p.status_id = s.status_id
WHERE  `issue_key` = 4
GROUP BY label;'''
df = sql.read_frame(q, conn2)
write_csv('edu-sent-day.csv', df)

#################################################################################
############  GOOGLE TRENDING TOPICS ############################################

regexhtml = re.compile(r'<.*?>')
r = requests.get('http://www.google.com/trends/hottrends/atom/feed?pn=p1')
soup = bs4.BeautifulSoup(r.content) # the prettiest of all soups

mainlist =[]
for title in soup.find_all('title')[1:11]:
    tempdict = {}
    tempdict['America\'s Priorities'] = regexhtml.sub('',title.text).encode('utf8')
    mainlist.append(tempdict)

i = 0
for newstitle in soup.find_all('ht:news_item_title')[:20][0::2]:
    mainlist[i]['Newstitle'] = regexhtml.sub('',newstitle.text).encode('utf8')
    i += 1

i = 0
for link in soup.find_all('ht:news_item_url')[:20][0::2]:
    mainlist[i]['url'] = link.text.encode('utf8')
    i += 1

for topic in mainlist:
    topic['SEE FOR YOURSELF'] = '<a href="' + topic['url'] + '">' + topic['Newstitle'] + '</a>'

i = 0
for snippet in soup.find_all('ht:news_item_snippet')[:20][0::2]:
    mainlist[i]['explained...'] = regexhtml.sub('',snippet.text).encode('utf8')
    i += 1

dframe = pd.DataFrame(mainlist)
pd.options.display.max_colwidth = 300 #20000

write_google_html('gtrends.html',dframe[:4])


####################################################################
########   Create Data for Adjacency Matrix  #######################

states = us.states.mapping('fips','abbr').keys()
del states[1] # None value
mapp = us.states.mapping("fips","abbr")
subregions = {'Mountain':1,'Pacific':2,'New England':3,'Middle Atlantic':4,'East North Central':5,'West North Central':6,'East South Central':7,'West South Central':8,'South Atlantic':9}

q = u'SELECT g.fips, g.state, d.text FROM geo as g INNER JOIN daily_download as d on d.state=g.state WHERE timestamp = CURDATE();'
df = sql.read_frame(q ,conn)

punct = re.compile(r'^[^A-Za-z0-9]+|[^a-zA-Z0-9]+$')
is_word=re.compile(r'[a-z]', re.IGNORECASE)
sentence_tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
word_tokenizer=nltk.tokenize.punkt.PunktWordTokenizer()

def get_words(sentence):
    return [punct.sub('',word) for word in word_tokenizer.tokenize(sentence) if is_word.search(word)]

def ngrams(text, n):
    for sentence in sentence_tokenizer.tokenize(text.lower()):
        words = get_words(sentence)

        for i in range(len(words)-(n-1)):
            yield(' '.join(words[i:i+n]))

def jaccard(set1,set2):
    inter = set1 & set2
    # union = set1 | set2
    return (float(2.0*len(inter))/float(len(set1) + len(set2)))*100

megadict = {}
for state in states:
    megadict[state] = ' '.join(df[df['fips'] == int(state)]['text'].tolist())

for state in megadict.keys():
    if len(megadict[state]) == 0:
        del megadict[state]

def heatmap(statename):
    fulllist = []
    for state1 in megadict.keys():
        if state1 == statename:
            fulllist.append((state1,statename,100.0))
        else:
            fulllist.append((state1,statename,jaccard(set(ngrams(megadict[state1],4)),set(ngrams(megadict[statename],4)))))
    return fulllist

stateiterator = iter(megadict.keys())
pool = multiprocessing.Pool()
j_list = pool.map(heatmap,stateiterator,chunksize = 10)                    

j_list = [item for sublist in j_list for item in sublist]
df1 = pd.DataFrame(j_list)
df1.columns = ['state1','state2','jaccard']
df1.to_csv('data_heatmap.csv',index=False)

df2 = pd.read_csv("statestdict.csv")
statesdict = df2.to_dict('records')

bigdict = {}
bigdict["nodes"] = []
bigdict["links"] = []
for state in statesdict:
    bigdict["nodes"].append({"name":state["state"],"group":subregions[state["subregion"]]})

df1 = df1.to_dict('records')

for row in df1:
    row['state1'] = str(int(row['state1']))
    if len(row['state1']) == 1:
        row['state1'] = '0' + row['state1']
    row['state2'] = str(int(row['state2']))
    if len(row['state2']) == 1:
        row['state2'] = '0' + row['state2']

for i in bigdict["nodes"]:
    for j in bigdict["nodes"]:
        for row in df1:
            if mapp[row['state1']] == i['name'] and mapp[row['state2']] == j['name']:
                jaccard = row["jaccard"]
        bigdict["links"].append({"source":bigdict["nodes"].index(i),"target":bigdict["nodes"].index(j),"value":jaccard})

with codecs.open('ajmatrix.json','w','utf8') as f:
    json.dump(bigdict, f, separators=(',',':'))


####################################################################
########    Data for America's Daily Movie     #####################

reg1 = re.compile("\$\(function.+")
reg2 = re.compile('\s')
r = requests.get("http://instantwatcher.com/") # instantwatcher has exclusive access to Netflix data despite API depreciate
soup = bs4.BeautifulSoup(r.content)
pop_titles = soup.findAll('div', {'class': 'span-8 homepage-most-popular'}) #always located here

def parse_string(el):
    text = ''.join(el.findAll(text=True))
    return text.strip()

for title in pop_titles:
    titles = map(parse_string,title.findAll('a'))

synop = []
for i in titles:
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


conn.close()
conn2.close()



