""" 
Pre-process Movie Synopses for Markov Chain plot generator

adapted from Ross's script (www.rosspetchler.com)
"""

import pandas as pd
from textblob import TextBlob

dataDirectory = "/home/twitter-data/website/qacprojects/static/data/twitter_project/tmp_data_test/"


data = pd.read_csv(dataDirectory + 'movie_synopses.csv')
data = data.dropna()  # Just in case
data['textblob'] = data.synopsis.apply(TextBlob) # or 'text' column, whatever it's labeled


######################################################################
# Extract and count paragraphs


tmp = data.synopsis.str.split('\n')
tmp = data.synopsis.str.strip()
tmp = data.synopsis.replace("\\","")
tmp = data.synopsis.replace(">","")
tmp = tmp.apply(lambda x: [i.strip() for i in x])
tmp = tmp.apply(lambda x: [i for i in x if len(i) > 0])
tmp = tmp.apply(lambda x: [i for i in x if not i.startswith('Tags:')])

data['para'] = tmp
data['n_para'] = tmp.apply(len)


######################################################################
# Extract and count sentences

def tmp(x):
    try:
        return x.sentences
    except ValueError:
        return []

data['sent'] = data.textblob.apply(tmp)
data['n_sent'] = data.sent.apply(len)


######################################################################
# Extract and count words

def tmp(x):
    try:
        return x.words
    except ValueError:
        return []

data['word'] = data.textblob.apply(tmp)
data['n_word'] = data.word.apply(len)


data.to_pickle(dataDirectory + 'markov_text.pkl')
