"""
Again, code borrowed from Ross !
"""

from collections import Counter, defaultdict
import json
import pandas as pd
from textblob import TextBlob

dataDirectory = "/home/twitter-data/website/qacprojects/static/data/twitter_project/tmp_data_test/"

def trigrams(words):
    """Generates trigrams from a list of words."""
    if len(words) < 3:
        return
    for i in range(len(words) - 2):
        yield (words[i], words[i + 1], words[i + 2])

heads = defaultdict(int)
tails = set()
d = defaultdict(lambda: defaultdict(int))


data = pd.read_pickle(dataDirectory + 'markov_text.pkl')

data.text = data.synopsis.map(str.strip)
print data.text

for i, document in enumerate(data.text):
    try:
        for sentence in TextBlob(document).sentences:
            words = sentence.words

            if len(words) < 3:
                continue  # Sentence is too short

            heads[(words[0], words[1])] += 1
            tails.add((words[-2], words[-1]))

            for w1, w2, w3 in trigrams(words):
                d[(w1, w2)][w3] += 1
    except:
        print('Error with document #{}'.format(i))

# Convert frequencies to probabilties.

for bigram in d:
    for unigram in d[bigram]:
        d[bigram][unigram] /= sum(d[bigram].values())

for bigram in heads:
    heads[bigram] /= sum(heads.values())

######################################################################
# Save data for a JavaScript implementation


def tmp(dictionary):
    """Convert collection-type keys to string keys suitable for JSON output."""
    return {' '.join(k): v for k, v in dictionary.items()}

with open(dataDirectory + 'markov.json', 'w') as f:
    json.dump({'heads': tmp(heads),
               'tails': [' '.join(i) for i in tails], 'd': tmp(d)},
              f, ensure_ascii=False, separators=(',', ':'))

