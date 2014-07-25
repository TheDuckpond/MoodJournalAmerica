"""
Script runs LDA across newspaper text by overall and region, uploads results to MySQL.
Also returns sentiment score from pos,neg values

@authored malam,habdulkafi 01 July 2014
"""



from gensim import corpora,models,similarities # wonderful, wonderful package
from gensim.corpora.dictionary import Dictionary
from gensim.corpora.mmcorpus import MmCorpus
from gensim.models.tfidfmodel import TfidfModel
import nltk
import os
import us
import pickle
import multiprocessing
from string import punctuation

from pandas.io import sql
import csv
import json
import MySQLdb
import datetime


# statesdict - State abbrev key paired with list of newspapers abbrev values
statesdict = {'WA': ['COL', 'DN', 'TH', 'NT', 'PDN', 'ST', 'SR', 'TCH', 'YHR'], 'DE': ['DSN', 'NJ'], 'DC': ['WP', 'WT'], 'WI': ['BNR', 'BDDC', 'CH', 'DT', 'GBP', 'HTR', 'JG', 'LCT', 'LT', 'MNH', 'MJS', 'ON', 'PDR', 'PC', 'TR', 'SP', 'SPJ', 'WDH', 'WSJ'], 'WV': ['CDM', 'DP', 'HD', 'TJ', 'PNS', 'TWV'], 'HI': ['GI', 'SA'], 'FL': ['CS', 'CCC', 'DLA', 'FTU', 'FT', 'GS', 'HT', 'JCF', 'LCR', 'TL', 'NDN', 'NP', 'NFDN', 'OS', 'PBP', 'PNJ', 'VBPJ', 'SAR', 'SLNT', 'SB', 'SN', 'SS', 'TD', 'TIMES', 'TT', 'TB', 'VDS'], 'WY': ['CST', 'JHD', 'LB', 'WTE'], 'NH': ['COL', 'CM', 'ET', 'PH', 'TT'], 'NJ': ['APP', 'BCT', 'CN', 'CP', 'DJ', 'DR', 'HN', 'HNT', 'JJ', 'NJH', 'PAC', 'SJT', 'SL', 'TTT', 'TT'], 'NM': ['ADN', 'AJ', 'CCA', 'DT', 'DH', 'LCSN', 'RDR', 'SFNM', 'SCSN'], 'TX': ['AAS', 'BH', 'CCCT', 'DS', 'DMN', 'DRC', 'TE', 'EPT', 'FWST', 'GCDN', 'HC', 'DH', 'LNJ', 'LDN', 'TM', 'SAEN', 'TDT', 'TG', 'VA'], 'LA': ['TA', 'AP', 'DA', 'DW', 'NOA', 'NS', 'TT', 'TP', 'TTT'], 'NC': ['ACT', 'CO', 'DA', 'DC', 'DD', 'DR', 'FO', 'GG', 'HS', 'HDR', 'HPE', 'NO', 'NR', 'NH', 'NT', 'RMT', 'SH', 'TS', 'STAR', 'SRL', 'WSJ'], 'ND': ['BT', 'TF', 'GFH'], 'NE': ['BDS', 'CT', 'FT', 'LJS', 'OWH'], 'TN': ['CTFP', 'CA', 'DT', 'JS', 'JCP', 'LC', 'KNS', 'TT'], 'NY': ['AMNY', 'BDN', 'BN', 'TC', 'DCO', 'DF', 'DG', 'DM', 'DN', 'TD', 'EDLP', 'ET', 'HAM', 'IJ', 'JN', 'MT', 'MET', 'NYP', 'NYT', 'ND', 'OJ', 'PS', 'TPS', 'PJ', 'PSB', 'TR', 'RS', 'RDC', 'TS', 'SG', 'THR', 'TU', 'WDT'], 'PA': ['AM', 'BCT', 'BCCT', 'CDT', 'CPO', 'CV', 'DA', 'DLN', 'DCDT', 'ETN', 'GT', 'HES', 'TI', 'IJ', 'LB', 'LDN', 'MER', 'MET', 'MC', 'NI', 'PN', 'PDN', 'PI', 'PPG', 'PTR', 'PR', 'RE', 'TR', 'TS', 'SS', 'TH', 'TT', 'TD', 'GTR', 'WSG', 'YDR', 'YD'], 'CA': ['AD', 'AJ', 'BC', 'CCT', 'DB', 'DN', 'TDN', 'DP', 'DS', 'ER', 'ETS', 'FB', 'MCH', 'IVDB', 'LO', 'LNS', 'LR', 'LAR', 'LAT', 'MANT', 'MIJ', 'MSS', 'MB', 'NVR', 'OT', 'OCR', 'PSN', 'PRP', 'PE', 'PT', 'TR', 'RBDN', 'RDF', 'REP', 'SB', 'SC', 'SDUT', 'SFC', 'SFE', 'SGVT', 'SJMN', 'SMDJ', 'SCS', 'SMT', 'TH', 'TT', 'TAR', 'TU', 'VTD', 'WRP', 'WDN'], 'NV': ['SUN', 'RGJ'], 'VA': ['CSE', 'DNR', 'DPRESS', 'DP', 'DRB', 'FLS', 'NA', 'NL', 'NV', 'NVD', 'RTD', 'VP', 'WS'], 'CO': ['AT', 'CCDR', 'CD', 'DC', 'DS', 'DP', 'FCC', 'TG', 'PI', 'GT', 'LTC', 'LRH', 'SPT', 'VD'], 'AK': ['ADN', 'FDNM', 'JE'], 'AL': ['AS', 'DS', 'DD', 'DE', 'EL', 'GT', 'MA', 'OAN', 'TD', 'TJ', 'TN'], 'AR': ['ADG', 'BB', 'HDT', 'SR'], 'VT': ['BFP', 'RH', 'TA'], 'IL': ['BND', 'CST', 'CT', 'DCN', 'DC', 'DH', 'DHR', 'TD', 'HOY', 'JG', 'JS', 'KCC', 'LISLE', 'NG', 'NH', 'TP', 'RE', 'RM', 'RIA', 'SI'], 'GA': ['AH', 'AJC', 'AC', 'BN', 'GDN', 'LE', 'MDJ', 'NC', 'RC', 'RNT', 'SMN', 'TT', 'GT', 'TG'], 'IN': ['ET', 'ECP', 'HT', 'IS', 'JC', 'JG', 'KT', 'NAT', 'PI', 'PHA', 'PDC', 'RR', 'SBT', 'SP', 'TT', 'TS', 'VSC'], 'IA': ['CCP', 'DR', 'TG', 'HE', 'PC', 'MCGG', 'MJ', 'QCT', 'SCJ', 'TH', 'TR', 'WC'], 'OK': ['NT', 'DOK', 'PDJ', 'TDP', 'TW', 'WEDN'], 'AZ': ['SUN', 'AI', 'AR', 'DC', 'KDM'], 'ID': ['BCDB', 'IPT', 'IS', 'TN'], 'CT': ['TA', 'CP', 'TD', 'GT', 'HC', 'TH', 'MP', 'NHR', 'NT', 'NB', 'RC'], 'ME': ['BDN', 'KJ', 'MS', 'PPH', 'SJ'], 'MD': ['TS', 'CCT', 'DT', 'FNP', 'SD'], 'MA': ['BG', 'BH', 'CCT', 'TE', 'HN', 'MET', 'MWDN', 'MDN', 'PL', 'SE', 'ST', 'TS', 'SC', 'TDG', 'TG'], 'OH': ['ABJ', 'AM', 'TB', 'CT', 'CE', 'CH', 'CD', 'TC', 'CN', 'DDN', 'TI', 'JN', 'MT', 'MG', 'TMJ', 'NH', 'CPD', 'RC', 'REP', 'SR', 'SNS', 'TR', 'TV'], 'UT': ['DH', 'DN', 'HJ', 'SLT', 'TS'], 'MO': ['FS', 'JG', 'KCS', 'LS', 'NL', 'RDN', 'SJNP', 'SLPD'], 'MN': ['BP', 'BD', 'DNT', 'FP', 'SCT', 'ST', 'WCT', 'WDN'], 'MI': ['BCE', 'DFP', 'DN', 'GRP', 'HS', 'JCP', 'KG', 'LSJ', 'MD', 'MNA', 'MEN', 'MS', 'MC', 'OP', 'PIO', 'TH', 'TCRE'], 'RI': ['NDN', 'PJ'], 'KS': ['DU', 'HN', 'LJW', 'OH', 'SJ', 'TCJ', 'WE'], 'MT': ['DIL', 'GFT', 'IR', 'MIS'], 'MS': ['CL', 'NMDJ', 'SH'], 'SC': ['AS', 'BG', 'GN', 'IJ', 'IP', 'TI', 'MN', 'PC', 'TS', 'TD'], 'KY': ['AM', 'CJ', 'TG', 'KE', 'LHL', 'TM', 'MI', 'NE'], 'OR': ['CGT', 'DT', 'EO', 'HN', 'MT', 'TO', 'RG', 'SJ'], 'SD': ['AN', 'SFAL', 'RCJ', 'YDP']}

# fipsdict - Region key paired with dictionary of subregions paired with list of fips value
fipsdict = {'Northeast':{'New England':['09','23','25','33','44','50'],'Middle Atlantic':['34','36','42']},'Midwest':{'East North Central':['18','17','26','39','55'],'West North Central':['19','20','27','29','31','38','46']},'South':{'South Atlantic':['10','11','12','13','24','37','45','51','54'],'East South Central':['01','21','28','47'],'West South Central':['05','22','40','48']},'West':{'Mountain':['04','08','16','35','30','49','32','56'],'Pacific':['02','06','15','41','53']}}

# fipsabbr - fips key paired with state abbreviation value
fipsabbr = us.states.mapping('fips','abbr')

# gets today's date
todaydate = datetime.date.today()

# creates a timestamp from today's date
datestamp = todaydate.strftime('%Y-%m-%d')

# load in parameters file
with open('') as f:
    p = json.load(f)

conn = MySQLdb.connect(host=p['mysql']['host'],
                       user=p['mysql']['user'],
                       passwd=p['mysql']['passwd'],
                       db=p['mysql']['db'],
                       charset='utf8')

# join daily_download table and geo table into a pandas dataframe
q = u'SELECT d.daily_id, d.url, d.pos, d.neg, d.text, d.timestamp, g.fips, g.region, g.sub_region, d.ext, g.state FROM geo as g INNER JOIN daily_download as d on d.state=g.state WHERE timestamp = (select curdate());'
df = sql.read_frame(q ,conn)

# clean up the text
def featurize(document_text):
	text = document_text.lower().split()
	text = [i.strip(punctuation) for i in text]
	text = [i for word in text for i in nltk.word_tokenize(word)]
	text = [i for i in text if len(i) > 3]
	text = [i for i in text if i not in stoplist]
	return text

# for topic moddelling - we're not interested in a weather topic, probably a better way to handle this in the future
# TO DO: clean this up. 
colors = list(set('Cyan Magenta Yellow Black cyan magenta yellow black'.split())) # when extracting text from PDFs color words frequently show up as a result of pictures
days = ['monday','Monday','tuesday','Tuesday','wednesday','Wednesday','thursday','Thursday','friday','Friday','saturday','Saturday','sunday','Sunday','tomorrow','yesterday']
months = ['january','January','february','February','march','March','april','April','may','May','june','June','july','July','august','August','september','September','october','October','november','November','december','December']
years = ['2013','2014','2015','2016','2017','2018','2019','2020']
loc = ['state','county','city']
random_words = ['would','could','should','said','last','daily','u.s.','us','like']
weather = ['weather','rain','sunny','thunder','storm','snow','fog','highs','lows','high']
stoplist = nltk.corpus.stopwords.words('english') + list(set('it\'s page also since many would news July August local police state county 2014 said city Monday Tuesday Wednesday Thursday Friday Saturday Sunday'.split() + colors + days + months + years + loc + random_words + weather))

# this function takes a keyword (either region or subregion or 'all') and returns the topics from that area
def topicmodel(keyword):
	# decides what to do with the keyword
	if keyword == 'all':
		documents = df['text'].tolist()
	elif keyword in fipsdict.keys():
		documents = df[df['region'] == keyword]['text'].tolist()
	else:
		documents = df[df['sub_region'] == keyword]['text'].tolist()

	# creates an iterator object from the documents that multiprocessing needs to work
	doc_iterator = iter(documents)

	# initiates the pool
	pool = multiprocessing.Pool()

	# the heavy lifting - uses the multiprocessing to create a corpus from all the documents
	corpus = pool.imap(featurize,doc_iterator,chunksize = 100)

	# closes the pool
	pool.close()

	# creates a dictionary from all the documents in the corpus
	dictionary = Dictionary(i for i in corpus)

	## values to fidget with
	# dictionary.filter_extremes(no_below=5,no_above=.5,keep_n = 100000)
	# dictionary.filter_extremes(no_below=5,no_above=.5,keep_n = 1)


### Ref Radim's Gensim tutorial, which is EXCELLENT 
### http://radimrehurek.com/gensim/tutorial.html

	# creates a MyCorpus class
	class MyCorpus(object):
		def __iter__(self):
			for i in documents:
				yield dictionary.doc2bow(i.lower().split())

	# create an instance of the class
	corpus_memory_friendly = MyCorpus() 

	# serialize corpus for later use
	corpora.MmCorpus.serialize('news_corp.mm',corpus_memory_friendly)
	corpus = corpus_memory_friendly 

	# initialize model
	tfidf = models.TfidfModel(corpus) 
	corpus_tfidf = tfidf[corpus]

	# rename
	id2word = dictionary 

	# load corpus
	mm = corpora.MmCorpus('news_corp.mm') 

	# LDA is the model of choice
	# TO DO: reconsider number of topics / passes
	lda = models.ldamodel.LdaModel(corpus=corpus,id2word=id2word,num_topics=4,passes=3)

	# returns the topics from the LDA model
	f = ''
	for line in lda.show_topics(topics=-1):
		f= f + line + '\n'
	return f

# regions and subregions according to FIPS
# topics for whole country, regions
topics = topicmodel('all')
topics_reg1 = topicmodel('Northeast')
topics_reg2 = topicmodel('Midwest')
topics_reg3 = topicmodel('South')
topics_reg4 = topicmodel('West')

# basic sentiment calculations from pos,neg values ( abs(pos) - abs(neg))
# TO DO: safety if preceding scripts do not run (start script does this already, but not a bad idea)
sentiment =  (df['pos'].sum() - df['neg'].sum() + 0.0) / len(df.index)
sentiment_reg1 = (df[df['region'] == 'Northeast']['pos'].sum() - df[df['region'] == 'Northeast']['neg'].sum() + 0.0) / len(df[df['region'] == 'Northeast'].index)
sentiment_reg2 = (df[df['region'] == 'Midwest']['pos'].sum() - df[df['region'] == 'Midwest']['neg'].sum() + 0.0) / len(df[df['region'] == 'Midwest'].index)
sentiment_reg3 = (df[df['region'] == 'South']['pos'].sum() - df[df['region'] == 'South']['neg'].sum() + 0.0) / len(df[df['region'] == 'South'].index)
sentiment_reg4 = (df[df['region'] == 'West']['pos'].sum() - df[df['region'] == 'West']['neg'].sum() + 0.0) / len(df[df['region'] == 'West'].index)


# insert results into MySQL
c = conn.cursor()
q = u'INSERT INTO daily_overall (date,topics,topics_reg1,topics_reg2,topics_reg3,topics_reg4,sentiment,sentiment_reg1,sentiment_reg2,sentiment_reg3,sentiment_reg4) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
c.execute(q, (datestamp,topics,topics_reg1,topics_reg2,topics_reg3,topics_reg4,sentiment,sentiment_reg1,sentiment_reg2,sentiment_reg3,sentiment_reg4))

#conn.close()
