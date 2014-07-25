"""
This script allows for parsing readable online PDF documents that are otherwise encrypted and annoying

@authored malam,habdulkafi 30 June 2014

"""
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfdevice import PDFDevice
from pdfminer.layout import LAParams
from pdfminer.converter import  TextConverter
from StringIO import StringIO
import logging
import logging.handlers
import Queue
import re
import socket
import threading
import time
import warnings

import MySQLdb
import requests
import pickle
import us
import datetime
import json


import sentiment # qac module


regexcid = re.compile('\(cid\:\d+\)')


logger = logging.getLogger('Stream_Logger')
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')

# Create a handler to write low-priority messages to a file.
handler = logging.FileHandler(filename='daily_news.log')
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)

with open('') as f:
    p = json.load(f)

conn = MySQLdb.connect(host=p['mysql']['host'],
                       user=p['mysql']['user'],
                       passwd=p['mysql']['passwd'],
                       db=p['mysql']['db'],
                       charset='utf8')
c = conn.cursor()

warnings.filterwarnings('ignore', category=MySQLdb.Warning)

# TO DO: JSON file? 

#### Dictionaries for Geo / date mapping #####
statesdict = {'WA': ['COL', 'DN', 'TH', 'NT', 'PDN', 'ST', 'SR', 'TCH', 'YHR'], 'DE': ['DSN', 'NJ'], 'DC': ['WP', 'WT'], 'WI': ['BNR', 'BDDC', 'CH', 'DT', 'GBP', 'HTR', 'JG', 'LCT', 'LT', 'MNH', 'MJS', 'ON', 'PDR', 'PC', 'TR', 'SP', 'SPJ', 'WDH', 'WSJ'], 'WV': ['CDM', 'DP', 'HD', 'TJ', 'PNS', 'TWV'], 'HI': ['GI', 'SA'], 'FL': ['CS', 'CCC', 'DLA', 'FTU', 'FT', 'GS', 'HT', 'JCF', 'LCR', 'TL', 'NDN', 'NP', 'NFDN', 'OS', 'PBP', 'PNJ', 'VBPJ', 'SAR', 'SLNT', 'SB', 'SN', 'SS', 'TD', 'TIMES', 'TT', 'TB', 'VDS'], 'WY': ['CST', 'JHD', 'LB', 'WTE'], 'NH': ['COL', 'CM', 'ET', 'PH', 'TT'], 'NJ': ['APP', 'BCT', 'CN', 'CP', 'DJ', 'DR', 'HN', 'HNT', 'JJ', 'NJH', 'PAC', 'SJT', 'SL', 'TTT', 'TT'], 'NM': ['ADN', 'AJ', 'CCA', 'DT', 'DH', 'LCSN', 'RDR', 'SFNM', 'SCSN'], 'TX': ['AAS', 'BH', 'CCCT', 'DS', 'DMN', 'DRC', 'TE', 'EPT', 'FWST', 'GCDN', 'HC', 'DH', 'LNJ', 'LDN', 'TM', 'SAEN', 'TDT', 'TG', 'VA'], 'LA': ['TA', 'AP', 'DA', 'DW', 'NOA', 'NS', 'TT', 'TP', 'TTT'], 'NC': ['ACT', 'CO', 'DA', 'DC', 'DD', 'DR', 'FO', 'GG', 'HS', 'HDR', 'HPE', 'NO', 'NR', 'NH', 'NT', 'RMT', 'SH', 'TS', 'STAR', 'SRL', 'WSJ'], 'ND': ['BT', 'TF', 'GFH'], 'NE': ['BDS', 'CT', 'FT', 'LJS', 'OWH'], 'TN': ['CTFP', 'CA', 'DT', 'JS', 'JCP', 'LC', 'KNS', 'TT'], 'NY': ['AMNY', 'BDN', 'BN', 'TC', 'DCO', 'DF', 'DG', 'DM', 'DN', 'TD', 'EDLP', 'ET', 'HAM', 'IJ', 'JN', 'MT', 'MET', 'NYP', 'NYT', 'ND', 'OJ', 'PS', 'TPS', 'PJ', 'PSB', 'TR', 'RS', 'RDC', 'TS', 'SG', 'THR', 'TU', 'WDT'], 'PA': ['AM', 'BCT', 'BCCT', 'CDT', 'CPO', 'CV', 'DA', 'DLN', 'DCDT', 'ETN', 'GT', 'HES', 'TI', 'IJ', 'LB', 'LDN', 'MER', 'MET', 'MC', 'NI', 'PN', 'PDN', 'PI', 'PPG', 'PTR', 'PR', 'RE', 'TR', 'TS', 'SS', 'TH', 'TT', 'TD', 'GTR', 'WSG', 'YDR', 'YD'], 'CA': ['AD', 'AJ', 'BC', 'CCT', 'DB', 'DN', 'TDN', 'DP', 'DS', 'ER', 'ETS', 'FB', 'MCH', 'IVDB', 'LO', 'LNS', 'LR', 'LAR', 'LAT', 'MANT', 'MIJ', 'MSS', 'MB', 'NVR', 'OT', 'OCR', 'PSN', 'PRP', 'PE', 'PT', 'TR', 'RBDN', 'RDF', 'REP', 'SB', 'SC', 'SDUT', 'SFC', 'SFE', 'SGVT', 'SJMN', 'SMDJ', 'SCS', 'SMT', 'TH', 'TT', 'TAR', 'TU', 'VTD', 'WRP', 'WDN'], 'NV': ['SUN', 'RGJ'], 'VA': ['CSE', 'DNR', 'DPRESS', 'DP', 'DRB', 'FLS', 'NA', 'NL', 'NV', 'NVD', 'RTD', 'VP', 'WS'], 'CO': ['AT', 'CCDR', 'CD', 'DC', 'DS', 'DP', 'FCC', 'TG', 'PI', 'GT', 'LTC', 'LRH', 'SPT', 'VD'], 'AK': ['ADN', 'FDNM', 'JE'], 'AL': ['AS', 'DS', 'DD', 'DE', 'EL', 'GT', 'MA', 'OAN', 'TD', 'TJ', 'TN'], 'AR': ['ADG', 'BB', 'HDT', 'SR'], 'VT': ['BFP', 'RH', 'TA'], 'IL': ['BND', 'CST', 'CT', 'DCN', 'DC', 'DH', 'DHR', 'TD', 'HOY', 'JG', 'JS', 'KCC', 'LISLE', 'NG', 'NH', 'TP', 'RE', 'RM', 'RIA', 'SI'], 'GA': ['AH', 'AJC', 'AC', 'BN', 'GDN', 'LE', 'MDJ', 'NC', 'RC', 'RNT', 'SMN', 'TT', 'GT', 'TG'], 'IN': ['ET', 'ECP', 'HT', 'IS', 'JC', 'JG', 'KT', 'NAT', 'PI', 'PHA', 'PDC', 'RR', 'SBT', 'SP', 'TT', 'TS', 'VSC'], 'IA': ['CCP', 'DR', 'TG', 'HE', 'PC', 'MCGG', 'MJ', 'QCT', 'SCJ', 'TH', 'TR', 'WC'], 'OK': ['NT', 'DOK', 'PDJ', 'TDP', 'TW', 'WEDN'], 'AZ': ['SUN', 'AI', 'AR', 'DC', 'KDM'], 'ID': ['BCDB', 'IPT', 'IS', 'TN'], 'CT': ['TA', 'CP', 'TD', 'GT', 'HC', 'TH', 'MP', 'NHR', 'NT', 'NB', 'RC'], 'ME': ['BDN', 'KJ', 'MS', 'PPH', 'SJ'], 'MD': ['TS', 'CCT', 'DT', 'FNP', 'SD'], 'MA': ['BG', 'BH', 'CCT', 'TE', 'HN', 'MET', 'MWDN', 'MDN', 'PL', 'SE', 'ST', 'TS', 'SC', 'TDG', 'TG'], 'OH': ['ABJ', 'AM', 'TB', 'CT', 'CE', 'CH', 'CD', 'TC', 'CN', 'DDN', 'TI', 'JN', 'MT', 'MG', 'TMJ', 'NH', 'CPD', 'RC', 'REP', 'SR', 'SNS', 'TR', 'TV'], 'UT': ['DH', 'DN', 'HJ', 'SLT', 'TS'], 'MO': ['FS', 'JG', 'KCS', 'LS', 'NL', 'RDN', 'SJNP', 'SLPD'], 'MN': ['BP', 'BD', 'DNT', 'FP', 'SCT', 'ST', 'WCT', 'WDN'], 'MI': ['BCE', 'DFP', 'DN', 'GRP', 'HS', 'JCP', 'KG', 'LSJ', 'MD', 'MNA', 'MEN', 'MS', 'MC', 'OP', 'PIO', 'TH', 'TCRE'], 'RI': ['NDN', 'PJ'], 'KS': ['DU', 'HN', 'LJW', 'OH', 'SJ', 'TCJ', 'WE'], 'MT': ['DIL', 'GFT', 'IR', 'MIS'], 'MS': ['CL', 'NMDJ', 'SH'], 'SC': ['AS', 'BG', 'GN', 'IJ', 'IP', 'TI', 'MN', 'PC', 'TS', 'TD'], 'KY': ['AM', 'CJ', 'TG', 'KE', 'LHL', 'TM', 'MI', 'NE'], 'OR': ['CGT', 'DT', 'EO', 'HN', 'MT', 'TO', 'RG', 'SJ'], 'SD': ['AN', 'SFAL', 'RCJ', 'YDP']}
cities = ['AL_AS', 'AL_DS', 'AL_DD', 'AL_DE', 'AL_EL', 'AL_GT', 'AL_MA', 'AL_OAN', 'AL_TD', 'AL_TJ', 'AL_TN', 'AK_ADN', 'AK_FDNM', 'AK_JE', 'AZ_SUN', 'AZ_AI', 'AZ_AR', 'AZ_DC', 'AZ_KDM', 'AR_ADG', 'AR_BB', 'AR_HDT', 'AR_SR', 'CA_AD', 'CA_AJ', 'CA_BC', 'CA_CCT', 'CA_DB', 'CA_DN', 'CA_TDN', 'CA_DP', 'CA_DS', 'CA_ER', 'CA_ETS', 'CA_FB', 'CA_MCH', 'CA_IVDB', 'CA_LO', 'CA_LNS', 'CA_LR', 'CA_LAR', 'CA_LAT', 'CA_MANT', 'CA_MIJ', 'CA_MSS', 'CA_MB', 'CA_NVR', 'CA_OT', 'CA_OCR', 'CA_PSN', 'CA_PRP', 'CA_PE', 'CA_PT', 'CA_TR', 'CA_RBDN', 'CA_RDF', 'CA_REP', 'CA_SB', 'CA_SC', 'CA_SDUT', 'CA_SFC', 'CA_SFE', 'CA_SGVT', 'CA_SJMN', 'CA_SMDJ', 'CA_SCS', 'CA_SMT', 'CA_TH', 'CA_TT', 'CA_TAR', 'CA_TU', 'CA_VTD', 'CA_WRP', 'CA_WDN', 'CO_AT', 'CO_CCDR', 'CO_CD', 'CO_DC', 'CO_DS', 'CO_DP', 'CO_FCC', 'CO_TG', 'CO_PI', 'CO_GT', 'CO_LTC', 'CO_LRH', 'CO_SPT', 'CO_VD', 'CT_TA', 'CT_CP', 'CT_TD', 'CT_GT', 'CT_HC', 'CT_TH', 'CT_MP', 'CT_NHR', 'CT_NT', 'CT_NB', 'CT_RC', 'DE_DSN', 'DE_NJ', 'DC_WP', 'DC_WT', 'FL_CS', 'FL_CCC', 'FL_DLA', 'FL_FTU', 'FL_FT', 'FL_GS', 'FL_HT', 'FL_JCF', 'FL_LCR', 'FL_TL', 'FL_NDN', 'FL_NP', 'FL_NFDN', 'FL_OS', 'FL_PBP', 'FL_PNJ', 'FL_VBPJ', 'FL_SAR', 'FL_SLNT', 'FL_SB', 'FL_SN', 'FL_SS', 'FL_TD', 'FL_TIMES', 'FL_TT', 'FL_TB', 'FL_VDS', 'GA_AH', 'GA_AJC', 'GA_AC', 'GA_BN', 'GA_GDN', 'GA_LE', 'GA_MDJ', 'GA_NC', 'GA_RC', 'GA_RNT', 'GA_SMN', 'GA_TT', 'GA_GT', 'GA_TG', 'HI_GI', 'HI_SA', 'ID_BCDB', 'ID_IPT', 'ID_IS', 'ID_TN', 'IL_BND', 'IL_CST', 'IL_CT', 'IL_DCN', 'IL_DC', 'IL_DH', 'IL_DHR', 'IL_TD', 'IL_HOY', 'IL_JG', 'IL_JS', 'IL_KCC', 'IL_LISLE', 'IL_NG', 'IL_NH', 'IL_TP', 'IL_RE', 'IL_RM', 'IL_RIA', 'IL_SI', 'IN_ET', 'IN_ECP', 'IN_HT', 'IN_IS', 'IN_JC', 'IN_JG', 'IN_KT', 'IN_NAT', 'IN_PI', 'IN_PHA', 'IN_PDC', 'IN_RR', 'IN_SBT', 'IN_SP', 'IN_TT', 'IN_TS', 'IN_VSC', 'IA_CCP', 'IA_DR', 'IA_TG', 'IA_HE', 'IA_PC', 'IA_MCGG', 'IA_MJ', 'IA_QCT', 'IA_SCJ', 'IA_TH', 'IA_TR', 'IA_WC', 'KS_DU', 'KS_HN', 'KS_LJW', 'KS_OH', 'KS_SJ', 'KS_TCJ', 'KS_WE', 'KY_AM', 'KY_CJ', 'KY_TG', 'KY_KE', 'KY_LHL', 'KY_TM', 'KY_MI', 'KY_NE', 'LA_TA', 'LA_AP', 'LA_DA', 'LA_DW', 'LA_NOA', 'LA_NS', 'LA_TT', 'LA_TP', 'LA_TTT', 'ME_BDN', 'ME_KJ', 'ME_MS', 'ME_PPH', 'ME_SJ', 'MD_TS', 'MD_CCT', 'MD_DT', 'MD_FNP', 'MD_SD', 'MA_BG', 'MA_BH', 'MA_CCT', 'MA_TE', 'MA_HN', 'MA_MET', 'MA_MWDN', 'MA_MDN', 'MA_PL', 'MA_SE', 'MA_ST', 'MA_TS', 'MA_SC', 'MA_TDG', 'MA_TG', 'MI_BCE', 'MI_DFP', 'MI_DN', 'MI_GRP', 'MI_HS', 'MI_JCP', 'MI_KG', 'MI_LSJ', 'MI_MD', 'MI_MNA', 'MI_MEN', 'MI_MS', 'MI_MC', 'MI_OP', 'MI_PIO', 'MI_TH', 'MI_TCRE', 'MN_BP', 'MN_BD', 'MN_DNT', 'MN_FP', 'MN_SCT', 'MN_ST', 'MN_WCT', 'MN_WDN', 'MS_CL', 'MS_NMDJ', 'MS_SH', 'MO_FS', 'MO_JG', 'MO_KCS', 'MO_LS', 'MO_NL', 'MO_RDN', 'MO_SJNP', 'MO_SLPD', 'MT_DIL', 'MT_GFT', 'MT_IR', 'MT_MIS', 'NE_BDS', 'NE_CT', 'NE_FT', 'NE_LJS', 'NE_OWH', 'NV_SUN', 'NV_RGJ', 'NH_COL', 'NH_CM', 'NH_ET', 'NH_PH', 'NH_TT', 'NJ_APP', 'NJ_BCT', 'NJ_CN', 'NJ_CP', 'NJ_DJ', 'NJ_DR', 'NJ_HN', 'NJ_HNT', 'NJ_JJ', 'NJ_NJH', 'NJ_PAC', 'NJ_SJT', 'NJ_SL', 'NJ_TTT', 'NJ_TT', 'NM_ADN', 'NM_AJ', 'NM_CCA', 'NM_DT', 'NM_DH', 'NM_LCSN', 'NM_RDR', 'NM_SFNM', 'NM_SCSN', 'NY_AMNY', 'NY_BDN', 'NY_BN', 'NY_TC', 'NY_DCO', 'NY_DF', 'NY_DG', 'NY_DM', 'NY_DN', 'NY_TD', 'NY_EDLP', 'NY_ET', 'NY_HAM', 'NY_IJ', 'NY_JN', 'NY_MT', 'NY_MET', 'NY_NYP', 'NY_NYT', 'NY_ND', 'NY_OJ', 'NY_PS', 'NY_TPS', 'NY_PJ', 'NY_PSB', 'NY_TR', 'NY_RS', 'NY_RDC', 'NY_TS', 'NY_SG', 'NY_THR', 'NY_TU', 'NY_WDT', 'NC_ACT', 'NC_CO', 'NC_DA', 'NC_DC', 'NC_DD', 'NC_DR', 'NC_FO', 'NC_GG', 'NC_HS', 'NC_HDR', 'NC_HPE', 'NC_NO', 'NC_NR', 'NC_NH', 'NC_NT', 'NC_RMT', 'NC_SH', 'NC_TS', 'NC_STAR', 'NC_SRL', 'NC_WSJ', 'ND_BT', 'ND_TF', 'ND_GFH', 'OH_ABJ', 'OH_AM', 'OH_TB', 'OH_CT', 'OH_CE', 'OH_CH', 'OH_CD', 'OH_TC', 'OH_CN', 'OH_DDN', 'OH_TI', 'OH_JN', 'OH_MT', 'OH_MG', 'OH_TMJ', 'OH_NH', 'OH_CPD', 'OH_RC', 'OH_REP', 'OH_SR', 'OH_SNS', 'OH_TR', 'OH_TV', 'OK_NT', 'OK_DOK', 'OK_PDJ', 'OK_TDP', 'OK_TW', 'OK_WEDN', 'OR_CGT', 'OR_DT', 'OR_EO', 'OR_HN', 'OR_MT', 'OR_TO', 'OR_RG', 'OR_SJ', 'PA_AM', 'PA_BCT', 'PA_BCCT', 'PA_CDT', 'PA_CPO', 'PA_CV', 'PA_DA', 'PA_DLN', 'PA_DCDT', 'PA_ETN', 'PA_GT', 'PA_HES', 'PA_TI', 'PA_IJ', 'PA_LB', 'PA_LDN', 'PA_MER', 'PA_MET', 'PA_MC', 'PA_NI', 'PA_PN', 'PA_PDN', 'PA_PI', 'PA_PPG', 'PA_PTR', 'PA_PR', 'PA_RE', 'PA_TR', 'PA_TS', 'PA_SS', 'PA_TH', 'PA_TT', 'PA_TD', 'PA_GTR', 'PA_WSG', 'PA_YDR', 'PA_YD', 'RI_NDN', 'RI_PJ', 'SC_AS', 'SC_BG', 'SC_GN', 'SC_IJ', 'SC_IP', 'SC_TI', 'SC_MN', 'SC_PC', 'SC_TS', 'SC_TD', 'SD_AN', 'SD_SFAL', 'SD_RCJ', 'SD_YDP', 'TN_CTFP', 'TN_CA', 'TN_DT', 'TN_JS', 'TN_JCP', 'TN_LC', 'TN_KNS', 'TN_TT', 'TX_AAS', 'TX_BH', 'TX_CCCT', 'TX_DS', 'TX_DMN', 'TX_DRC', 'TX_TE', 'TX_EPT', 'TX_FWST', 'TX_GCDN', 'TX_HC', 'TX_DH', 'TX_LNJ', 'TX_LDN', 'TX_TM', 'TX_SAEN', 'TX_TDT', 'TX_TG', 'TX_VA', 'UT_DH', 'UT_DN', 'UT_HJ', 'UT_SLT', 'UT_TS', 'VT_BFP', 'VT_RH', 'VT_TA', 'VA_CSE', 'VA_DNR', 'VA_DPRESS', 'VA_DP', 'VA_DRB', 'VA_FLS', 'VA_NA', 'VA_NL', 'VA_NV', 'VA_NVD', 'VA_RTD', 'VA_VP', 'VA_WS', 'WA_COL', 'WA_DN', 'WA_TH', 'WA_NT', 'WA_PDN', 'WA_ST', 'WA_SR', 'WA_TCH', 'WA_YHR', 'WV_CDM', 'WV_DP', 'WV_HD', 'WV_TJ', 'WV_PNS', 'WV_TWV', 'WI_BNR', 'WI_BDDC', 'WI_CH', 'WI_DT', 'WI_GBP', 'WI_HTR', 'WI_JG', 'WI_LCT', 'WI_LT', 'WI_MNH', 'WI_MJS', 'WI_ON', 'WI_PDR', 'WI_PC', 'WI_TR', 'WI_SP', 'WI_SPJ', 'WI_WDH', 'WI_WSJ', 'WY_CST', 'WY_JHD', 'WY_LB', 'WY_WTE']
day_to_month = {'01':'January','02':'February','03':'March','04':'April','05':'May','06':'June','07':'July','08':'August','09':'September','10':'October','11':'November','12':'December'}
fipsdict = {'Northeast':{'New England':['09','23','25','33','44','50'],'Middle Atlantic':['34','36','42']},'Midwest':{'East North Central':['18','17','26','39','55'],'West North Central':['19','20','27','29','31','38','46']},'South':{'South Atlantic':['10','11','12','13','24','37','45','51','54'],'East South Central':['01','21','28','47'],'West South Central':['05','22','40','48']},'West':{'Mountain':['04','08','16','35','30','49','32','56'],'Pacific':['02','06','15','41','53']}}
fipsabbr = us.states.mapping('fips','abbr')

# get today's date, YYYY-DD-MM
todaydate = datetime.date.today()
todaydaynum = todaydate.strftime('%d')
if todaydaynum[0] == '0':
	todaydaynum = todaydaynum[1]


def dwn_pdf_txt(url):
	""" Given a readable but encrypted PDF URL, parses document to text """

	r = requests.get(url)

	memory_file = StringIO(r.content)

	# Create a PDF parser object associated with the StringIO object
	parser = PDFParser(memory_file)

	# Create a PDF document object that stores the document structure
	document = PDFDocument(parser)

	# Define parameters to the PDF device objet 
	rsrcmgr = PDFResourceManager()
	retstr = StringIO()
	laparams = LAParams()
	codec = 'utf-8'

	# Create a PDF device object
	device = TextConverter(rsrcmgr, retstr, codec = codec, laparams = laparams)

	# Create a PDF interpreter object
	interpreter = PDFPageInterpreter(rsrcmgr, device)

	# Process each page contained in the document
	for page in PDFPage.create_pages(document):
	    interpreter.process_page(page)
	    parsed_document =  retstr.getvalue()

	return parsed_document # everything is stored here, needs to be cleaned up

docstringdict = {}
def on_download():
	for state in statesdict.keys():
		for statext in statesdict[state]:
			try:
				url = "http://webmedia.newseum.org/newseum-multimedia/dfp/pdf" + todaydaynum + "/" + state + '_' + statext + ".pdf"
				to_up = dwn_pdf_txt("http://webmedia.newseum.org/newseum-multimedia/dfp/pdf" + todaydaynum + "/" + state + '_' + statext + ".pdf")
				to_up = re.sub(regexcid,'',to_up)
				docstringdict[state + '_' + statext] = to_up
				
				datestamp = todaydate.strftime('%Y-%m-%d')
				uniqid = idmaker(todaydate.strftime('%m'),todaydate.strftime("%d"),state + '_' + statext)
				bag_of_words = sentiment.preprocess(to_up)

				p, n = sentiment.score(bag_of_words,
					sentiment.positive_words,
					sentiment.negative_words)
				q = u'INSERT INTO daily_download (daily_id,text,url,state,timestamp,ext,pos,neg) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);'

				c.execute(q, (uniqid,to_up,url,state,datestamp,statext,p,n))
			except:
				print state + '_' + statext
	pickle.dump(docstringdict,open('docstringdict','wb'))



def idmaker(month, day, fullstname):
	month = str(month)
	day = str(day)
	if len(day) == 1:
		day = '0' + day
	if len(month) == 1:
		month = '0' + month
	return fullstname.replace('_','-') + '-' + month + day



on_download()