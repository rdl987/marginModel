#!/usr/bin/python3
# coding: utf-8
import argparse
import pandas
from pandas.tseries.offsets import BDay, DateOffset
from datetime import date, datetime
import numpy
import sys, time, random
import scenarioKV as skv
# display options for pandas
pandas.set_option('display.max_rows', None)
pandas.set_option('display.max_columns', None)
pandas.options.display.width = 1000
pandas.options.display.float_format = '{:,.4f}'.format

# input parameters
#evaluation_date = date(2020, 1, 22)
##lookback_period = 1000

# argument parser for real-time execution
##parser = argparse.ArgumentParser(description='')
##parser.add_argument('--REQUEST_ID', dest='REQUEST_ID',required=False)
##parser.add_argument('--company', dest='company',required=False)
##args = parser.parse_args()
##if args.company is not None:
##        company = args.company+'-'
##else:
##        company = ''
##REQUEST_ID = args.REQUEST_ID

##print(' Chosen REQUEST_ID: ' + str(REQUEST_ID))

# # Scenario Unit

#old_print = print

#def timestamped_print(*args, **kwargs):
#  old_print(datetime.now(), *args, **kwargs)
#
#print = timestamped_print

def importCountryCurves(node):
	   node_hist = skv.getRfHist(company,node)
	   node_hist_frame = pandas.DataFrame(node_hist.values())
	   node_hist_frame.sort_values(by=['RF_ID', 'rf_dttm'], ascending=False, inplace=True)
	   node_hist_frame.reset_index(drop=True, inplace=True)
	   node_hist_frame.dropna(inplace=True)
	   
	   return node_hist_frame

def createScenarios(hp_list, lookback_period, retrieved_curves):
	used_nodes = retrieved_curves['rf_node'].drop_duplicates().tolist()

	# scenarios construction
	scenarios = pandas.DataFrame()
	for holding_period in hp_list:
		for node_str in used_nodes:
			node = int(node_str)
			
			subframe = retrieved_curves.loc[retrieved_curves['rf_node'] == node_str].copy()
			subframe.sort_values(by=['rf_dttm'], ascending=False, inplace=True)
			
			shifted_prices = subframe['rf_value'][holding_period:].tolist()
			subframe = subframe[:-holding_period]
			
			subframe['SH_CLOSEPR'] = shifted_prices
			
			subframe['rf_value'] = subframe['rf_value'].astype(float)
			subframe['rf_node'] = subframe['rf_node'].astype(int)
			subframe['SH_CLOSEPR'] = subframe['SH_CLOSEPR'].astype(float)

			if node < 1:
				subframe['PRICE'] = 100 * numpy.exp(-subframe['rf_node'] * subframe['rf_value'] / 100)
				subframe['SH_PRICE'] = 100 * numpy.exp(-subframe['rf_node'] * subframe['SH_CLOSEPR'] / 100)
			else:
				subframe['PRICE'] = 100 / (1 + subframe['rf_value'] / 100) ** subframe['rf_node']
				subframe['SH_PRICE'] = 100 / (1 + subframe['SH_CLOSEPR'] / 100) ** subframe['rf_node']

			subframe['SN_VALUE'] = subframe['PRICE'] / subframe['SH_PRICE']
			subframe = subframe[:lookback_period]
			subframe['HP'] = [holding_period] * len(subframe.index)
			       
			scenarios = scenarios.append(subframe)

	scenarios.reset_index(inplace=True, drop=True)

	return scenarios

##req = skv.getRequest(company,int(REQUEST_ID))
##print('req:' + str(req))

##HPstart = int(req['HP'])
##HPlist = [HPstart]
##while len(HPlist) < int(req['HP_CNT']):
##	HP = HPstart + int(req['HP_STEP'])
##	HPlist.append(HP)
##	HPstart = HP

##node = skv.getRf(company)
#print('Creating scenarios...')
#while node:
##	retrieved_curves = importCountryCurves(node)
##	retrieved_curves.head(10)

##	for holding_period in HPlist:
##		scen = createScenarios(holding_period, lookback_period, retrieved_curves)
##		scen = scen[['HP', 'RF_ID', 'RF_DTTM', 'SN_VALUE']].copy()
##		id_scen = 0
	    
##		for index, row in scen.iterrows():
##			skv.putScenario(company,{'HP':row['HP'],'RF_ID':row['RF_ID'],'RF_DTTM':row['RF_DTTM'],'SN_VALUE':row['SN_VALUE']})
##	id_scen += 1
##	node = skv.getRf(company)
##print('All scenarios have been created!')
#exit = random.randint(0,1)
#if exit == 1:
#	exitvar = 'OK'
#else:
#	exitvar = 'KO'
##exitvar = 'OK'
##print(' Exitvar is '+exitvar)
##exitfile = open('/tmp/exitfile','w')
##exitfile.write(exitvar)	
##exitfile.close()
