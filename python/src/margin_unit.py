#!/usr/bin/python3
import argparse
import pandas
from pandas.tseries.offsets import BDay, DateOffset
from datetime import date, datetime
import numpy
import sys, time
import marginKV as mkv
import scenarioKV as skv
# display options for pandas
pandas.set_option('display.max_rows', None)
pandas.set_option('display.max_columns', None)
pandas.options.display.width = 1000
pandas.options.display.float_format = '{:,.4f}'.format

# input parameters
#evaluation_date = date(2020, 1, 22)
##confidence_level = 0.997

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

#while True:
#	time.sleep(2)
#	print('sono qui')
#
# # Margin Unit

def portfolioAggregation(position,instrRf_df):
	position = position
	final_merge = pandas.DataFrame()

	for p in position['position_id'].tolist():
##		isin_map = mkv.getInstrumentRf(company,p)
		isin_frame = instrRf_df

		pos_subframe = position.loc[position['position_id'] == p]

		member_merge = pandas.merge(isin_frame, pos_subframe, on='instrument_id', how='inner')

		member_merge['ctv'] = member_merge['ctv'].astype(float)
		member_merge['bucket_flow'] = member_merge['bucket_flow'].astype(float)

		member_merge['FLOW_CTV'] = member_merge['bucket_flow'] / 100 * member_merge['ctv']

		member_merge = member_merge[['account_id', 'rf_id', 'FLOW_CTV']].copy()
		member_merge.sort_values(by=['account_id', 'rf_id'], ascending=True, inplace=True)
		member_merge.reset_index(inplace=True, drop=True)

		final_merge = final_merge.append(member_merge)

	final_merge = pandas.pivot_table(final_merge, index=['rf_id', 'account_id'], aggfunc=numpy.sum).reset_index()

	return final_merge

def expectedShortfall(scenario, position_rf, HPlist, CLlist):
	scenario_frame = scenario

	es_result = []
	hp_TOT, cl_TOT = [], []
	acc_id = []
    
	for hp in HPlist:
		scen_hp = scenario_frame.loc[scenario_frame['hp'] == hp]
		node_reval_frames = []
		for node in position_rf['rf_id'].tolist():
		    # loc and iloc methods are needed to extract rows from a data frame
		    # iloc needs an index; loc can also use boolean selection
		    flow_to_reval = position_rf.loc[position_rf['rf_id'] == node]['FLOW_CTV'].iloc[0]

		    sce_subframe = scen_hp.loc[scen_hp['rf_id'] == node].copy()
		    SUB = numpy.array(sce_subframe['sn_value'].astype('float32'))
			#rivalutazione
		    sce_subframe['G/L_{}'.format(node)] = SUB * flow_to_reval - flow_to_reval
		    sce_subframe = sce_subframe[['rf_dttm', 'G/L_{}'.format(node)]].copy()
		    sce_subframe.reset_index(inplace=True, drop=True)
		   # print(sce_subframe.info())
		    node_reval_frames.append(sce_subframe)

		es_frame = node_reval_frames[0]
		print(type(node_reval_frames[0]))
		#print(node_reval_frames[0])

		for node_reval in node_reval_frames[:]:
			es_frame = pandas.merge(es_frame, node_reval, on=['rf_dttm'], how='inner')

		es_frame['PNL'] = es_frame.iloc[:, 1:].sum(axis=1)

		es_frame = es_frame[['rf_dttm', 'PNL']].copy()
		es_frame.sort_values(by=['PNL'], ascending=True, inplace=True)
		es_frame.reset_index(inplace=True, drop=True)
		
		for cl in CLlist:
			num_events_tail = max(int(round(len(es_frame.index) * (1 - cl), 0)), 1)
			es_frame = es_frame[:num_events_tail]

			es = es_frame['PNL'].mean()
			es_result.append(abs(es))

			cl_TOT.append(cl)
			hp_TOT.append(hp)
			acc_id.append(position_rf['account_id'].iloc[0])
        
	results_frame = pandas.DataFrame({'account_id': acc_id,
				      'EXPECTED_S': es_result,        
				      'HP': hp_TOT,
				      'CONF': cl_TOT})

	return results_frame

##print(' Chosen REQUEST ID: '+REQUEST_ID)
##req = skv.getRequest(company,int(REQUEST_ID))
##print('req: ' + str(req))

##HPstart = int(req['HP'])
##HPlist = [HPstart]
##while len(HPlist) < int(req['HP_CNT']):
##	HP = HPstart + int(req['HP_STEP'])
##	HPlist.append(HP)
##	HPstart = HP

##start = time.time()
##scenario = mkv.getScenario(company)
##end = time.time()
##print('Scenarios loaded in...')
##print(end - start)
##scenario_frame = pandas.DataFrame(scenario.values())

##while (mkv.get_status_of_day(company) != 'End'):
##	account = mkv.getAccount(company)
##	print('About to loop on accounts...')
##	while account:
##		print('account: '+str(account))
##		position = mkv.getPosition(account)

##		position_rf = portfolioAggregation(position)

##		CLstart = float(req['CONF'])
##		CLlist = [CLstart]

##		while len(CLlist) < float(req['CONF_CNT']):
##		    CL = CLstart + float(req['CONF_STEP'])
##		    CLlist.append(CL)
##		    CLstart = CL

##		es = expectedShortfall(scenario, position_rf, CLlist)
##		for index, row in es.iterrows():
##			mkv.putMargin(company,{'account_id':row['account_id'], 'EXPECTED_S':row['EXPECTED_S'], \
##				 'HP':row['HP'], 'CONF':row['CONF'], 'REQ_ID':REQUEST_ID})
##		print('margin for account '+row['account_id']+' has been written')

##		account = mkv.getAccount(company)

##	time.sleep(1)
##	if REQUEST_ID == '2':
##		break # stops in the case of sensitivity test
