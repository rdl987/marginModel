import pyarrow.feather as feather
import sys
# insert at 1, 0 is the script path (or '' in REPL)
sys.path.insert(1, '/home/delauri/WorkSpaces/CCPRO/python')
import margin_unit as mU


positions_df = feather.read_feather('/home/delauri/WorkSpaces/CCPRO/python/test/positions.feather')
instrumentRf_df = feather.read_feather('/home/delauri/WorkSpaces/CCPRO/python/test/instrumentRf.feather')
scenarios_df = feather.read_feather('/home/delauri/WorkSpaces/CCPRO/python/test/scenarios.feather')
aggrAccountRf_df = mU.portfolioAggregation(positions_df,instrumentRf_df)

HP = 1
HP_STEP = 1
HP_CNT = 2
CONF = 90.00
CONF_STEP =1.00
CONF_CNT = 3

HPstart = HP
HPlist = [HPstart]

while len(HPlist) < HP_CNT:
    
    HP = HPstart + HP_STEP
    HPlist.append(HP)
    HPstart = HP

CLstart = CONF
CLlist = [CLstart]

while len(CLlist) < CONF_CNT:
    
    CL = CLstart + CONF_STEP
    CLlist.append(CL)
    CLstart = CL

resutl = mU.expectedShortfall(scenarios_df,aggrAccountRf_df,HPlist,CLlist)
print(resutl)