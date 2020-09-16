import pyarrow.feather as feather
import sys
# insert at 1, 0 is the script path (or '' in REPL)
sys.path.insert(1, '/home/delauri/WorkSpaces/CCPRO/python')
import scenario_unit as sU

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

df = feather.read_feather("/home/delauri/WorkSpaces/CCPRO/python/test/retrieved_curves.feather")
result = sU.createScenarios(HPlist,1000,df)
feather.write_feather(result,'/home/delauri/WorkSpaces/CCPRO/python/test/scenarios.feather')
print(result)