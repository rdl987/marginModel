import Pkg
using Pkg

#include("FeatherFilesHandler.jl") 

Pkg.add("Test")
Pkg.add("Feather")
Pkg.add("DataFrames")
include("../src/margin_unit.jl") 
#include("../src/margin_unitCopy.jl")

using Test, Feather, DataFrames

positions_df = Feather.read("positions.feather")
instrumentRf_df = Feather.read("instrumentRf.feather")
scenarios_df = Feather.read("scenarios.feather")
aggrAccountRf_df = margin_unit.portfolioAggregation(positions_df,instrumentRf_df)

HP = 1
HP_STEP = 1
HP_CNT = 2
CONF = 0.90
CONF_STEP =0.01
CONF_CNT = 3

CLstart = Float64(CONF)
CLlist = [CLstart]

while size(CLlist,1) < CONF_CNT
    CL = CLstart + CONF_STEP
    append!(CLlist,CL)
    global CLstart = CL
end

HPstart = Int32(HP)
HPlist = [HPstart]

while size(HPlist,1) < HP_CNT
    HP = HPstart + HP_STEP
    append!(HPlist,HP)
    global HPstart = HP
end

shortfall_df = margin_unit.expectedShortfall(scenarios_df, aggrAccountRf_df,CLlist)
#scenario_df = Feather.read("scenarios.feather")
#margin_unit.expectedShortfall(scenario_df,portFolio,0.997)
#println(result)