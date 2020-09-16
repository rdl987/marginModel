using Pkg

Pkg.add("Test")
Pkg.add("Feather")
Pkg.add("DataFrames")
include("../src/scenario_unit.jl") 

using Test, Feather, DataFrames

HP = 1
HP_STEP = 1
HP_CNT = 2
HPstart = Int32(HP)
HPlist = [HPstart]

while size(HPlist,1) < HP_CNT
    HP = HPstart + HP_STEP
    append!(HPlist,HP)
    global HPstart = HP
end

df = Feather.read("retrieved_curves.feather")
result = scenario_unit.createScenarios(HPlist,1000,df)
Feather.write("scenarios.feather",result)
println(result)