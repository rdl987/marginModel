#module FeatherFilesHandler
import Pkg 

using Pkg
export retrievedCurvesFeatherCreation, positionsFeatherCreation

Pkg.add("DBInterface")
Pkg.add("Tables")
Pkg.add("DataFrames")
Pkg.add("Feather")

using MySQL, DBInterface, Tables, DataFrames, Profile , Feather
#=function retrievedCurvesFeatherCreation()
    selct = "select rh.rf_id ,rh.rf_dttm ,CAST(rh.rf_value as float) as rf_value, FLOOR(r.rf_node) as rf_node from nextGen.rf_hist rh, nextGen.rf r where rh.rf_id = 'ITA10YZ' and r.rf_id  = rh.rf_id ;"
    con = DBInterface.connect(MySQL.Connection, "127.0.0.1", "nextgen", "nextgen")

    selectStmt = DBInterface.prepare(con,selct)
    #updStmt = DBInterface.prepare(con,update)
    curs = DBInterface.execute(selectStmt)
    retrieved_curves = DataFrame(curs)
    DBInterface.close!(selectStmt)
    #1,10 secondi
    Feather.write("retrieved_curves.feather_2", retrieved_curves)

    fs = Feather.read("retrieved_curves.feather_2")
    print(fs)
    DBInterface.close!(con)
end

function positionsFeatherCreation()=#
    selct = "select p.position_id, p.iter_id, p.account_id, p.instrument_id, p.qty, CAST(p.ctv as float) as ctv, CAST(p.price as float) as price from nextGen.`position` p  where p.account_id  = 33;"
    con = DBInterface.connect(MySQL.Connection, "127.0.0.1", "nextgen", "nextgen")

    selectStmt = DBInterface.prepare(con,selct)
    #updStmt = DBInterface.prepare(con,update)
    curs = DBInterface.execute(selectStmt)
    retrieved_position = DataFrame(curs)
    DBInterface.close!(selectStmt)
    #1,10 secondi
    Feather.write("positions.feather", retrieved_position)

    fs = Feather.read("positions.feather")
    print(fs)
    DBInterface.close!(con)
#= end
end # module
=#
