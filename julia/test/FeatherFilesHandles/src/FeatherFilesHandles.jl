module FeatherFilesHandles
import Pkg 

using Pkg
Pkg.add("DBInterface")
Pkg.add("Tables")
Pkg.add("DataFrames")
Pkg.add("Feather")

using MySQL, DBInterface, Tables, DataFrames, Profile, Feather

function retrievedCurvesFeatherCreation()
    selct = "select rh.rf_id ,rh.rf_dttm ,CAST(rh.rf_value as float) as rf_value, FLOOR(r.rf_node) as rf_node from nextGen.rf_hist rh, nextGen.rf r where rh.rf_id like 'ITA%' and r.rf_id  = rh.rf_id ;"
    con = DBInterface.connect(MySQL.Connection, "127.0.0.1", "nextgen", "nextgen")

    selectStmt = DBInterface.prepare(con,selct)
    #updStmt = DBInterface.prepare(con,update)
    curs = DBInterface.execute(selectStmt)
    retrieved_curves = DataFrame(curs)
    DBInterface.close!(selectStmt)
    #1,10 secondi
    Feather.write("../../retrieved_curves.feather", retrieved_curves)

    DBInterface.close!(con)
end

function positionsFeatherCreation()
    selct = "select p.position_id, p.iter_id, p.account_id, p.instrument_id, p.qty, CAST(p.ctv as float) as ctv, CAST(p.price as float) as price from nextGen.`position` p  where p.account_id  in (33,45);"
    con = DBInterface.connect(MySQL.Connection, "127.0.0.1", "nextgen", "nextgen")

    selectStmt = DBInterface.prepare(con,selct)
    #updStmt = DBInterface.prepare(con,update)
    curs = DBInterface.execute(selectStmt)
    retrieved_position = DataFrame(curs)
    DBInterface.close!(selectStmt)
    #1,10 secondi
    Feather.write("../../positions.feather", retrieved_position)

    DBInterface.close!(con)
end

function instrumentRfFeatherCreation()
    selct = "select ir.instrument_id, ir.rf_id, CAST(ir.bucket_flow as float) as bucket_flow from nextGen.instr_rf ir;"
    con = DBInterface.connect(MySQL.Connection, "127.0.0.1", "nextgen", "nextgen")

    selectStmt = DBInterface.prepare(con,selct)
    #updStmt = DBInterface.prepare(con,update)
    curs = DBInterface.execute(selectStmt)
    retrieved_position = DataFrame(curs)
    DBInterface.close!(selectStmt)
    #1,10 secondi
    Feather.write("../../instrumentRf.feather", retrieved_position)
    DBInterface.close!(con)
end

end # module
