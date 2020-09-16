module scenario_unit
using Pkg
Pkg.add("ArgParse")
Pkg.add("DataFrames")

using ArgParse, DataFrames

export createScenarios

#=function importCountryCurves(node)
    node_hist = scenarioKV.getRfHist(company,node)
    node_hist_frame = DataFrame(node_hist.values())
    sort!(node_hist_frame,[:RF_ID, :RF_DTTM],rev = true)
    # node_hist_frame.reset_index(drop=True, inplace=True)
    dropmissing(node_hist_frame)
    

    return node_hist_frame
end=#

"""

Parameters
----------
hp_list : Array{Int32}
    holding periods list
    
lookback_period : Int32

curves_df : DataFrame
	containing curves

Returns
-------
scenarios_df : DataFrame
"""
function createScenarios(hp_list, lookback_period, curves_df)
    used_nodes = unique(curves_df.rf_node)
    scenarios = DataFrame()
    for hp in hp_list
        for node_str in used_nodes
            node = convert(Int64,node_str)

            #create dataframe for the specific node, order by date descending
            node_df = curves_df[curves_df.rf_node .== node, :]
            if size(node_df,1)<=hp
               error("not enought values for the node " * string(node_str))
            end
            sort!(node_df,[:rf_dttm],rev = true)

            #create shifted_prices vector
            shifted_prices = node_df[(hp+1):(size(node_df, 1)),:rf_value]
            
            #create dataframe adding shifted prices column
            filteredNode_df = first(node_df,length(shifted_prices))

            #create price and shifted price columns
            if node < 1
                price = 100 .* ((-filteredNode_df.rf_node) .* filteredNode_df.rf_value ./ 100)
                sh_price = 100 .* ((-filteredNode_df.rf_node) .* shifted_prices ./ 100)
            else
                price = 100 ./ (1 .+ (filteredNode_df.rf_value ./ 100)) .^ filteredNode_df.rf_node
                sh_price = 100 ./ (1 .+ shifted_prices ./ 100) .^ filteredNode_df.rf_node
            end

            #calculate price ratio
            insertcols!(filteredNode_df,5,:sn_value => price ./ sh_price)

            #get last holding period record
            filteredNode_df = first(filteredNode_df,lookback_period)

            #insert holding period column
            insertcols!(filteredNode_df,6,:hp => hp)
            
            append!(scenarios,filteredNode_df)
        end
    end

	return select(scenarios_df,Not([:rf_value]))
end

end # module