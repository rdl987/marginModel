module margin_unit
using Pkg
Pkg.add("ArgParse")
Pkg.add("DataFrames")

using ArgParse, DataFrames, Statistics

export portfolioAggregation, expectedShortfall

"""
Merge accounts with risk factors and calculate flow countervalue per every group of risk factor and account

Parameters
----------
account_df:
	data frame containing positions 
instrRf_df:
	data frame containing risk factors 

Returns
-------
aggrAccountRf_df:
	data frame containing portfolio
"""
function portfolioAggregation(account_df,instrRf_df)
	dfPrint(account_df,"account_df")
	dfPrint(instrRf_df,"instrRf_df")
	#spread positions per risk factor
	accountRf_df = innerjoin(instrRf_df,account_df,on= :instrument_id)

	insertcols!(accountRf_df,10,:FLOW_CTV => accountRf_df.bucket_flow ./ 100 .* accountRf_df.ctv)
	
	#groupby rf_id and account_id
	aggrAccountRf_df = combine(groupby(accountRf_df, [:rf_id, :account_id], sort=false, skipmissing=false), names(accountRf_df, Not([:rf_id, :account_id,:instrument_id,:bucket_flow,:position_id, :iter_id,:qty,:ctv,:price])) .=> sum => :flow_ctv)

	return aggrAccountRf_df
end

"""
Merge accounts with risk factors and calculate flow countervalue per every group of risk factor and account

Parameters
----------
scenarios_df : DataFrame
	containing scenarios

aggrAccountRf_df : DataFrame
	containing portfolio

CLlist: Array{Float64}
	confidence levels list

Returns
-------
exp_short_df: DataFrame
"""
function expectedShortfall(scenarios_df, aggrAccountRf_df,CLlist)
	accountScenario_df = innerjoin(scenarios_df,aggrAccountRf_df, on = :rf_id)
	accountScenario_df = select(accountScenario_df,Not([:rf_value]))
	insertcols!(accountScenario_df,8, :rf_pnl => accountScenario_df.sn_value .* accountScenario_df.flow_ctv .- accountScenario_df.flow_ctv)

	accountScenario_df = combine(groupby(accountScenario_df, [:account_id,:hp, :rf_dttm], sort=false, skipmissing=false), names(accountScenario_df, ([:rf_pnl])) .=> sum => :account_pnl)

	gdf = groupby(accountScenario_df, [:account_id,:hp])
	exp_short_df = DataFrame()
	for g in gdf
		sortedG_df = DataFrame(g)
		sort!(sortedG_df,[:account_pnl],rev=true)

		numero_righe=size(sortedG_df,1)
				
		for c in CLlist
			c_val=convert(Int32,round((1 - c) * numero_righe,digits=0))
			tailSortedG_Df = last(sortedG_df,c_val)
			exp_short_row_df = combine(groupby(tailSortedG_Df, [:account_id,:hp], sort=false, skipmissing=false), names(tailSortedG_Df, :account_pnl) .=> mean => :exp_short, names(tailSortedG_Df, :account_pnl) .=> maximum => :exp_short_lim)
			insertcols!(exp_short_row_df,5,:conf => c)
			exp_short_row_df.exp_short = abs.(exp_short_row_df.exp_short)
			append!(exp_short_df,exp_short_row_df)
		end
	end
	return exp_short_df
end

end # module