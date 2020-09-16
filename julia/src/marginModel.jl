module marginModel
using Pkg
Pkg.add("DataFrames")

using Dates,Tables, DataFrames

function bondPaymentDates(evaluation_date, maturity, coupon_frequency, coupon)
    payment_dates = []
    # No ZCB
    if coupon !== 0
        rolling_date = maturity
        while rolling_date > evaluation_date
            append!(payment_dates,rolling_date)
            rolling_date = rolling_date-Dates.Month(coupon_frequency)
        end

        payment_dates = sort(payment_dates, reverse=false)
    else
        # ZCB
        append!(payment_dates,maturity)
    end
    return payment_dates
end

function bondPaymentFlows(evaluation_date, maturity, coupon_frequency, coupon, nominal)
    payments_MF = []
    # ZCB
    if coupon == 0 
        payments_MF = [nominal]
    else
        # NO ZCB
        payment_dates_fixed = []
        rolling_date_fixed = maturity
        while rolling_date_fixed > evaluation_date
            append!(payment_dates_fixed,rolling_date_fixed)
            rolling_date_fixed = rolling_date_fixed-Dates.Month(coupon_frequency)
        end

        payment_dates_fixed = sort(payment_dates_fixed, reverse=false)

        for date in payment_dates_fixed
            if date !== maturity
                # intermediate payment
                append!(payments_MF,coupon * nominal)
            else
                # final payment
                append!(payments_MF,coupon * nominal + nominal)
            end
        end
    end
    return payments_MF
end

function cashFlowMapping(evaluation_date, isin_list)

end

function createScenarios(holding_period, lookback_period, retrieved_curves)
    used_nodes = retrieved_curves["RF_NODE"].drop_duplicates().tolist()

    for node_str in used_nodes
        node = parse(Int64,node_str)

        subframe = retrieved_curves.loc[retrieved_curves["RF_NODE"] == node_str].copy()
        subframe.sort_values(by=["RF_DTTM"], ascending=false, inplace=true)
#prende gli ultimi 5 elementi.
        shifted_prices = subframe.["RF_VALUE"][holding_period:].tolist()
        subframe = subframe[:-holding_period]

        subframe["SH_CLOSEPR"] = shifted_prices
        
        subframe["RF_VALUE"] = parse(Float64,subframe["RF_VALUE"])
        subframe["RF_NODE"] = parse(Float64,subframe["RF_NODE"])
        subframe["SH_CLOSEPR"] = parse(Float64,subframe["SH_CLOSEPR"])

        if node < 1
            subframe["PRICE"] = 100 * numpy.exp(-subframe["RF_NODE"] * subframe["RF_VALUE"] / 100)
            subframe["SH_PRICE"] = 100 * numpy.exp(-subframe["RF_NODE"] * subframe["SH_CLOSEPR"] / 100)
        else
            subframe["PRICE"] = 100 / (1 + subframe["RF_VALUE"] / 100) ^ subframe["RF_NODE"]
            subframe["SH_PRICE"] = 100 / (1 + subframe["SH_CLOSEPR"] / 100) ^ subframe["RF_NODE"]
        end
        subframe["SN_VALUE"] = subframe["PRICE"] / subframe["SH_PRICE"]

        subframe = subframe[:lookback_period]
        subframe["HP"] = [holding_period] * length(subframe.index)
        
        scenarios = scenarios.append(subframe)
    end
    scenarios.reset_index(inplace=true, drop=true)
    return scenarios
end
end # module
