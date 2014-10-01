#THIS FILE IS CURRENTLY RUNNING ON THE VIRTUAL SERVER
#PURPOSE OF THIS FILE Calculations.py: called by Main.py, two functions, one for calculating the per-minute..
#                                    ..standard deviations, and another for calculating the per-hour..
#                                   ..standard deviations. Once calculated, the result is stored in one of the...
#                                  ..following tables "trades_hour_analysis", "trades_minute_analysis" in the...
#                                 ..Bitcoin database.
#
#E.g.: fields from 'trades_minute_analysis': minute_id, exchange_id, timestamp, average, standard_deviation, trade_json
#E.g.:    row from 'trades_minute_analysis':      2335,           1, 1409841900,  488.1,  0.599999999999994, {1409841900: ([488.7, 487.5], [5, 41])}
#'trade_json' stores the timestamp, then the prices of the trades [488.7, 487.5] that occured during the 60 seconds from that timestamp and..
#..then the seconds from 0-60 that those trades occured [5, 41], this allows for easy graphing if we ever want to graph each trade.
#'trades_hour_analysis' is the same, with 'hour_id' replacing 'minute_id'.

__author__ = 'Sam'

import TradesDatabase as trade_db
import AnalysisDatabase as analysis_db
import numpy as np

emptyList = ['']
emptyList2 = [['']]
noneList = []

#Calculating the standard deviations of trade prices found between a single minute
#Parameters: timestamp of the minute to analyse, and the exchange to analyse it on
def getMinuteStandardDeviations(minuteTimestamp, exchange):

    #from the database, retrieve the last hour we just analysed
    lastTimestamp = analysis_db.getLastAnalysedTimestampMinute(exchange)
    #if this hour is different from the last hour, enter if statement: (prevents duplicate data)
    if lastTimestamp != minuteTimestamp:

        NO_DATA_PRICES = [-1]
        NO_DATA_TIMES = [-1]
        tradesSinceTimestamp = []
        tradesSincePrice = []
        tradesSinceSecond = []
        tradeJson = {}

        #get all trades from the provided exchange id, between the following timestamps
        tradesSince = trade_db.getTradesRangeExchangeID(minuteTimestamp, minuteTimestamp+60, exchange)
        #tradesSince is now a list/array of trade data.

        #if the trade data we received for this minute wasn't empty, e.g.: [['']] or [''] or [], enter if statement:
        if (tradesSince != emptyList and tradesSince != emptyList2 and tradesSince != noneList):
            #for each element between 0 and the length of our tradesSince array:
            for element in range(len(tradesSince)):
                #retrieve each trade timestamp from the tradesSince array and store it in a new array of all timestamps
                tradesSinceTimestamp.append(tradesSince[element][2])
                #retrieve each trade price from the tradesSince array and store it in a new array of all prices
                tradesSincePrice.append(float(tradesSince[element][3]))

            average = np.average(tradesSincePrice)  #calculate the average of all prices
            priceArray = np.array(tradesSincePrice) #convert prices into a 'Numpy' compatible array
            standard_dev = np.std(priceArray)       #calculate the standard deviation of all prices

            #for each timestamp between 0 and the length of our array of all timestamps:
            for time in range(len(tradesSinceTimestamp)):
                #calculate how many seconds into the minute the trade occured
                thisTime = int(tradesSinceTimestamp[time]) - minuteTimestamp
                #store all the 0-60 second times in a single array 'tradesSinceSecond'
                tradesSinceSecond.append(thisTime)

            #create a Python dictionary (same as Json) with the 'minuteTimestamp' as the key, followed by..
            #the array of all prices, and the array of all 0-60 seconds that those trades occured
            tradeJson[minuteTimestamp] = tradesSincePrice, tradesSinceSecond

            #send data to the database. Exchange ID, minuteTimestamp, average, standard deviation, and tradeJson
            analysis_db.storeMinuteTradeAnalysis(exchange, minuteTimestamp, average, standard_dev, tradeJson)

        #else the trade data we received for this minute WAS empty:
        else:
            exchangeName = trade_db.getExchangeName(exchange)
            print("EXCHANGE: " + exchangeName + " NO DATA") #print out the exchange that has no data for this hour.

            #create a tradeJson which includes the timestamp, but stores [-1] instead of prices, and [-1] instead of seconds
            tradeJson[minuteTimestamp] = NO_DATA_PRICES, NO_DATA_TIMES
            #Note: because this timestamp has no trades we can't ignore it, we still need to have a record of this..
            #..timestamp in our database or we won't be able to do per-minute analysis of several exchanges at once..
            #..but by adding -1's we can go through our data in the future and detect whenever a timestamp had no trades

            #from the database, retrieve all data from the previously analysed minute
            lastMinute = analysis_db.getLastMinuteTradeAnalysis(exchange)
            #if we successfully retrieved data, enter the if statement:
            if lastMinute != None or lastMinute != ['']:
                #get the average of the previously analysed minute
                minuteAverage = lastMinute[3]

                #send data to the database. We're using the average of the previous minute instead of just 0 so that..
                #..when we graph this data the scale doesn't mess up. We're sending 0 for the standard deviation so...
                #..that no volatility is shown for this particular minute, and we're sending our tradeJson with -1's
                analysis_db.storeMinuteTradeAnalysis(exchange, minuteTimestamp, minuteAverage, 0, tradeJson)
            else:
                print "No analysis for "+exchangeName+", lastMinute: ",lastMinute
    else:
        print "Timestamp: "+str(minuteTimestamp)+" already analysed for exchange: ", exchange

#Identical as the function above, but uses 3600 seconds (1 hour) instead of 60 seconds
#Calculating the standard deviations of trade prices found between a single hour
#Parameters: timestamp of the minute to analyse, and the exchange to analyse it on
def getHourStandardDeviations(hourTimestamp, exchange):

    lastTimestamp = analysis_db.getLastAnalysedTimestampHour(exchange)
    if lastTimestamp != hourTimestamp:

        NO_DATA_PRICES = [-1]
        NO_DATA_TIMES = [-1]
        tradesSinceTimestamp = []
        tradesSincePrice = []
        tradesSinceSecond = []
        tradeJson = {}

        tradesSince = trade_db.getTradesRangeExchangeID(hourTimestamp, hourTimestamp+3600, exchange)

        if (tradesSince != emptyList and tradesSince != emptyList2 and tradesSince != noneList):
            for element in range(len(tradesSince)):
                tradesSinceTimestamp.append(tradesSince[element][2])
                tradesSincePrice.append(float(tradesSince[element][3]))

            average = np.average(tradesSincePrice)
            priceArray = np.array(tradesSincePrice)
            standard_dev = np.std(priceArray)

            for time in range(len(tradesSinceTimestamp)):
                thisTime = int(tradesSinceTimestamp[time]) - hourTimestamp
                tradesSinceSecond.append(thisTime)

            tradeJson[hourTimestamp] = tradesSincePrice, tradesSinceSecond
            analysis_db.storeHourTradeAnalysis(exchange, hourTimestamp, average, standard_dev, tradeJson)

        else:
            exchangeName = trade_db.getExchangeName(exchange)
            print("EXCHANGE: " + exchangeName + " NO DATA")
            tradeJson[hourTimestamp] = NO_DATA_PRICES, NO_DATA_TIMES

            lastHour = analysis_db.getLastHourTradeAnalysis(exchange)
            if lastHour != None or lastHour != ['']:
                hourAverage = lastHour[3]
                analysis_db.storeHourTradeAnalysis(exchange, hourTimestamp, hourAverage, 0, tradeJson)
            else:
                print "No analysis for "+exchangeName+", lastHour: ",lastHour
    else:
        print "Timestamp: "+str(hourTimestamp)+" already analysed for exchange: ", exchange





