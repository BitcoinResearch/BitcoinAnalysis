#THIS FILE IS CURRENTLY RUNNING ON THE VIRTUAL SERVER
#PYTHON 2.7
#PACKAGES REQUIRED: datetime, calendar, numpy, pymysql
#PURPOSE OF THIS FILE Main.py: check each exchange to see if it has trade data to be analysed. Will check each minute..
#                              ..and each hour, then analyse new trades every minute, and every hour.

__author__ = 'Sam'

import Calculations as calc
import datetime
import calendar
import AnalysisDatabase as analysis_db
import TradesDatabase as trade_db

if __name__ == "__main__":

    while True:
        exchangeCount = trade_db.getExchangeCount() #how many exchanges in database? checking incase of new additions

        for thisExchange in range(1, exchangeCount): #for each and every exchange, do the following:
            print "thisExchange: ", thisExchange          #print the exchange number

            #from the database, retrieve the timestamp of the first trade every recorded for this exchange
            firstTradeTimestamp = trade_db.getEarliestTimestamp(thisExchange)

            #from the database, retrieve the newest timestamp for the most recently stored trade on this exchange
            newestTradeTimestamp = trade_db.getMostRecentExchangeTimestamp(thisExchange)
            #from the database, retrieve the most recently analysed minute as a timestamp for this exchange
            mostRecentAnalysisMinute = analysis_db.getLastTimestampMinute(thisExchange)  #(might be none)

            #Purpose of this if statement: check if we've ever analysed this exchange before, if not, analyse every..
            #..minute starting from the very trade we have stored for this exchange.
            #if there are no analysed minutes, but there is trade data for this exchange, enter this if statement
            if mostRecentAnalysisMinute == None and newestTradeTimestamp != None and firstTradeTimestamp != None:
                print ">if1 first time minutes"

                #using the firstTradeTimestamp for this exchange, and converting it to a readable date format
                roundThisDate = datetime.datetime.utcfromtimestamp(firstTradeTimestamp)
                #we now have the readable date format for the timestamp, we're going to extract the year, month, day..
                #..hour, and minute from the date and save it in this array below. Notice we're not extracting the..
                #..second, instead we're entering a 0. This is so the date/time is rounded down to the nearest minute
                roundToMinuteArray = [roundThisDate.year, roundThisDate.month, roundThisDate.day,
                                      roundThisDate.hour, roundThisDate.minute, 0]
                #we're taking this newly rounded readable date format, and converting it back into a timestamp
                roundedToMinute = calendar.timegm(roundToMinuteArray)

                #while there's more than 60 seconds of new trade data to analyse, enter the while loop:
                while (newestTradeTimestamp - roundedToMinute) >= 60:
                    print "(newestTradeTimestamp - roundedToMinute): " + str(newestTradeTimestamp - roundedToMinute) + ", " + str(newestTradeTimestamp) + " - " + str(roundedToMinute) + ""
                    #look in Calculatons.py for this function. Passing it the roundedToMinute timestamp, and exchangeID
                    calc.getMinuteStandardDeviations(roundedToMinute, thisExchange)
                    #add another 60 seconds, causes the function to compute the next 60 seconds of trades assuming..
                    #..we don't drop out of the while loop.
                    roundedToMinute = roundedToMinute + 60

            #Purpose of this else if statement: if the first if statement fails, it's because we previously analysed..
            #..minutes exist in the database. This else if statement will continue from the last analysed minute.
            #if previously analysed minutes exist, and 60 seconds or more of unanalysed trades exist, enter if statement
            elif newestTradeTimestamp != None and (newestTradeTimestamp - mostRecentAnalysisMinute) >= 60:
                print ">if2 checking for new minute"
                while (newestTradeTimestamp - mostRecentAnalysisMinute) >= 60:
                    print "(newestTradeTimestamp - mostRecentAnalysisMinute): " + str(newestTradeTimestamp - mostRecentAnalysisMinute) + ", " + str(newestTradeTimestamp) + " - " + str(mostRecentAnalysisMinute) + ""
                    #same Calculatons.py function as above. Passing mostRecentAnalysisMinute timestamp, and exchangeID
                    calc.getMinuteStandardDeviations(mostRecentAnalysisMinute, thisExchange)
                    #add another 60 seconds, will fall out of the while loop when there is less than 60 seconds..
                    #..worth of new trades to analyse
                    mostRecentAnalysisMinute = mostRecentAnalysisMinute + 60


            #Once minutes have been computed, the hourly analysis will be computed below.
            #Completely the same code and conditions, but uses 3600 seconds (1 hour) instead of 60 seconds seen above.
            #Also sends timestamps to calc.getHourStandardDeviations() instead of calc.getMinuteStandardDeviations()
            newestTradeTimestamp = trade_db.getMostRecentExchangeTimestamp(thisExchange)
            mostRecentAnalysisHour = analysis_db.getLastTimestampHour(thisExchange)

            if mostRecentAnalysisHour == None and newestTradeTimestamp != None and firstTradeTimestamp != None:
                print ">if1 first time hours"

                roundThisDate = datetime.datetime.utcfromtimestamp(firstTradeTimestamp)
                roundToHourArray = [roundThisDate.year, roundThisDate.month, roundThisDate.day,
                                    roundThisDate.hour, 0, 0]
                roundedToHour = calendar.timegm(roundToHourArray)
                while (newestTradeTimestamp - roundedToHour) >= 3600:
                    print "(newestTradeTimestamp - roundedToHour): " + str(newestTradeTimestamp - roundedToHour) + ", " + str(newestTradeTimestamp) + " - " + str(roundedToHour) + ""
                    calc.getHourStandardDeviations(roundedToHour, thisExchange)
                    roundedToHour = roundedToHour + 3600

            elif newestTradeTimestamp != None and (newestTradeTimestamp - mostRecentAnalysisHour) >= 3600:
                print ">if2 checking for new hour"
                print "(newestTradeTimestamp - mostRecentAnalysisHour): " + str(newestTradeTimestamp - mostRecentAnalysisHour) + ", " + str(newestTradeTimestamp) + " - " + str(mostRecentAnalysisHour) + ""
                while (newestTradeTimestamp - mostRecentAnalysisHour) >= 3600:
                    print "(newestTradeTimestamp - mostRecentAnalysisHour): " + str(newestTradeTimestamp - mostRecentAnalysisHour) + ", " + str(newestTradeTimestamp) + " - " + str(mostRecentAnalysisHour) + ""
                    calc.getHourStandardDeviations(mostRecentAnalysisHour, thisExchange)
                    mostRecentAnalysisHour = mostRecentAnalysisHour + 3600

