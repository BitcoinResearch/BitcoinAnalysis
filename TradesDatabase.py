#THIS FILE IS CURRENTLY RUNNING ON THE VIRTUAL SERVER

__author__ = 'Sam'

import pymysql
emptyList = ['']
conn = pymysql.Connect(host='127.0.0.1', port=3306, user='root', passwd='bitcoin', db='bitcoin', autocommit=True)
cur = conn.cursor()
print("Connected to trades database")


def getMostRecentTimestamp():
    """Get most recent trade timestamp on all exchanges"""
    cur.execute("SELECT MAX(trade_timestamp) FROM trades")
    recentTimestamp = cur.fetchone()
    return recentTimestamp[0]

def getMostRecentExchangeTimestamp(exchange):
    """Get most recent trade timestamp for a specific exchange"""
    cur.execute("SELECT MAX(trade_timestamp) FROM trades "
                "WHERE trade_exchangeID='"+str(exchange)+"'")
    exchangeTimestamp = cur.fetchone()
    return exchangeTimestamp[0]

def getEarliestTimestamp(exchange):
    """Get the earliest recorded trade timestamp for a specific exchange"""
    cur.execute("SELECT MIN(trade_timestamp) FROM trades "
                "WHERE trade_exchangeID='"+str(exchange)+"'")
    earliestTimestamp = cur.fetchone()
    return earliestTimestamp[0]

def getExchangeCount():
    """Get a count of all the exchanges & currencies stored in the database"""
    cur.execute("SELECT COUNT(exchange_name) FROM exchanges")
    count = cur.fetchall()
    return count[0][0]

def getTradesRangeExchangeID(minimumTime, maximumTime, exchangeID):
    """Get trades between a timestamp range on a particular exchange"""
    cur.execute("SELECT * FROM trades "
                "WHERE trade_exchangeID='"+str(exchangeID)+"' "
                "AND trade_timestamp BETWEEN '"+str(minimumTime)+"' AND '"+str(maximumTime)+"'")

    exchangeRange = str(cur.fetchall())

    theseTrades = exchangeRange.strip('(')                          #cleaning up the returned data
    theseTrades = theseTrades.split('),')
    for element in range(len(theseTrades)):
        theseTrades[element] = theseTrades[element].strip(' (')
        theseTrades[element] = theseTrades[element].split(',')
        for newElement in range(len(theseTrades[element])):
            theseTrades[element][newElement] = theseTrades[element][newElement].strip(' ')
            theseTrades[element][newElement] = theseTrades[element][newElement].strip('))')
    if(theseTrades[-1] == emptyList):
        print("POPPING: ", theseTrades[-1])
        theseTrades.pop(-1)

    return theseTrades

def getExchangeName(exchangeID):
    """Provide an exchange ID and return the exchange's name"""
    cur.execute("SELECT exchange_name FROM exchanges "
                "WHERE exchange_id='"+str(exchangeID)+"' ")
    exchangeName = str(cur.fetchone())
    if exchangeName != None:
        exchangeName = exchangeName.strip("(',')")                  #cleaning up the returned data
    return exchangeName

def getExchangeID(exchangeName):
    """Provide an exchange name and return the exchange's ID"""
    cur.execute("SELECT exchange_id FROM exchanges "
                "WHERE exchange_name='"+exchangeName+"'")
    exchangeID = str(cur.fetchone())
    if exchangeID != None:
        exchangeID = exchangeID.strip("(',')")                  #cleaning up the returned data
    return exchangeID
