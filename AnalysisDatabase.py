#THIS FILE IS CURRENTLY RUNNING ON THE VIRTUAL SERVER
#PURPOSE OF THIS FILE AnalysisDatabase.py: access the tables "trade_hour_analysis", "trade_minute_analysis" within the..
#                                        ..Bitcoin database. Functions for inputting analysis data and retrieving data.

__author__ = 'Sam'

import pymysql

conn = pymysql.Connect(host='127.0.0.1', port=3306, user='root', passwd='bitcoin', db='bitcoin', autocommit=True)
cur = conn.cursor()
print("Connected to analysis database")


def getLastHourTradeAnalysis(exchange_id):
    """Get the last hour's worth of analysis for a particular exchange"""
    cur.execute("SELECT * FROM trades_hour_analysis "
                "WHERE hour_id=(SELECT MAX(hour_id) FROM trades_hour_analysis "
                "WHERE exchange_id='"+str(exchange_id)+"')")

    lastHour = cur.fetchone()
    return lastHour

def getLastMinuteTradeAnalysis(exchange_id):
    """Get all data from last minute's worth of analysis for a particular exchange"""
    cur.execute("SELECT * FROM trades_minute_analysis "
                "WHERE minute_id=(SELECT MAX(minute_id) FROM trades_minute_analysis "
                "WHERE exchange_id='"+str(exchange_id)+"')")

    lastMinute = cur.fetchone()
    return lastMinute

def getLastAnalysedTimestampHour(exchange_id):
    """Get the timestamp from the last hour analysed for a particular exchange"""
    cur.execute("SELECT timestamp FROM trades_hour_analysis "
                "WHERE hour_id=(SELECT MAX(hour_id) FROM trades_hour_analysis "
                "WHERE exchange_id='"+str(exchange_id)+"')")
    lastTimestamp = cur.fetchone()
    print lastTimestamp
    if lastTimestamp != None:
        return lastTimestamp[0]
    else:
        return 0

def getLastAnalysedTimestampMinute(exchange_id):
    """Get the timestamp from the last minute analysed for a particular exchange"""
    cur.execute("SELECT timestamp FROM trades_minute_analysis "
                "WHERE minute_id=(SELECT MAX(minute_id) FROM trades_minute_analysis "
                "WHERE exchange_id='"+str(exchange_id)+"')")
    lastTimestamp = cur.fetchone()
    print lastTimestamp
    if lastTimestamp != None:
        return lastTimestamp[0]
    else:
        return 0

def storeMinuteTradeAnalysis(exchange_id, timestamp, average, sd, json):
    """Store a minute's worth of analysed trades in the database"""
    values = (exchange_id, timestamp, float(average), float(sd), str(json))

    cur.execute("INSERT INTO trades_minute_analysis(exchange_id, timestamp, average, standard_deviation, trade_json) "
                "VALUES (%s, %s, %s, %s, %s) ", values)

def storeHourTradeAnalysis(exchange_id, timestamp, average, sd, json):
    """Store an hour's worth of analysed trades in the database"""
    values = (exchange_id, timestamp, float(average), float(sd), str(json))

    cur.execute("INSERT INTO trades_hour_analysis(exchange_id, timestamp, average, standard_deviation, trade_json) "
                "VALUES (%s, %s, %s, %s, %s) ", values)

def getLastTimestampHour(exchangeID):
    """Alternative function for getting the most recently analysed timestamp for an hour"""
    cur.execute("SELECT MAX(timestamp) FROM trades_hour_analysis "
                "WHERE exchange_id='"+str(exchangeID)+"'")
    timestamp = cur.fetchone()
    return timestamp[0]

def getFirstTimestampHour(exchangeID):
    """Get the first timestamp ever analysed for an hour"""
    cur.execute("SELECT MIN(timestamp) FROM trades_hour_analysis "
                "WHERE exchange_id='"+str(exchangeID)+"'")
    timestamp = cur.fetchone()
    return timestamp[0]

def getLastTimestampMinute(exchangeID):
    """Alternative function for getting the most recently analysed timestamp for a minute"""
    cur.execute("SELECT MAX(timestamp) FROM trades_minute_analysis "
                "WHERE exchange_id='"+str(exchangeID)+"'")
    timestamp = cur.fetchone()
    return timestamp[0]

def getFirstTimestampMinute(exchangeID):
    """Get the first timestamp ever analysed for a minute"""
    cur.execute("SELECT MIN(timestamp) FROM trades_minute_analysis "
                "WHERE exchange_id='"+str(exchangeID)+"'")
    timestamp = cur.fetchone()
    return timestamp[0]