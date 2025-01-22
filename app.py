import pandas as pd
from flask import Flask, jsonify
import sqlite3
from sql_helper import SQLHelper

app = Flask(__name__)

def query_db(query, args=()):
    conn = sqlite3.connect("hawaii.sqlite")
    cur = conn.cursor()
    result = cur.execute(query, args).fetchall()
    conn.close()
    return result

@app.route("/")
def welcome():
    return (
        "Available Routes:<br/>"
        "/api/v1.0/precipitation<br/>"
        "/api/v1.0/stations<br/>"
        "/api/v1.0/tobs<br/>"
        "/api/v1.0/<start><br/>"
        "/api/v1.0/<start>/<end>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    query = """
        SELECT date, prcp
        FROM measurement
        WHERE date >= (SELECT DATE(MAX(date), '-1 year') FROM measurement)
    """
    results = query_db(query)
    return jsonify({date: prcp for date, prcp in results})

@app.route("/api/v1.0/stations")
def stations():
    results = query_db("SELECT station FROM station")
    return jsonify([station[0] for station in results])

@app.route("/api/v1.0/tobs")
def tobs():
    query = """
        SELECT date, tobs
        FROM measurement
        WHERE station = (
            SELECT station
            FROM measurement
            GROUP BY station
            ORDER BY COUNT(*) DESC
            LIMIT 1
        )
        AND date >= (SELECT DATE(MAX(date), '-1 year') FROM measurement)
    """
    results = query_db(query)
    return jsonify([{"date": date, "tobs": tobs} for date, tobs in results])

@app.route("/api/v1.0/<start>")
def stats_start(start):
    query = "SELECT MIN(tobs), AVG(tobs), MAX(tobs) FROM measurement WHERE date >= ?"
    results = query_db(query, (start,))[0]
    return jsonify({"TMIN": results[0], "TAVG": results[1], "TMAX": results[2]})

@app.route("/api/v1.0/<start>/<end>")
def stats_start_end(start, end):
    query = "SELECT MIN(tobs), AVG(tobs), MAX(tobs) FROM measurement WHERE date BETWEEN ? AND ?"
    results = query_db(query, (start, end))[0]
    return jsonify({"TMIN": results[0], "TAVG": results[1], "TMAX": results[2]})

if __name__ == '__main__':
    app.run(debug=True)
