# Import the dependencies.
import datetime as dt
import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify

#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
conn = engine.connect()
session = Session(bind=engine)

#################################################
# Flask Setup
#################################################

app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    """List all available routes."""
    return (
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end><br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():

    """Return the precipitation data for the last year"""
    # Calculate the date 1 year ago from the last data point in the database
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first().date
    # Convert the string date to a datetime object
    last_date = dt.datetime.strptime(last_date, '%Y-%m-%d')
    Year_precipitaion = last_date - dt.timedelta(days=365)
    
    # Query for the last 12 months of precipitation data
    results = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= Year_precipitaion).all()
    
    # Create a dictionary from the row data and append to a list of all_precipitation
    all_precipitation = {date: prcp for date, prcp in results}
    
    return jsonify(all_precipitation)

@app.route("/api/v1.0/stations")
def stations():
    """Return a list of stations."""
    results = session.query(Measurement.station).distinct().all()
    stations = list(np.ravel(results))
    return jsonify(stations=stations)

@app.route("/api/v1.0/tobs")
def temp_monthly():
    """Return the temperature observations (tobs) for previous year."""
    # Calculate the date 1 year ago from last date in database
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first().date
    # Convert the string date to a datetime object
    last_date = dt.datetime.strptime(last_date, '%Y-%m-%d')
    Year_temp = last_date - dt.timedelta(days=365)

    # Query the primary station for all tobs from the last year
    most_active_station = session.query(Measurement.station, func.count(Measurement.station)).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).first()
    most_active_station_id = most_active_station[0]
    results = session.query(Measurement.tobs).\
        filter(Measurement.station == most_active_station_id).\
        filter(Measurement.date >= Year_temp).all()

    # Unravel results into a 1D array and convert to a list
    temps = list(np.ravel(results))

    # Return the results
    return jsonify(temps=temps)

@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def temp_stats(start, end=None):
    """Return TMIN, TAVG, and TMAX for a date range."""
    
    # Convert date strings to datetime objects
    try:
        start_date = dt.datetime.strptime(start, "%m-%d-%Y")
        end_date = dt.datetime.strptime(end, "%m-%d-%Y") if end else None
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use MM-DD-YYYY."}), 400

    # Select statement
    sel = [func.min(Measurement.tobs), 
           func.avg(Measurement.tobs), 
           func.max(Measurement.tobs)]
    
    if end_date is None:
        # For a specified start date, calculate TMIN, TAVG, and TMAX 
        # for all dates greater than and equal to the start date
        results = session.query(*sel).\
            filter(Measurement.date >= start_date.strftime("%Y-%m-%d")).all()
    else:
        # For a specified start date and end date, calculate the TMIN, TAVG, and TMAX 
        # for dates between the start and end date inclusive
        results = session.query(*sel).\
            filter(Measurement.date >= start_date.strftime("%Y-%m-%d")).\
            filter(Measurement.date <= end_date.strftime("%Y-%m-%d")).all()
    
    # Check if we have results
    if not results or not results[0]:
        return jsonify({"error": "No data found for the given date range."}), 404

    # Create a dictionary from the row data and append to a list of all_temps
    temps = list(results[0])
    
    # Create a dictionary to hold the results
    temp_stats = {
        "TMIN": temps[0],
        "TAVG": round(temps[1], 2) if temps[1] is not None else None,
        "TMAX": temps[2]
    }
    
    return jsonify(temp_stats)

if __name__ == '__main__':
    app.run(debug=True)