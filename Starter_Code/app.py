# Import the dependencies.
from matplotlib import style
style.use('fivethirtyeight')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import datetime as dt
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func,  inspect, text
from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
Base.prepare(autoload_with=engine)

# reflect the tables
Measurment = Base.classes.measurement
Station = Base.classes.station

# Save references to each table
Measurment = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Setup our last observed date
    last_date = session.query(Measurment.date).order_by(Measurment.date.desc()).first()[0]
    last_date = dt.datetime.strptime(last_date, '%Y-%m-%d')

    #Get the start date
    first_date = last_date - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    query = f"""
    SELECT date, prcp
    FROM measurement
    WHERE date BETWEEN '{first_date}' AND '{last_date}
    """

    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    result = session.execute(text(query))
    
    # Fetch all the data
    data = result.fetchall()
    
    # Define the column names
    columns = ['date', 'prcp']
        
    # Create the Dataframe
    climate_df = pd.DataFrame(data, columns=columns)

    # Convert the DataFrame to a JSON object
    climate_json = climate_df.to_json()

    return jsonify(climate_json)

@app.route('/api/v1.0/stations')
def stations():
        # Setup our last observed date
    last_date = session.query(Measurment.date).order_by(Measurment.date.desc()).first()[0]
    last_date = dt.datetime.strptime(last_date, '%Y-%m-%d')

    #Get the start date
    first_date = last_date - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    query = f"""
    SELECT station, name
    FROM station
    """

    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    result = session.execute(text(query))
    
    # Fetch all the data
    data = result.fetchall()
    
    # Define the column names
    columns = ['station', 'name']
        
    # Create the Dataframe
    station_df = pd.DataFrame(data, columns=columns)

    # Convert the DataFrame to a JSON object
    station_json = station_df.to_json()

    return jsonify(station_json)

@app.route('/api/v1.0/tobs')
def temperature():
    # Setup our last observed date
    last_date = session.query(Measurment.date).order_by(Measurment.date.desc()).first()[0]
    last_date = dt.datetime.strptime(last_date, '%Y-%m-%d')

    # From earlier analysis the most active station is 

    #Get the start date
    first_date = last_date - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    query = f"""
    SELECT station, date, tobs
    FROM measurement
    WHERE date BETWEEN '{first_date}' AND '{last_date}
    """

    # Save the query results as a Pandas DataFrame. Explicitly set the column names
    result = session.execute(text(query))
    
    # Fetch all the data
    data = result.fetchall()
    
    # Define the column names
    columns = ['station','date', 'prcp']
        
    # Create the Dataframe
    climate_df = pd.DataFrame(data, columns=columns)

    # Design a query to calculate the total number of stations in the dataset
    stations = climate_df['station'].value_counts()

    # Design a query to find the most active stations (i.e. which stations have the most rows?)
    # List the stations and their counts in descending order.
    active_station = stations.sort_values(ascending=False)[0]

    active_df = climate_df[climate_df['station']==active_station]

    # Convert the DataFrame to a JSON object
    active_json = active_df.to_json()

    return jsonify(active_json)

@app.route('/api/v1.0/<start>')
def start_weather(start):
    start_date = start
    end_date = session.query(Measurment.date).order_by(Measurment.date).first()[0]
    
    # Query to get the max, min, and average tobs per day
    results = session.query(
        Measurment.date,
        func.max(Measurment.tobs).label('max_tobs'),
        func.min(Measurment.tobs).label('min_tobs'),
        func.avg(Measurment.tobs).label('avg_tobs')
    ).filter(Measurment.date >= start_date).filter(Measurment.date <= end_date).group_by(Measurment.date).all()

    # Convert the results to a DataFrame
    tobs_df = pd.DataFrame(results, columns=['date', 'max_tobs', 'min_tobs', 'avg_tobs'])

    # Convert the DataFrame to a JSON object
    tobs_json = tobs_df.to_json()

    return jsonify(tobs_json)

@app.route('/api/v1.0/<start>/<end>')
def start_stop_weather(start,end):
    start_date = start
    end_date = end

    # Query to get the max, min, and average tobs per day
    results = session.query(
        Measurment.date,
        func.max(Measurment.tobs).label('max_tobs'),
        func.min(Measurment.tobs).label('min_tobs'),
        func.avg(Measurment.tobs).label('avg_tobs')
    ).filter(Measurment.date >= start_date).filter(Measurment.date <= end_date).group_by(Measurment.date).all()

    # Convert the results to a DataFrame
    tobs_df = pd.DataFrame(results, columns=['date', 'max_tobs', 'min_tobs', 'avg_tobs'])

    # Convert the DataFrame to a JSON object
    tobs_json = tobs_df.to_json()

    return jsonify(tobs_json)

if __name__ == '__main__':
    app.run(debug=True)