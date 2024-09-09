# Import the dependencies.
from flask import Flask, jsonify
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine, text, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
Base.prepare(autoload_with=engine)

# reflect the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
Session = sessionmaker(bind=engine)
session = Session()

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
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;"
    )

@app.route('/api/v1.0/precipitation')
def precipitation():
    # Calculate the date one year ago from the last date in the dataset
    last_date = session.query(func.max(Measurement.date)).scalar()
    first_date = dt.datetime.strptime(last_date, '%Y-%m-%d') - dt.timedelta(days=365)
    # Perform a query to retrieve the data and precipitation scores
    query = f"""
    SELECT station, date, tobs
    FROM measurement
    WHERE date BETWEEN '{first_date}' AND '{last_date}'
    """
    # Execute the query
    result = session.execute(text(query))
    # Fetch all the data
    data = result.fetchall()
    # Define the column names
    columns = ['station', 'date', 'tobs']
    # Create the Dataframe
    climate_df = pd.DataFrame(data, columns=columns)
    # Find the most active station
    stations = climate_df['station'].value_counts()
    active_station = stations.idxmax()
    # Filter the DataFrame for the most active station
    active_df = climate_df[climate_df['station'] == active_station]
    # Convert the DataFrame to a JSON object
    active_json = active_df.to_json(orient='records')
    # Close the session
    session.close()
    return jsonify(active_json)


@app.route('/api/v1.0/stations')
def stations():
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
def tobs():
    # Calculate the date one year ago from the last date in the dataset
    last_date = session.query(func.max(Measurement.date)).scalar()
    first_date = dt.datetime.strptime(last_date, '%Y-%m-%d') - dt.timedelta(days=365)
    # Query to retrieve the date and temperature observations (tobs) for the previous year
    query = f"""
    SELECT date, tobs
    FROM measurement
    WHERE date BETWEEN '{first_date}' AND '{last_date}'
    """
    # Execute the query
    result = session.execute(text(query))
    # Fetch all the data
    data = result.fetchall()
    # Define the column names
    columns = ['date', 'tobs']
    # Create the Dataframe
    tobs_df = pd.DataFrame(data, columns=columns)
    # Convert the DataFrame to a JSON object
    tobs_json = tobs_df.to_json(orient='records')
    # Close the session
    session.close()
    return jsonify(tobs_json)

@app.route('/api/v1.0/<start>')
def start_weather(start):
    start_date = start
    end_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    # Query to get the max, min, and average tobs per day
    results = session.query(
        Measurement.date,
        func.max(Measurement.tobs).label('max_tobs'),
        func.min(Measurement.tobs).label('min_tobs'),
        func.avg(Measurement.tobs).label('avg_tobs')
    ).filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).group_by(Measurement.date).all()
    # Convert the results to a DataFrame
    tobs_df = pd.DataFrame(results, columns=['date', 'max_tobs', 'min_tobs', 'avg_tobs'])
    # Convert the DataFrame to a JSON object
    tobs_json = tobs_df.to_json(orient='records')
    # Close the session
    session.close()
    return jsonify(tobs_json)

@app.route('/api/v1.0/<start>/<end>')
def start_stop_weather(start,end):
    start_date = start
    end_date = end

    # Query to get the max, min, and average tobs per day
    results = session.query(
        Measurement.date,
        func.max(Measurement.tobs).label('max_tobs'),
        func.min(Measurement.tobs).label('min_tobs'),
        func.avg(Measurement.tobs).label('avg_tobs')
    ).filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).group_by(Measurement.date).all()

    # Convert the results to a DataFrame
    tobs_df = pd.DataFrame(results, columns=['date', 'max_tobs', 'min_tobs', 'avg_tobs'])

    # Convert the DataFrame to a JSON object
    tobs_json = tobs_df.to_json()

    # session.close()
    return jsonify(tobs_json)

if __name__ == '__main__':
    app.run(debug=True)