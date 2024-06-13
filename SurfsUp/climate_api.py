#
# Module 10 Challenge
#
# Climate API - A Python Flask SQLAlchemy Web Application
#
# Class: U of M Data Analytics and Visualization Boot Camp , Spring 2024
# Student: Mark Olson
# Professor: Thomas Bogue , Assisted by Jordan Tompkins
# Date: 06/13/2024
#

#################################################
# Import the dependencies
#################################################

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

# ----------
# NOTE: Import "request" from flask, to ensure that the request object is available
#       within the app.errorhandler "not_found" function,
#       allowing access to the request.path attribute, to display failed endpoint request
from flask import Flask, jsonify, request    

import pandas as pd

import datetime as dt
from dateutil.relativedelta import relativedelta

# ----------
# NOTE: Used to collect a list of values, associated to a single key
from collections import defaultdict
    
# ----------
# NOTE: To remedy the following error that I was constantly seeing in my flask debug console ...
#           " 127.0.0.1 - - [12/Jun/2024 14:54:19] "GET /favicon.ico HTTP/1.1" 404 - "
#       I used MS-Paint to create a 32x32 pixel icon file named "favicon.ico" ...
#       and saved it into a subfolder named "static", from the directory in which this file "climate_api.py" is located


#################################################
# Database Setup
#################################################

# create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# View all of the classes that automap found
Base.classes.keys()

# Save references to each table
station = Base.classes.station
measurement = Base.classes.measurement

# Create our session (link) from Python to the DB
session = Session(engine)


#################################################
# Functions
#################################################

def calculate_one_year_earlier(recent_date_string):
        
    # Convert the string date to a datetime object
    recent_date = dt.datetime.strptime(recent_date_string, '%Y-%m-%d')

    # Calculate the date one year earlier
    one_year_earlier_date = recent_date - relativedelta(years=1)

    # Convert the date back to a string
    one_year_earlier_date_string = one_year_earlier_date.strftime('%Y-%m-%d')

    return one_year_earlier_date_string
    
# ----------------------------------------

def find_one_year_prior_to_the_most_recent_measurement_data():

    # Find the most recent date in the measurement data set.
    most_recent_date_row = session.query(
        measurement.date
    ).order_by(
        measurement.date.desc()
    ).first()
    
    print(f"Most Recent Date in the Measurement Data Set: {most_recent_date_row.date}")
    
    # Calculate a date one year prior
    one_year_earlier_date_string = calculate_one_year_earlier(most_recent_date_row.date)

    print(f"One Year Earlier: {one_year_earlier_date_string}")

    return one_year_earlier_date_string

# ----------------------------------------

def find_most_active_station_id_in_measurement_data():

    # find the most active station (i.e. has the most rows)
    # sort stations and their counts in descending order
    # grab the first (most active) row
    station_activity_row = session.query(
        measurement.station, func.count(measurement.station)
    ).group_by(
        measurement.station
    ).order_by(
        func.count(measurement.station).desc()
    ).first()
    
    most_active_station_id = station_activity_row.station

    print(f"Most Active Station Id: {most_active_station_id}")

    return most_active_station_id

# ----------------------------------------

def validate_date_parameters(start_date_string, end_date_string=None):

    # Check if start_date_string is a valid date
    try:
        start_date = dt.datetime.strptime(start_date_string, '%Y-%m-%d')
        # print("Found Start Date")
    except ValueError:
        raise ValueError(f"start date {start_date_string} is not in the correct format YYYY-MM-DD")
    
    # Check if start_date is in the future
    if start_date > dt.datetime.today():
        raise ValueError(f"start date {start_date_string} cannot be in the future")

    if end_date_string is not None:
        # Check if end_date_string is a valid date
        try:
            end_date = dt.datetime.strptime(end_date_string, '%Y-%m-%d')
            # print("Found End Date")
        except ValueError:
            raise ValueError(f"end date {end_date_string} is not in the correct format YYYY-MM-DD")

        # Check if end_date is in the future
        if end_date > dt.datetime.today():
            raise ValueError(f"end date {end_date_string} cannot be in the future")

        # Check if start_date is prior to end_date
        if start_date > end_date:
            raise ValueError(f"start date {start_date_string} must not follow end date {end_date_string}")

        # print("Start Date and End Date are Valid")
        return True
        
    else:
        # print("Start Date is Valid")
        return True

# ----------------------------------------

def pull_min_max_avg_tobs_data(start_date, end_date=None):

    validate_date_parameters(start_date, end_date)
    
    # Start building the query
    query = session.query(
        func.min(measurement.tobs).label('min_tobs'),
        func.max(measurement.tobs).label('max_tobs'),
        func.avg(measurement.tobs).label('avg_tobs')
    ).filter(
        measurement.date >= start_date
    )
    
    # If end_date is provided, add it to the filter
    if end_date is not None:
        query = query.filter(
            measurement.date <= end_date
        )

    # Execute the query and get the result
    result = query.one()
    
    # Return the result as a dictionary
    return {
        'min_tobs': result.min_tobs,
        'max_tobs': result.max_tobs,
        'avg_tobs': result.avg_tobs
    }

# ----------------------------------------

def pull_tobs_data():
   
    most_active_station_id = find_most_active_station_id_in_measurement_data()

    one_year_earlier_date_string = find_one_year_prior_to_the_most_recent_measurement_data()

    tobs = session.query(
        measurement
    ).filter(
        measurement.station == most_active_station_id
    ).filter(
        measurement.date >= one_year_earlier_date_string
    ).all()

    # Display the type of the measurements variable
    print(f"tobs type: {type(tobs)}")
    # Display the number of results
    print(f"tobs results: {len(tobs)}")

    # Convert SQLAlchemy result to JSON
    tobs_data = [
        { 'id': measurement.id, 'station': measurement.station, 'tobs': measurement.tobs, 'prcp': measurement.prcp, 'date': measurement.date }
        for measurement in tobs
    ]

    # Display the type of the measurements variable
    print(f"tobs_data type: {type(tobs_data)}")
    # Display the number of results
    print(f"tobs_data results: {len(tobs_data)}")
    
    return tobs_data

# ----------------------------------------
    
def pull_stations_data():

    stations = session.query(
        station
    ).all()
    
    # Convert SQLAlchemy result to JSON
    stations_data = [
        { 'id': station.id, 'station': station.station, 'name': station.name, 'latitude': station.latitude, 'longitude': station.longitude, 'elevation': station.elevation }
        for station in stations
    ]

    # Display the type of the measurements variable
    print(f"stations_data type: {type(stations_data)}")
    # Display the number of results
    print(f"stations_data results: {len(stations_data)}")
    
    return stations_data

# ----------------------------------------

def pull_precipitation_data():
    
    one_year_earlier_date_string = find_one_year_prior_to_the_most_recent_measurement_data()
    
    # Perform a query to retrieve the data and precipitation scores
    measurements = session.query(
        measurement.prcp, measurement.date
    ).filter(
        measurement.date >= one_year_earlier_date_string
    ).all()

    # ----------
    # NOTE: measurements is a variable that stores a list object containing tuples.
    #       Each tuple represents a row from the database query result,
    #       where the first element is measurement.prcp and the second element is measurement.date.
    #
    # Display the type of the measurements variable
    print(f"measurements type: {type(measurements)}")
    # Display the number of results
    print(f"measurements results: {len(measurements)}")

    # Create a Pandas DataFrame from measurements
    df = pd.DataFrame(measurements, columns=['prcp', 'date'])

    # Display the type of the measurements variable
    print(f"df type: {type(df)}")
    # Display the number of results
    print(f"df results: {len(df)}")

    # Initialize a defaultdict with a list
    date_prcp_dict = defaultdict(list)
    
    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        date_prcp_dict[row['date']].append(row['prcp'])

    # Display the type of the measurements variable
    print(f"date_prcp_dict type: {type(date_prcp_dict)}")
    # Display the number of results
    print(f"date_prcp_dict results: {len(date_prcp_dict)}")

    # convert defaultdict to a regular dictionary
    date_prcp_dict = dict(date_prcp_dict)

    # Display the type of the measurements variable
    print(f"date_prcp_dict type: {type(date_prcp_dict)}")
    # Display the number of results
    print(f"date_prcp_dict results: {len(date_prcp_dict)}")

    return date_prcp_dict


#################################################
# Flask Setup
#################################################

# Create an app, being sure to pass __name__
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

# ----------
# NOTE: The key-value pair ordering of the json response, ...
#       as recieved by a web browser (or curl command) ...
#       may not appear in the same order as being written here


# Define the response to an invalid (undefined) route.
#
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error_message": "The requested resource was not found on the server.",
        "invalid_route": request.path,
        "error": {
            "code": error.code,
            "name": error.name,
            "description": error.description
        }
    }), 404

# ----------------------------------------

# Define the "/" main index route.
#
@app.route("/")
def home():
    return (
        f"Welcome to the Climate API!<br/>"
        f"<br/>"
        f"Available Routes:<br/>"
        f"-----------------------<br/>"
        f"<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"&nbsp;&nbsp;&nbsp;&nbsp;expectation: returns jsonified percipitation data for the last year in the database<br/>"
        f"&nbsp;&nbsp;&nbsp;&nbsp;return data: a dictionary of key-value pairs , where each key is a date , and each value is a list containing one or more precipitation readings that occured on that particular date<br/>"
        f"<br/>"
        f"/api/v1.0/stations<br/>"
        f"&nbsp;&nbsp;&nbsp;&nbsp;expectation: returns jsonified informational data for all the weather stations in the database<br/>"
        f"&nbsp;&nbsp;&nbsp;&nbsp;return data: a list of dictionaries , where each dictionary contains a number of key-value pairs which describe various aspects of an individual weather station<br/>"
        f"<br/>"
        f"/api/v1.0/tobs<br/>"
        f"&nbsp;&nbsp;&nbsp;&nbsp;expectation: returns jsonified weather observation data for the most active weather station for the last year in the database<br/>"
        f"&nbsp;&nbsp;&nbsp;&nbsp;return data: a list of dictionaries , where each dictionary contains a number of key-value pairs which describe various aspects of a particular weather observation<br/>"
        f"<br/>"
        f"/api/v1.0/&lt;start&gt;<br/>" 
        f"&nbsp;&nbsp;&nbsp;&nbsp;expectation: returns jsonified min, max, and average temperatures calculated from given start date to end of data in the dataset<br/>"
        f"&nbsp;&nbsp;&nbsp;&nbsp;return data: a dictionary of 3 key-value pairs ... where the 3 keys are min_tobs, max_tobs, avg_tobs ... and their values are the associated aggregate function results<br/>"
        f"&nbsp;&nbsp;&nbsp;&nbsp;parameter: expected start date format is yyyy-mm-dd ... an invalid start date will return an error response 400<br/>"
        f"<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;<br/>"
        f"&nbsp;&nbsp;&nbsp;&nbsp;expectation: returns jsonified min, max, and average temperatures calculated from given start date to given end date found within dataset<br/>"
        f"&nbsp;&nbsp;&nbsp;&nbsp;return data: a dictionary of 3 key-value pairs ... where the 3 keys are min_tobs, max_tobs, avg_tobs ... and their values are the associated aggregate function results<br/>"
        f"&nbsp;&nbsp;&nbsp;&nbsp;parameters: expected start and end date format is yyyy-mm-dd ... an invalid start or end date will return an error response 400<br/>"
        f"<br/>"
        f"note: invalid routes will return an error response 404<br/>"
)    

# ----------------------------------------

# Define the "/api/v1.0/precipitation" route
#
@app.route("/api/v1.0/precipitation")
def api_precipitation():
    pulled_data = pull_precipitation_data()

    # Display the type of the measurements variable
    print(f"pulled_data type: {type(pulled_data)}")
    # Display the number of results
    print(f"pulled_data results: {len(pulled_data)}")
    
    response_data = {
        "message": "Hello from /api/v1.0/precipitation",
        "data": pulled_data
    }
    return jsonify(response_data), 200

# ----------------------------------------

# Define the "/api/v1.0/stations" route
#
@app.route("/api/v1.0/stations")
def api_stations():
    pulled_data = pull_stations_data()
    
    # Display the type of the measurements variable
    print(f"pulled_data type: {type(pulled_data)}")
    # Display the number of results
    print(f"pulled_data results: {len(pulled_data)}")

    response_data = {
        "message": "Hello from /api/v1.0/stations",
        "data": pulled_data
    }
    return jsonify(response_data), 200

# ----------------------------------------

# Define the "/api/v1.0/tobs" route
#
@app.route("/api/v1.0/tobs")
def api_tobs():
    pulled_data = pull_tobs_data()

    # Display the type of the measurements variable
    print(f"pulled_data type: {type(pulled_data)}")
    # Display the number of results
    print(f"pulled_data results: {len(pulled_data)}")
    
    response_data = {
        "message": "Hello from /api/v1.0/tobs",
        "data": pulled_data
    }
    return jsonify(response_data), 200

# ----------------------------------------

# Define the parameterized "/api/v1.0/<start>" route
#
@app.route("/api/v1.0/<start>")
def api_start(start):

    try:
        pulled_data = pull_min_max_avg_tobs_data(start)

        # Display the type of the measurements variable
        print(f"pulled_data type: {type(pulled_data)}")
        # Display the number of results
        print(f"pulled_data results: {len(pulled_data)}")
        
        response_data = {
            "message": "Hello from /api/v1.0/<start>",
            "start": start,
            "data": pulled_data
        }
        return jsonify(response_data), 200

    except ValueError as e:
        response_data = {
            "message": "Hello from /api/v1.0/<start>",
            "start": start,
            "error": {
                # "code": e.args[0],       # Assuming ValueError carries an error code
                "code": 400,
                "name": type(e).__name__,  # Get the class name of the exception
                "description": str(e)      # Convert exception to string for description
            }
        }
        return jsonify(response_data), 400
        
# ----------------------------------------

# Define the parameterized "/api/v1.0/<start>/<end>" route
#
@app.route("/api/v1.0/<start>/<end>")
def api_start_end(start, end):

    try:
        pulled_data = pull_min_max_avg_tobs_data(start, end)

        # Display the type of the measurements variable
        print(f"pulled_data type: {type(pulled_data)}")
        # Display the number of results
        print(f"pulled_data results: {len(pulled_data)}")
    
        response_data = {
            "message": "Hello from /api/v1.0/<start>/<end>",
            "start": start,
            "end": end,
            "data": pulled_data
        }
        return jsonify(response_data), 200
    
    except ValueError as e:
        response_data = {
            "message": "Hello from /api/v1.0/<start>/<end>",
            "start": start,
            "end": end,
            "error": {
                # "code": e.args[0],       # Assuming ValueError carries an error code
                "code": 400,
                "name": type(e).__name__,  # Get the class name of the exception
                "description": str(e)      # Convert exception to string for description
            }
        }
        return jsonify(response_data), 400


#################################################


# This block ensures the Flask application runs only when the script is executed directly,
# not when it is imported as a module. It starts the Flask development server in debug mode,
# enabling automatic reloading and detailed error messages.
if __name__ == "__main__":
    app.run(debug=True)
