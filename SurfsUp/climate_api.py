
# Import the dependencies.

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func


# ----------
# importing request from flask, you ensure that the request object is available
# within the not_found error handler function,
# allowing you to access the request.path attribute
# ----------
#from flask import Flask, jsonify
from flask import Flask, jsonify, request    


# ----------
# NOTE: To remedy the following error that I was constantly seeing in my flask debug console ...
#           " 127.0.0.1 - - [12/Jun/2024 14:54:19] "GET /favicon.ico HTTP/1.1" 404 - "
#       ... I used MS-Paint to create a 32x32 pixel icon file named "favicon.ico" ...
#       ... and saved it into a subfolder named "static", from the directory in which this file "climate_api.py" is located

import pandas as pd
import datetime as dt

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

from dateutil.relativedelta import relativedelta

def calculate_one_year_earlier(most_recent_date_string):
        
    # Convert the string date to a datetime object
    most_recent_date = dt.datetime.strptime(most_recent_date_string, '%Y-%m-%d')

    # Calculate the date one year earlier
    one_year_earlier_date = most_recent_date - relativedelta(years=1)

    # Convert the date back to a string
    one_year_earlier_date_string = one_year_earlier_date.strftime('%Y-%m-%d')

    return one_year_earlier_date_string
    
#################################################

def find_one_year_prior_to_most_recent_measurement_data():

    # Find the most recent date in the measuremnet data set.
    most_recent_date_row = session.query(
        measurement.date
    ).order_by(
        measurement.date.desc()
    ).first()
    
    print(f"The most recent date in the measurement data set: {most_recent_date_row.date}")
    
    # Calculate a date one year prior
    one_year_earlier_date_string = calculate_one_year_earlier(most_recent_date_row.date)

    print(f"One year earlier: {one_year_earlier_date_string}")

    return one_year_earlier_date_string

#################################################

def pull_min_max_avg_tobs_data(start_date, end_date=None):

    if end_date is not None:
        print("Found End Date")
    else:
        print("No End Date")

    return ("")
    
#################################################

def pull_tobs_data():

    # Design a query to find the most active stations (i.e. which stations have the most rows?)
    # List the stations and their counts in descending order.
    station_activity = session.query(
        measurement.station, func.count(measurement.station)
    ).group_by(
        measurement.station
    ).order_by(
        func.count(measurement.station).desc()
    ).all()
    
    print("Station and Activity Counts (by Descending Activity Order):\n{station_activity}")

    # Using the most active station id from the previous query, calculate the lowest, highest, and average temperature.
    most_active_station_id = station_activity[0][0]

    print(f"Most Active Station: {most_active_station_id}")
    

    one_year_earlier_date_string = find_one_year_prior_to_most_recent_measurement_data()

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

#################################################
    
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

#################################################

def pull_precipitation_data():
    
    # # Find the most recent date in the data set.
    # most_recent_date_row = session.query(
    #     measurement.date
    # ).order_by(
    #     measurement.date.desc()
    # ).first()
    
    # print(f"The most recent date in the data set: {most_recent_date_row.date}")
    
    # # Calculate the date one year from the last date in data set.
    # one_year_earlier_date_string = calculate_one_year_earlier(most_recent_date_row.date)

    one_year_earlier_date_string = find_one_year_prior_to_most_recent_measurement_data()

    
    # Perform a query to retrieve the data and precipitation scores
    measurements = session.query(
        measurement.prcp, measurement.date
    ).filter(
        measurement.date >= one_year_earlier_date_string
    ).all()


    # Display the type of the measurements variable
    print(f"measurements type: {type(measurements)}")
    #
    # measurements is a variable that stores a list object containing tuples.
    # Each tuple represents a row from the database query result,
    # where the first element is measurement.prcp and the second element is measurement.date.

    # Display the number of results
    print(f"measurements results: {len(measurements)}")


    # # Save the query results as a Pandas DataFrame. Explicitly set the column names
    # df = pd.DataFrame(measurements, columns=['Precipitation','Date'])
    
    # # Sort the dataframe by date
    # df_sorted = df.sort_values(by='Date', ascending=True)



    # # # Example measurements list (replace this with your actual list)
    # # measurements = [
    # #     (0.08, '2016-08-23'),
    # #     (0.02, '2016-08-24'),
    # #     (None, '2016-08-25'),  # Example with None value for precipitation
    # #     (0.05, '2016-08-26'),
    # #     (0.0, '2016-08-27')
    # # ]
    
    # # Create an empty dictionary to store the JSON structure
    # measurements_json = {}
    
    # # Iterate through each tuple in measurements
    # for prcp, date in measurements:
    #     # Skip tuples where prcp is None (or handle accordingly)
    #     if prcp is None:
    #         continue
    #     # Assign the date as key and precipitation as value in the dictionary
    #     measurements_json[date] = prcp
    
    # # Convert the dictionary to JSON format
    # measurements_json_str = json.dumps(measurements_json, indent=4)
    
    # # Print the JSON string
    # print(measurements_json_str)


    # Create a Pandas DataFrame from measurements
    df = pd.DataFrame(measurements, columns=['prcp', 'date'])

    # Display the type of the measurements variable
    print(f"df type: {type(df)}")
    # Display the number of results
    print(f"df results: {len(df)}")




    
    # ERR ----------------------------------------
    # # # Drop rows with NaN values (if any)
    # # df = df.dropna()
    
    # # # Convert DataFrame to JSON format (orient='index' to use 'date' as index/key)
    # # measurements_json = df.set_index('date')['prcp'].to_json(orient='index', indent=4)
    # #
    # # above gave runtime error ... "ValueError: Series index must be unique for orient='index'"
    # #
    # # Convert DataFrame to JSON format (orient='index' to use 'date' as index/key)
    # measurements_json = df.set_index('date')['prcp'].to_json(indent=4)
    
    # # Print the JSON string
    # print(measurements_json)

    # return measurements_json
    # ERR ----------------------------------------


    # ERR / ONLY LAST VALUE FOUND PER KEY IS RETAINED ----------------------------------------
    # # Set 'date' column as index and convert to dictionary
    # date_prcp_dict = df.set_index('date')['prcp'].to_dict()

    # # Display the type of the measurements variable
    # print(f"date_prcp_dict: {type(date_prcp_dict)}")
    # # Display the number of results
    # print(f"date_prcp_dict results: {len(date_prcp_dict)}")

    
    # # Convert dictionary to JSON
    # date_prcp_json = date_prcp_dict
    
    # #print(date_prcp_json)
    # return date_prcp_json
    # ERR / ONLY LAST VALUE FOUND PER KEY IS RETAINED ----------------------------------------


    from collections import defaultdict
    
    # Initialize a defaultdict with a list
    date_prcp_dict = defaultdict(list)
    
    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        date_prcp_dict[row['date']].append(row['prcp'])

    # Display the type of the measurements variable
    print(f"date_prcp_dict type: {type(date_prcp_dict)}")
    # Display the number of results
    print(f"date_prcp_dict results: {len(date_prcp_dict)}")


    # If you want to convert defaultdict to a regular dictionary
    date_prcp_dict = dict(date_prcp_dict)

    # Display the type of the measurements variable
    print(f"date_prcp_dict type: {type(date_prcp_dict)}")
    # Display the number of results
    print(f"date_prcp_dict results: {len(date_prcp_dict)}")


#    print(date_prcp_dict)

    # return date_prcp_json
    return date_prcp_dict


#################################################
# Flask Setup
#################################################

# Create an app, being sure to pass __name__
app = Flask(__name__)


#################################################
# Flask Routes
#################################################


# @app.errorhandler(404)
# def not_found(error):
#     return jsonify({
#         "error": "The requested resource was not found on the server.",
#         "invalid_route": request.path
#     }), 404

# NOTE: the key-value pair ordering of the json response,
#       ... as recieved by a web browser (or curl command)
#       ... may not appear in the same order as being written here
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

# NOTE: turns out that the json key-value pair ordering is mixed up anyways,
#       as displayed from web browser , or a curl command
#       despite "response_data = OrderedDict" having intended element order
#       ... so , going back to the non-OrderedDict version of the function
#
# # NOTE enforces a particular order ( for viewing consistency )
# #
# from collections import OrderedDict
# #
# @app.errorhandler(404)
# def not_found(error):
#     response_data = OrderedDict([
#         ("error_message", "The requested resource was not found on the server."),
#         ("invalid_route", request.path),
#         ("error", {
#             "code": error.code,
#             "name": error.name,
#             "description": error.description
#         })
#     ])

#     print( type(response_data) )
#     print(response_data)
    
#     return jsonify(response_data), 404
    

# Define what to do when a user hits the index route.
@app.route("/")
def home():
    return (
        f"Welcome to the Climate API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        #
        # f"/api/v1.0/<start><br/>"
        # f"/api/v1.0/<start>/<end>"
        #
        # f"/api/v1.0/{{start}}<br/>"
        # f"/api/v1.0/{{start}}/{{end}}"
        #
        f"/api/v1.0/&lt;start&gt;<br/>"   # Use &lt; and &gt; for < and > in HTML
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;"
    )    

# Define what to do when a user hits the /normal route
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

# Define what to do when a user hits the /jsonified route
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

# Define what to do when a user hits the /jsonified route
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

# Define what to do when a user hits the /jsonified route
@app.route("/api/v1.0/<start>")
def api_start(start):

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

# Define what to do when a user hits the /jsonified route
@app.route("/api/v1.0/<start>/<end>")
def api_start_end(start, end):

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
    
#################################################

    # if (True):
    #     response_data = {
    #         "message": "Hello from /api/v1.0/<start>/<end>",
    #         "start": start,
    #         "end": end
    #     }
    #     return jsonify(response_data), 200
    # else:
    #     error_message = "YOU_DID_IT_WRONG"
    #     error_response = {
    #         "error": f"Request terminated due to an unfortunate occurance of an {error_message} error."
    #     }
    #     return jsonify(error_response), 404

#################################################


# This block ensures the Flask application runs only when the script is executed directly,
# not when it is imported as a module. It starts the Flask development server in debug mode,
# enabling automatic reloading and detailed error messages.
if __name__ == "__main__":
    app.run(debug=True)
