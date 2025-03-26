import requests
import pandas as pd
import json
import sqlalchemy as sa
from sqlalchemy import text
from datetime import datetime, timedelta, timezone


# read the file locally
df = pd.read_csv('C:/Users/bbeen/Downloads/ca_geo_dimension.csv')

# 13 provinces; this satisfies the condition of 10 - 15 locations,
# while representing each region of Canada (first zipcodes with keep='first' param)
df1 = df.drop_duplicates(subset=['province'], keep='first')

api = '899b0fc42b25bf5b79bfcba655cb6c17'

forecastAggregate = pd.DataFrame()
historyAggregate = pd.DataFrame()


# Function to pull data from a week ago
def find_historic(lat, lon, zip):
    historicReq = 'https://history.openweathermap.org/data/2.5/history/city?lat=' + lat + '&lon=' + lon + '&appid=' + api + '&units=metric&cnt=168'
    response = requests.get(historicReq)
    data = json.loads(response.content)

    history = pd.json_normalize(data['list'])
    
    history[['dt']] = history[['dt']].map(lambda x: datetime.fromtimestamp(x, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
    history['datePulled'] = datetime.today().strftime('%Y-%m-%d')

    history['lat'] = lat
    history['lon'] = lon
    history['zip'] = zip

    history = history.drop(columns=['weather'])
    return history

# Function to pull forecast data, conveniently this pulls today as well
def find_forecast(lat, lon, zip):
    forecastReq = 'https://api.openweathermap.org/data/3.0/onecall?lat=' + lat + '&lon=' + lon + '&appid=' + api + '&units=metric'
    response = requests.get(forecastReq)
    data = json.loads(response.content)
    
    forecast = pd.json_normalize(data['daily'])
    forecast[['dt', 'sunrise', 'sunset', 'moonrise', 'moonset']] = forecast[['dt', 'sunrise', 'sunset', 'moonrise', 'moonset']].map(
        lambda x: datetime.fromtimestamp(x, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
    forecast['datePulled'] = datetime.today().strftime('%Y-%m-%d')
    
    forecast['type'] = 'Forecast'
    forecast.loc[0, 'type'] = 'Current'

    forecast['lat'] = lat
    forecast['lon'] = lon
    forecast['zip'] = zip
    forecast = forecast.drop(columns=['summary', 'weather'])
    return forecast

# Only gathers yesterday's data
def find_yesterday(lat, lon, zip):
    
    today = datetime.today()

    end = datetime(today.year, today.month, today.day, 0, 0, tzinfo=timezone.utc)
    start = end - timedelta(days=1)

    start_timestamp = start.timestamp()
    start_timestamp = str(start_timestamp)
    
    historicReq = 'https://history.openweathermap.org/data/2.5/history/city?lat=' + lat + '&lon=' + lon + '&appid=' + api + '&units=metric&type=hour&cnt=24' + '&start=' + start_timestamp
    response = requests.get(historicReq)
    data = json.loads(response.content)

    history = pd.json_normalize(data['list'])
    
    history[['dt']] = history[['dt']].map(lambda x: datetime.fromtimestamp(x, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
    history['datePulled'] = datetime.today().strftime('%Y-%m-%d')

    history['lat'] = lat
    history['lon'] = lon
    history['zip'] = zip

    history = history.drop(columns=['weather'])
    return history

today = datetime.today()

end = datetime(today.year, today.month, today.day, 0, 0, tzinfo=timezone.utc)
start = end - timedelta(days=7)

# Convert to UNIX time for API call
start_timestamp = start.timestamp()


# Convert it back to string so that it can be called to the API
start_timestamp = str(start_timestamp)


# Calls find_forecast function for each location seen in df1
for index, row in df1.iterrows():
    latitude = row['latitude']
    longitude = row['longitude']
    zipcode = row['zipcode']
    
    forecast = find_forecast(str(latitude), str(longitude), zipcode)
    forecastAggregate = pd.concat([forecastAggregate, forecast])
    forecastAggregate = forecastAggregate.reset_index(drop=True)
print('test')

# Calls find_historic for each location seen in df1
for index, row in df1.iterrows():
    latitude = row['latitude']
    longitude = row['longitude']
    zipcode = row['zipcode']

    history = find_historic(str(latitude), str(longitude), zipcode)
    historyAggregate = pd.concat([historyAggregate, history])
    historyAggregate = historyAggregate.reset_index(drop=True)
print('worked')

# Splits forecastAggregate into current_weather and forecast_weather
current_weather = forecastAggregate[forecastAggregate.duplicated(subset = ['type', 'zip'], keep=False) == False]
forecast_weather = forecastAggregate[forecastAggregate.duplicated(subset = ['type', 'zip'], keep=False) == True]

# Setting up connection_url to connect to the database for storage
connection_url = sa.engine.URL.create(
    drivername = "mssql+pyodbc",
    username   = "jaeloh",       ## Please add your user
    password   = "2024!Schulich",
    host       = "mmai2024-ms-sql-server.c1oick8a8ywa.ca-central-1.rds.amazonaws.com",
    port       = "1433",
    database   = "jaeloh_db",       ## Please add your database
    query = {
        "driver" : "ODBC Driver 18 for SQL Server",
        "TrustServerCertificate" : "yes"
    }   
)

# my_engine is created
my_engine = sa.create_engine(connection_url)

# Pull TABLE_NAME from INFORMATION_SCHEMA to see historicWeather already exists
with my_engine.connect() as connection:
    text = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE \'historicWeather\''
    output = pd.read_sql(text, connection)

# If output dataframe is empty, then historicWeather needs to be created & populated with historyAggregate
if output.empty:
    historyAggregate.to_sql(
        name   = 'historicWeather',
        con    = my_engine,
        schema = 'uploads',
        if_exists = 'replace',
        index  = False,
        chunksize = 50,
        dtype  = {
            'dt' : sa.types.DATETIME,
            'main.temp' : sa.types.DECIMAL(10,2),
            'main.feels_like' : sa.types.DECIMAL(10,2),
            'main.pressure' : sa.types.INT,
            'main.humidity' : sa.types.INT,
            'main.temp_min' : sa.types.DECIMAL(10,2),
            'main.temp_max' : sa.types.DECIMAL(10,2),
            'wind.speed' : sa.types.DECIMAL(10,2),
            'wind.deg' : sa.types.INT,
            'wind.gust' : sa.types.DECIMAL(10,2),
            'clouds.all' : sa.types.INT,
            'rain.1h' : sa.types.DECIMAL(10,2),
            'datePulled' : sa.types.DATETIME,
            'lat' : sa.types.FLOAT,
            'lon' : sa.types.FLOAT,
            'zip' : sa.types.VARCHAR,
            'snow.1h' : sa.types.DECIMAL(10,2)
        },
        method = 'multi'
    )

# Similarly, check if forecastWeather table already exists. If empty, then populate with forecast_weather
with my_engine.connect() as connection:
    text = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE \'forecastWeather\''
    output = pd.read_sql(text, connection)

if output.empty:
    forecast_weather.to_sql(
    name    = 'forecastWeather',
    con     = my_engine,
    schema  = 'uploads',
    if_exists = 'replace',
    index   = False,
    chunksize = 50,
    dtype   = {
        'dt' : sa.types.DATETIME,
        'sunrise' : sa.types.DATETIME,
        'sunset' : sa.types.DATETIME,
        'moonrise' : sa.types.DATETIME,
        'moonset' : sa.types.DATETIME,
        'moon_phase' : sa.types.DECIMAL(10,2),
        'pressure' : sa.types.INT,
        'humidity' : sa.types.INT,
        'dew_point' : sa.types.DECIMAL(10,2),
        'wind_speed' : sa.types.DECIMAL(10,2),
        'wind_deg' : sa.types.INT,
        'wind_gust' : sa.types.DECIMAL(10,2),
        'clouds' : sa.types.INT,
        'pop' : sa.types.DECIMAL(10,2),
        'uvi' : sa.types.DECIMAL(10,2),
        'temp.day' : sa.types.DECIMAL(10,2),
        'temp.min' : sa.types.DECIMAL(10,2),
        'temp.max' : sa.types.DECIMAL(10,2),
        'temp.night' : sa.types.DECIMAL(10,2),
        'temp.eve' : sa.types.DECIMAL(10,2),
        'temp.morn' : sa.types.DECIMAL(10,2),
        'feels_like.day' : sa.types.DECIMAL(10,2),
        'feels_like.night' : sa.types.DECIMAL(10,2),
        'feels_like.eve' : sa.types.DECIMAL(10,2),
        'feels_like.morn' : sa.types.DECIMAL(10,2),
        'rain' : sa.types.DECIMAL(10,2),
        'datePulled' : sa.types.DATETIME,
        'type' : sa.types.VARCHAR,
        'lat' : sa.types.FLOAT,
        'lon' : sa.types.FLOAT,
        'zip' : sa.types.VARCHAR
    },
    method = 'multi'
    )

# Acquire the most recent date from historicWeather's datePulled column
with my_engine.connect() as connection:
    text = 'SELECT TOP 1 datePulled FROM uploads.historicWeather ORDER BY datePulled DESC'
    output = pd.read_sql(text, connection)

# Format it for comparison, to yyyy/mm/dd
datecheck = str(output['datePulled'][0])
lastdate = datetime.strptime(datecheck[0:10], '%Y-%m-%d')

# If today's date is not the last date, then
if lastdate.date() != datetime.today().date():
    # Make yesterdayAggregate dataframe
    yesterdayAggregate = pd.DataFrame()
    # For each location in df1
    for index, row in df1.iterrows():
        latitude = row['latitude']
        longitude = row['longitude']
        zipcode = row['zipcode']
        # find_yesterday is applied, and continue adding newly sourced data back into yesterdayAggregate
        yesterday = find_yesterday(str(latitude), str(longitude), zipcode)
        yesterdayAggregate = pd.concat([yesterdayAggregate, yesterday])
        yesterdayAggregate = yesterdayAggregate.reset_index(drop=True)

    # Ingest yesterday's weather into historicWeather TABLE, append so as to keep the existing historical data
    yesterdayAggregate.to_sql(
        name   = 'historicWeather',
        con    = my_engine,
        schema = 'uploads',
        if_exists = 'append',
        index  = False,
        chunksize = 50,
        dtype  = {
            'dt' : sa.types.DATETIME,
            'main.temp' : sa.types.DECIMAL(10,2),
            'main.feels_like' : sa.types.DECIMAL(10,2),
            'main.pressure' : sa.types.INT,
            'main.humidity' : sa.types.INT,
            'main.temp_min' : sa.types.DECIMAL(10,2),
            'main.temp_max' : sa.types.DECIMAL(10,2),
            'wind.speed' : sa.types.DECIMAL(10,2),
            'wind.deg' : sa.types.INT,
            'wind.gust' : sa.types.DECIMAL(10,2),
            'clouds.all' : sa.types.INT,
            'rain.1h' : sa.types.DECIMAL(10,2),
            'datePulled' : sa.types.DATETIME,
            'lat' : sa.types.FLOAT,
            'lon' : sa.types.FLOAT,
            'zip' : sa.types.VARCHAR,
            'snow.1h' : sa.types.DECIMAL(10,2)
                },
        method = 'multi'
    )

# Acquire the most recent date from currentWeather's datePulled column
with my_engine.connect() as connection:
    text = 'SELECT TOP 1 datePulled FROM uploads.currentWeather ORDER BY datePulled DESC'
    output = pd.read_sql(text, connection)

datecheck = str(output['datePulled'][0])
lastdate = datetime.strptime(datecheck[0:10], '%Y-%m-%d')

# If today's date is not the last date, then ingest & append current_weather into currentWeather TABLE
if lastdate.date() != datetime.today().date():
    current_weather.to_sql(
    name    = 'currentWeather',
    con     = my_engine,
    schema  = 'uploads',
    if_exists = 'append',
    index   = False,
    chunksize = 50,
    dtype   = {
        'dt' : sa.types.DATETIME,
        'sunrise' : sa.types.DATETIME,
        'sunset' : sa.types.DATETIME,
        'moonrise' : sa.types.DATETIME,
        'moonset' : sa.types.DATETIME,
        'moon_phase' : sa.types.DECIMAL(10,2),
        'pressure' : sa.types.INT,
        'humidity' : sa.types.INT,
        'dew_point' : sa.types.DECIMAL(10,2),
        'wind_speed' : sa.types.DECIMAL(10,2),
        'wind_deg' : sa.types.INT,
        'wind_gust' : sa.types.DECIMAL(10,2),
        'clouds' : sa.types.INT,
        'pop' : sa.types.DECIMAL(10,2),
        'uvi' : sa.types.DECIMAL(10,2),
        'temp.day' : sa.types.DECIMAL(10,2),
        'temp.min' : sa.types.DECIMAL(10,2),
        'temp.max' : sa.types.DECIMAL(10,2),
        'temp.night' : sa.types.DECIMAL(10,2),
        'temp.eve' : sa.types.DECIMAL(10,2),
        'temp.morn' : sa.types.DECIMAL(10,2),
        'feels_like.day' : sa.types.DECIMAL(10,2),
        'feels_like.night' : sa.types.DECIMAL(10,2),
        'feels_like.eve' : sa.types.DECIMAL(10,2),
        'feels_like.morn' : sa.types.DECIMAL(10,2),
        'rain' : sa.types.DECIMAL(10,2),
        'datePulled' : sa.types.DATETIME,
        'type' : sa.types.VARCHAR,
        'lat' : sa.types.FLOAT,
        'lon' : sa.types.FLOAT,
        'zip' : sa.types.VARCHAR
    },
    method = 'multi'
)

# Acquire the most recent date from forecastWeather's datePulled column
with my_engine.connect() as connection:
    text = 'SELECT TOP 1 datePulled FROM uploads.forecastWeather ORDER BY datePulled DESC'
    output = pd.read_sql(text, connection)

datecheck = str(output['datePulled'][0])
lastdate = datetime.strptime(datecheck[0:10], '%Y-%m-%d')

# If today's date is not the last date, then ingest & append forecast_weather into forecastWeather TABLE
if lastdate.date() != datetime.today().date():
    forecast_weather.to_sql(
    name    = 'forecastWeather',
    con     = my_engine,
    schema  = 'uploads',
    if_exists = 'append',
    index   = False,
    chunksize = 50,
    dtype   = {
        'dt' : sa.types.DATETIME,
        'sunrise' : sa.types.DATETIME,
        'sunset' : sa.types.DATETIME,
        'moonrise' : sa.types.DATETIME,
        'moonset' : sa.types.DATETIME,
        'moon_phase' : sa.types.DECIMAL(10,2),
        'pressure' : sa.types.INT,
        'humidity' : sa.types.INT,
        'dew_point' : sa.types.DECIMAL(10,2),
        'wind_speed' : sa.types.DECIMAL(10,2),
        'wind_deg' : sa.types.INT,
        'wind_gust' : sa.types.DECIMAL(10,2),
        'clouds' : sa.types.INT,
        'pop' : sa.types.DECIMAL(10,2),
        'uvi' : sa.types.DECIMAL(10,2),
        'temp.day' : sa.types.DECIMAL(10,2),
        'temp.min' : sa.types.DECIMAL(10,2),
        'temp.max' : sa.types.DECIMAL(10,2),
        'temp.night' : sa.types.DECIMAL(10,2),
        'temp.eve' : sa.types.DECIMAL(10,2),
        'temp.morn' : sa.types.DECIMAL(10,2),
        'feels_like.day' : sa.types.DECIMAL(10,2),
        'feels_like.night' : sa.types.DECIMAL(10,2),
        'feels_like.eve' : sa.types.DECIMAL(10,2),
        'feels_like.morn' : sa.types.DECIMAL(10,2),
        'rain' : sa.types.DECIMAL(10,2),
        'datePulled' : sa.types.DATETIME,
        'type' : sa.types.VARCHAR,
        'lat' : sa.types.FLOAT,
        'lon' : sa.types.FLOAT,
        'zip' : sa.types.VARCHAR
    },
    method = 'multi'
)
