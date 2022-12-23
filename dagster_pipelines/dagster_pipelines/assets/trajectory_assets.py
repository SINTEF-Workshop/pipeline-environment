from dagster import asset, get_dagster_logger
import pandas as pd
import numpy as np
import asyncio
import json
import pytz
from datetime import datetime, timedelta
from shapely.geometry import LineString, Point
import geopandas as gpd
import dateutil.parser


@asset(group_name="trajectory_assets", compute_kind="nats-stream", required_resource_keys={ "nats_client" })
async def ais_messages(context) -> pd.DataFrame:
    """ Collect messages from nats stream and store in DataFrame """
    nats_conn = context.resources.nats_client
    end_time, start_time = get_interval()

    # Connect to local nats server
    nc = await nats_conn.get_connection("localhost:4222")
    js = nc.jetstream()

    # Subscribe to the stream
    psub = await js.subscribe("ais", durable="fff")
    
    # Collect messages to DataFrame
    df_list = await get_messages(psub, end_time, start_time)

    # Close NATS connection
    await nc.close()

    logger = get_dagster_logger()
    logger.info(df_list)

    return pd.concat(df_list)


@asset(group_name="trajectory_assets", compute_kind="pandas")
def ais_messages_cleaned(context, ais_messages: pd.DataFrame) -> pd.DataFrame:
    """ Removes null values and filters out unrealistic values """
    # Remove null values
    ais = ais_messages
    ais.dropna(inplace=True)
    
    # Cast to appropriate type
    ais = ais.astype(dtype={'mmsi': 'int64'})
    ais['msgtime'] = pd.to_datetime(ais['msgtime'])
    
    # Remove unrealistic speed and course values
    ais = ais[ais['speedoverground'] < 40]
    ais = ais[ais['courseoverground'] <= 360]

    ais.reset_index(inplace=True)

    return ais


@asset(required_resource_keys={"track_maker"}, group_name="trajectory_assets", compute_kind="pandas")
def trajectory_list(context, ais_messages_cleaned: pd.DataFrame) -> list[pd.DataFrame]:
    """ Creates the routes """
    interpolate_traj = context.resources.track_maker
    ais = ais_messages_cleaned
    delta_t=10
    delta_sog=0.2
    interpolate=True

    ais = ais[ais["speedoverground"] > delta_sog]  # Filter out stopping points
    ais = ais.sort_values(by=['mmsi', 'msgtime'])
    ais = ais.reset_index()

    mmsi_bool = ais.mmsi.diff() > 0  # Find changes in imo
    time_bool = ais.msgtime.diff() > timedelta(minutes=delta_t)  # Find changes in time
    tot_bool = np.column_stack((mmsi_bool, time_bool)).any(axis=1)

    traj_index = ais.index[tot_bool]

    traj_list = []
    i = 0
    for i in range(len(traj_index) - 1):
        traj = ais[traj_index[i]:traj_index[i + 1]]

        if traj.shape[0] > 5:
            i+=1
            if interpolate:
                traj_list.append(interpolate_traj(traj))
            else:
                traj_list.append(traj)

    return traj_list


@asset(required_resource_keys={"track_maker"}, group_name="trajectory_assets", compute_kind="pandas")
def trajectory_dataframe(trajectory_list: list[pd.DataFrame]) -> gpd.GeoDataFrame:
    """ Creates a GeoDataFrame from the list of trajectories """
    df = pd.DataFrame(columns=(
    'mmsi', 
        # 'imo', 
    'start_lon', 'start_lat', 'stop_lon', 'stop_lat', 'start_loc', 'stop_loc', 'start_geom',
    'stop_geom', 'start_time', 'stop_time', 'courseoverground', 'avg_cog', 'speedoverground', 'avg_sog', 'loc', 'line_geom', 'msgtime'))

    for traj in trajectory_list:
        if traj.shape[0] > 5:
            line = LineString(list(zip(traj['longitude'], traj['latitude'])))
            start_loc = Point(traj.iloc[0]['longitude'], traj.iloc[0]['latitude'])
            stop_loc = Point(traj.iloc[-1]['longitude'], traj.iloc[-1]['latitude'])

            row = pd.DataFrame({'mmsi': [traj.iloc[0]['mmsi']], 
                                    # 'imo': [traj.iloc[0]['mmsi']],
                                'start_lon': [traj.iloc[0]['longitude']],
                                'start_lat': [traj.iloc[0]['latitude']], 
                                'stop_lon': [traj.iloc[-1]['longitude']],
                                'stop_lat': [traj.iloc[-1]['latitude']], 
                                'start_loc': start_loc.wkt,
                                'stop_loc': stop_loc.wkt, 
                                'start_geom': start_loc, 
                                'stop_geom': stop_loc,
                                'start_time': [traj.iloc[0]['msgtime']], 
                                'stop_time': [traj.iloc[-1]['msgtime']],
                                'avg_cog': [np.mean(traj['courseoverground'])], 
                                'avg_sog': [np.mean(traj['speedoverground'])],
                                'courseoverground': [list(traj['courseoverground'])], 
                                'speedoverground': [list(traj['speedoverground'])], 
                                'loc': line.wkt,
                                'line_geom': line, 'msgtime': [list(traj['msgtime'])]})

            df = pd.concat((df, row))
    gdf = gpd.GeoDataFrame(df, geometry='line_geom')
    gdf = gdf.set_crs('EPSG:4326')

    return gdf

@asset(required_resource_keys={"postgres"}, group_name="trajectory_assets", compute_kind="postgres")
def trajectory_table(context, trajectory_dataframe: gpd.GeoDataFrame):
    """ Inserts the trajectories to postgis """
    asyncio.run(context.resources.postgres.geodf_to_postgis(trajectory_dataframe))

async def get_messages(psub, end_time, start_time):
    dataframe_list = []

    while start_time < end_time:
        # Get message
        msg = await psub.next_msg(20)
        data = msg.data.decode()

        # Extract the date
        date = json.loads(data)["msgtime"]
        timestamp = dateutil.parser.parse(date)

        # Append to file
        data = json.loads(str( data ).lower())
        df = pd.json_normalize(data)
        dataframe_list.append(df)

        # update the time
        start_time = timestamp

        await msg.ack()

    return dataframe_list

def get_interval() -> tuple[datetime, datetime]:
    """ Gets the time interval to fetch data for """
    utc = pytz.utc
    # End time is the time the run is initiated
    end_time = utc.localize(datetime.now())
    # Start time defaults to yesterday
    start_time = utc.localize(datetime.today() - timedelta(days=1))

    return end_time, start_time