from dagster import get_dagster_logger
import geopandas as gpd
from tqdm import tqdm
import numpy as np
import pandas as pd
from datetime import timedelta
from shapely.geometry import LineString, Point, Polygon

class TrackMaker():

    def load_trips(self, path='./data/trips.pkl', geom='line_geom', crs='EPSG:4326'):
        """
        Load pre-processed data
        """
        gdf = gpd.GeoDataFrame(pd.read_pickle(path))
        gdf = gdf.set_geometry(geom)
        gdf = gdf.set_crs(crs)

        return gdf

    async def create_trajectories(self, df):
        log = get_dagster_logger().info
        log("Cleaning data...")
        cleaned = self.clean_ais(df)
        log("Splitting routes...")
        routes = self.split_routes(cleaned, delta_t=10, delta_sog=0.2, interpolate=True)
        log("Generating tracks...")
        trips = self.gen_trips(routes)
        return trips

        
    def gen_trips(self, traj_list):
        df = pd.DataFrame(columns=(
        'mmsi', 
         # 'imo', 
        'start_lon', 'start_lat', 'stop_lon', 'stop_lat', 'start_loc', 'stop_loc', 'start_geom',
        'stop_geom', 'start_time', 'stop_time', 'courseoverground', 'avg_cog', 'speedoverground', 'avg_sog', 'loc', 'line_geom', 'msgtime'))

        for traj in tqdm(traj_list):
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



    def clean_ais(self, ais, mask = False):
        """
        Remove unrealistic speed and course values

        mask = [lower_lon, upper_lon, lower_lat, upper_lat]
        """
        ais.dropna(inplace=True)
        ais = ais.astype(dtype={'mmsi': 'int64', 'name' : 'object', 'speedoverground' : 'int64', 'rateofturn' : 'int64', 'trueheading' : 'int64', 'courseoverground' : 'int64'})
        ais = ais[ais['speedoverground'] < 40]
        ais = ais[ais['courseoverground'] <= 360]
        ais['msgtime'] = pd.to_datetime(ais['msgtime'])

        ais.reset_index(inplace=True)

        """
        If local_coast.shp does not exist, clip from Europe coastline using "get_masked_map"
        """
        if mask:
            try:
                m = gpd.read_file('./data/local_coast.shp')  # load map data
                m = m.set_crs('EPSG:4326')
            except:
                m = self.get_masked_map(mask)
                m = m.set_crs('EPSG4236')

            points = gpd.GeoDataFrame(
                geometry=gpd.points_from_xy(ais.lon, ais.lat, crs='EPSG:4326'))  # Generate point geometries
            points.set_crs('EPSG:4326')  # Set CRS

            polygons_contains = gpd.sjoin(points, m, op='within')  # find points that are inside land polygons
            clean = ais.loc[~ais.index.isin(polygons_contains.index)]  # Remove these data points from data set
        else:
            clean = ais

        return clean

    def get_masked_map(self, geom_path='.data/Europe Coastline (Polygone).shp', mask=None):
        """
        Clip data to given mask (e.g. Oslo fjord)
        save file will save geometry to file e.g. .shp
        """

        m = gpd.read_file(geom_path)
        m = m.set_crs('EPSG:3035')
        m = m.to_crs('EPSG:4326')

        try:
            lower_lon, upper_lon, lower_lat, upper_lat = mask[0], mask[1], mask[2], mask[3]
            mask = [(lower_lon, lower_lat), (upper_lon, lower_lat), (upper_lon, upper_lat), (lower_lon, upper_lat)]
            mask = Polygon(mask)
            m = gpd.clip(m, mask)  # change gdf to m?
            save_path = '/data/local_coast.shp'
            m.to_file(save_path)
            return m
        except:
            print('Invalid mask')

    def split_routes(self, ais, delta_t=10, delta_sog=0.2, interpolate=True):
        """
        Parameters
        ----------
        ais : df of ais data
        delta_t : integer [min]
            time between trajectories
        delta_sog: float [kn]
            filter threshold for stopping points

        Returns
        -------
        List of trajectories
        """
        ais = ais[ais["speedoverground"] > delta_sog]  # Filter out stopping points
        ais = ais.sort_values(by=['mmsi', 'msgtime'])
        ais = ais.reset_index()

        mmsi_bool = ais.mmsi.diff() > 0  # Find changes in imo
        time_bool = ais.msgtime.diff() > timedelta(minutes=delta_t)  # Find changes in time
        tot_bool = np.column_stack((mmsi_bool, time_bool)).any(axis=1)

        traj_index = ais.index[tot_bool]

        traj_list = []
        i = 0
        for i in tqdm(range(len(traj_index) - 1)):
            traj = ais[traj_index[i]:traj_index[i + 1]]

            if traj.shape[0] > 5:
                i+=1
                if interpolate:
                    traj_list.append(self.interpolate_traj(traj))
                else:
                    traj_list.append(traj)

        return traj_list

    def interpolate_traj(self, df, freq='min'):
        """
        Interpolate a Pandas DataFrame with AIS trajectory data at given frequency
        Drops other data
        """
        # df.to_parquet('before_int.parquet')
        df = df.copy()
        df.drop(columns=['level_0', 'index'], inplace=True)

        time_index = pd.date_range(start=df.iloc[0]['msgtime'], end=df.iloc[-1]['msgtime'], freq=freq)
        index = pd.DataFrame(list(df['msgtime']) + list(time_index), columns=['ind'])
        index.drop_duplicates(keep='first', inplace=True)

        index.sort_values(by='ind', inplace=True)
        index.drop_duplicates(keep='first', inplace=True)
        index.reset_index(drop=True, inplace=True)

        df.set_index('msgtime', inplace=True, drop=True)

        df = df.reindex(index.loc[:, 'ind'])

        df.interpolate(method='time', inplace=True, limit_direction='forward', axis=0)

        df = df[df.index.isin(time_index)]

        df['msgtime'] = df.index
        df.reset_index(inplace=True, drop=True)
        # df.to_parquet('after_int.parquet')

        return df
