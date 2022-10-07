import numpy as np
import torch
from dagster import get_dagster_logger
from sklearn.preprocessing import MinMaxScaler
from dotenv import load_dotenv
from urllib.parse import quote_plus as url_quote
import os
import sqlalchemy as db


class processed_data():
    """
    scaler: sckikit learn min max scaler [-1,1] fit on cluster data
    data: list of trajectory features [t_0, t_1...], t_n = [[x],[y],[cog_x],[cog_y],[sog]]
    """

    def __init__(self, gdf, feature_range=(-1, 1)):


        data_list = []

        for i in range(gdf.shape[0]):
            data = np.array(gdf.iloc[i].line_geom.xy).T
            data_list.append(data)

        self.data = data_list
        self.feature_dim = self.data[0].shape[1]

        scaler = MinMaxScaler(feature_range=feature_range)
        data = np.vstack(self.data)
        scaler.fit(data)
        self.scaler, self.scaled_data = scaler, [scaler.transform(x) for x in self.data]



def get_traj_segments(data, input_length, output_length, to_tensor = True):
    logger = get_dagster_logger()
    src = []
    trg = []

    for i in range(len(data)):
        x = data[i]
        print(x.shape[0])
        print(input_length)
        print(output_length)
        print("range:", x.shape[0] - input_length - output_length)
        unpacked_src = [torch.from_numpy(x[j:j + input_length, :]).unsqueeze(1) for j in range(x.shape[0] - input_length
                                                                                               - output_length)]
        unpacked_trg = [torch.from_numpy(x[j:j + output_length, :]).unsqueeze(1) for j in range(input_length, x.shape[0]
                                                                                                - output_length)]

        print("unpacked: ", unpacked_src)
        src.extend(unpacked_src)
        trg.extend(unpacked_trg)

    logger.info("src")
    logger.info(src)
    logger.info("trg")
    logger.info(trg)


    if to_tensor:
        src = torch.cat(src, dim=1).float()
        trg = torch.cat(trg, dim=1).float()
    else:
        pass

    return src, trg


def string_to_float(x):
    x = x.replace("[", "")
    x = x.replace("]", "")
    x = x.replace(",","")
    x = list(x.split(" "))
    x = [float(i) for i in x]
    return x


def get_con():
    return connection2Postgres(get_db_url())


def connection2Postgres(url: str):
    engine = db.create_engine(url, echo=False)
    return engine.connect()


def get_db_url():
    load_dotenv()
    host = os.getenv('DB_AZURE_HOST')
    dbname = os.getenv('DB_AZURE_NAME')
    user = os.getenv('DB_AZURE_USERNAME')
    password = os.getenv('DB_AZURE_PASSWORD')
    db_url = 'postgresql+psycopg2://{}:{}@{}:5432/{}'.format(user, url_quote(password), host, dbname)
    return db_url
