import torch
import torch.nn as nn
import pytorch_lightning as pl
from torch.utils.data import DataLoader, Dataset
import numpy as np
import geopandas as gpd
from dagster import get_dagster_logger
from dagster_ais.components.postgres_conn import PostgresConn
from dagster_ais.ML_data_pipeline.src.simple_model.utils import get_traj_segments, string_to_float, get_con
from dagster_ais.ML_data_pipeline.src.simple_model.utils import processed_data
from sklearn.model_selection import train_test_split

from sqlalchemy import create_engine

class AISDataset(Dataset):
    # Custom Pytorch dataset
    def __init__(self, data, data_idx, input_len, output_len):

        self.x, self.y = get_traj_segments([data.scaled_data[idx] for idx in data_idx],
                                           input_length=input_len, output_length=output_len, to_tensor=True)

    def __len__(self):
        # Number of samples in our dataset
        return self.x.shape[1]

    def __getitem__(self, idx):

        return self.x[:, idx, :], self.y[:, idx, :]

class RNNDataModule(pl.LightningDataModule):
    # Load data into our model:
    def __init__(self, batch_size=5000, schema='ais', tablename='trajectories', feature_range=(-1, 1),
                 crs='EPSG:3035', test_size=0.2, val_size=0.25, input_len=10, output_len=55, random_state=22):
        self.save_hyperparameters()
        self.batch_size = batch_size
        self.schema = schema
        self.tablename = tablename

        self.feature_range = feature_range
        self.crs = crs
        self.test_size = test_size
        self.val_size = val_size
        self.input_len = input_len
        self.output_len = output_len
        self.random_state = random_state
        self.prepare_data()
        self.prepare_data_per_node = True

    def prepare_data(self):

        logger = get_dagster_logger()
        """
        Load data from DB

        UPDATE THIS TO GET YOUR DATA
        """
        postgres_conn = PostgresConn()
        connection = postgres_conn.engine

        # connection = create_engine("postgresql://erik:password@localhost:5432/ais_data?sslmode=disable")

        traj_gdf = gpd.read_postgis(sql=f'SELECT * FROM {self.schema}.{self.tablename}', con=connection, geom_col='line_geom', crs='EPSG:4326')
        # traj_gdf['speedoverground'] = traj_gdf['speedoverground'].apply(string_to_float)
        # traj_gdf['courseoverground'] = traj_gdf['courseoverground'].apply(string_to_float)
        traj_gdf = traj_gdf.to_crs(self.crs)

        """
        Process data
        """
        data = processed_data(gdf=traj_gdf, feature_range=self.feature_range)
        logger.info(data)
        
        """
        Save values
        """
        self.gdf = traj_gdf
        self.data = data
        self.scaler = data.scaler
        self.feature_dim = data.scaled_data[0].shape[1]

        self.train_idx, self.test_idx = train_test_split(np.arange(len(self.data.scaled_data)), test_size=self.test_size,
                                               shuffle=True, random_state=self.random_state)
        self.train_idx, self.val_idx = train_test_split(self.train_idx, test_size=self.val_size, shuffle=False,
                                              random_state=self.random_state)



    def setup(self, stage):

        self.train_dataset = AISDataset(data=self.data, data_idx=self.train_idx, input_len=self.input_len,
                                        output_len=self.output_len)
        self.val_dataset = AISDataset(data=self.data, data_idx=self.val_idx, input_len=self.input_len,
                                      output_len=self.output_len)
        self.test_dataset = AISDataset(data=self.data, data_idx=self.test_idx, input_len=self.input_len,
                                       output_len=self.output_len)



    def train_dataloader(self):
        return DataLoader(self.train_dataset, batch_size=self.batch_size, num_workers=0)

    def val_dataloader(self):
        return DataLoader(self.val_dataset, batch_size=self.batch_size, num_workers=0)

    def test_dataloader(self):
        return DataLoader(self.test_dataset, batch_size=self.batch_size, num_workers=0)

    def predict_dataloader(self):
        return DataLoader(self.test_dataset, batch_size=self.batch_size, num_workers=0)



class RNN(pl.LightningModule):
    def __init__(self, feature_dim: int, hid_dim: int, lr: float):
        super().__init__()
        self.save_hyperparameters() # Automtically saves all hyperparams passed in init.
        self.feature_dim = feature_dim
        self.hid_dim = hid_dim
        self.lr = lr
        self.criterion = nn.MSELoss(reduction='mean')

        """
        When using data module, it seems that the batch size is always given first [batch_size, length, dim]
        """
        self.rnn = nn.GRU(feature_dim, hid_dim, batch_first=True) #batch_first=False for eval?
        self.fc_out1 = nn.Linear(hid_dim, int((hid_dim)/2))
        self.fc_out = nn.Linear(int((hid_dim)/2), feature_dim)


    def forward(self, x): #x dim [input_len, batch_size, feature_dim]

        output, hidden = self.rnn(x)
        pred_in = hidden
        m = nn.ReLU()
        prediction = self.fc_out(m(self.fc_out1(pred_in)))

        return prediction

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=self.lr)
        return optimizer

    def training_step(self, batch: AISDataset, batch_idx: int):
        x, y = batch
        y_pred = self(x,)
        loss = self.criterion(torch.transpose(y_pred, 1, 0), y) # need to transpose since y is given as batch first
        self.log("train_loss", loss, on_step=False, on_epoch=True, prog_bar=True, logger=True)
        return loss

    def validation_step(self, batch: AISDataset, batch_idx: int):
        x,y = batch
        y_pred = self(x)
        val_loss = self.criterion(torch.transpose(y_pred, 1, 0), y)
        self.log("val_loss", val_loss, on_step=False, on_epoch=True, prog_bar=True, logger=True)







