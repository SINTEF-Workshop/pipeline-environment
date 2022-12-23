import argparse
import pytorch_lightning as pl
from .simple_model.rnn_pl_model import RNNDataModule, RNN
import mlflow
import torch

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", default=5000, type=int)
    parser.add_argument("--input_len", default=10, type=int)
    parser.add_argument("--output_len", default=1, type=int)
    parser.add_argument("--patience", default=10, type=int, help='Patience in early stopping criterion.')
    parser.add_argument("--hid_dim", default=100, type=int)
    parser.add_argument("--lr", default=0.0001, type=float, help='Learning rate')
    parser.add_argument("--max_epochs", default=100, type=int)
    parser.add_argument("--gpus", default=1, type=int, help='Number of GPUs used for training.')
    parser.add_argument("--model_name", default='simpleRNN', type=str, help='Name of model')
    #parser = pl.Trainer.add_argparse_args(parser)
    args = parser.parse_args()

    # Data
    dm = RNNDataModule(batch_size=args.batch_size, schema="ais", tablename='trajectories', input_len=args.input_len, output_len=args.output_len)

    # Model
    model = RNN(feature_dim=dm.feature_dim, hid_dim=args.hid_dim,
                lr=args.lr)

    model.scaler = dm.scaler
    model.train_idx = dm.train_idx
    model.val_idx = dm.val_idx
    model.test_idx = dm.test_idx

    print("Running Training")
    #mlflow.pytorch.autolog(registered_model_name=args.model_name)

    with mlflow.start_run() as run:
        mlflow.autolog()
        mlflow.pytorch.log_model(pytorch_model=model, artifact_path='prediction_model')
        mlflow.sklearn.log_model(dm.scaler, artifact_path='scaler')

        # Training
        early_stop_callback = pl.callbacks.early_stopping.EarlyStopping(monitor="val_loss", patience=args.patience,
                                                                         mode="min")
        #trainer = pl.Trainer(max_epochs=args.max_epochs, callbacks=[early_stop_callback], fast_dev_run=False)

        # If using GPU, train on 2 GPUs, using the DDP strategy

        trainer = pl.Trainer(max_epochs=args.max_epochs, log_every_n_steps=1,
                             callbacks=[early_stop_callback], fast_dev_run=False)

        trainer.fit(model, dm)
        #torch.save(model,'../prelim_results/models/rnn_models/rnn_100.pt')


def train_model():
    batch_size = 5000
    input_len = 10
    hid_dim = 100
    lr = 0.0001
    output_len = 1
    max_epochs = 100
    patience = 10

    # Data
    dm = RNNDataModule(batch_size=batch_size, schema="ais", tablename='trajectories', input_len=input_len, output_len=output_len)

    # Model
    model = RNN(feature_dim=dm.feature_dim, hid_dim=hid_dim, lr=lr)

    model.scaler = dm.scaler
    model.train_idx = dm.train_idx
    model.val_idx = dm.val_idx
    model.test_idx = dm.test_idx

    print("Running Training")
    #mlflow.pytorch.autolog(registered_model_name=model_name)

    with mlflow.start_run() as run:
        mlflow.autolog()
        mlflow.pytorch.log_model(pytorch_model=model, artifact_path='prediction_model')
        mlflow.sklearn.log_model(dm.scaler, artifact_path='scaler')

        # Training
        early_stop_callback = pl.callbacks.early_stopping.EarlyStopping(monitor="val_loss", patience=patience, mode="min")

        # If using GPU, train on 2 GPUs, using the DDP strategy
        trainer = pl.Trainer(max_epochs=max_epochs, callbacks=[early_stop_callback], fast_dev_run=False)

        trainer.fit(model, dm)
        #torch.save(model,'../prelim_results/models/rnn_models/rnn_100.pt')