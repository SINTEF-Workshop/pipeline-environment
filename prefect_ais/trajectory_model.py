from prefect import flow, task, context
import mlflow
import pytorch_lightning as pl
from ML_data_pipeline.src.simple_model.rnn_pl_model import RNNDataModule, RNN

@task
def train_model():
    batch_size = 5000
    input_len = 10
    hid_dim = 100
    lr = 0.0001
    output_len = 1
    max_epochs = 10
    patience = 10

    # Data
    dm = RNNDataModule(batch_size=batch_size, schema="ais", tablename='trajectories', input_len=input_len, output_len=output_len)

    # Model
    model = RNN(feature_dim=dm.feature_dim, hid_dim=hid_dim, lr=lr)

    model.scaler = dm.scaler
    model.train_idx = dm.train_idx
    model.val_idx = dm.val_idx
    model.test_idx = dm.test_idx

    #mlflow.pytorch.autolog(registered_model_name=model_name)

    with mlflow.start_run() as run:
        mlflow.autolog()
        mlflow.pytorch.log_model(pytorch_model=model, artifact_path='prediction_model')
        mlflow.sklearn.log_model(dm.scaler, artifact_path='scaler')

        # Training
        early_stop_callback = pl.callbacks.early_stopping.EarlyStopping(monitor="val_loss", patience=patience,
                                                                        mode="min")
        #trainer = pl.Trainer(max_epochs=max_epochs, callbacks=[early_stop_callback], fast_dev_run=False)

        # If using GPU, train on 2 GPUs, using the DDP strategy

        trainer = pl.Trainer(max_epochs=max_epochs, #log_every_n_steps=1,
                            callbacks=[early_stop_callback], fast_dev_run=False)

        trainer.fit(model, dm)


        print(model)
        #torch.save(model,'../prelim_results/models/rnn_models/rnn_100.pt')


@flow
async def trajectory_model():
    trained_model = train_model()
 

# if __name__ == "__main__":
#     asyncio.run(generate_trajectories())