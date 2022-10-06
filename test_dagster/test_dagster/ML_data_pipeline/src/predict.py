import os
import torch
import numpy as np
import mlflow



def predict(model, scaler, x, y, pred_horizon=55):

    model.eval()
    x = torch.from_numpy(scaler.transform(x)).float()
    y = torch.from_numpy(scaler.transform(y)).float()

    with torch.no_grad():

        pred = torch.zeros(y.shape)
        for j in range(pred_horizon):
            if j == 0:
                input = x
            y_pred = model(input)
            pred[j, :] = y_pred
            input = input[1:, :]
            input = torch.cat((input, y_pred), dim=0)

        y = scaler.inverse_transform(pred.numpy())
        assert y.shape[0] == pred_horizon, 'Unmatched prediction horizon'

    return y



if __name__ == '__main__':



    """
    Load model
    
        1. Choose previously trained model that you want in ml_runs/0/random_run_code/artifacts/
    
        2. Move all folders and files to test_model to run inference

    """

    """
    Loading pre-trained RNN model
    """
    scaler_path= 'ml_models/test_model/scaler/'
    model_path = 'ml_models/test_model/prediction_model/'

    scaler = mlflow.sklearn.load_model(scaler_path) #Load scaler
    model = mlflow.pytorch.load_model(model_path) #Load model
    model.eval()

    """
    Trajectory prediciton example 
    """


    """
    Get data
        x: input data (line_geom.xy)
    crs='EPSG:4326'
    MUST INPUT x of length 10 (i.e. 10 minutes/10 rows with one minute intervals)
    
    """
    x = np.ones((10, 2))
    x[:, 0]=x[:,0]*4347501
    x[:, 1]=x[:, 1]*3979015

    y = np.ones((55, 2))
    y[:,0]=y[:,0]*4347501
    y[:,1]=y[:,1]*3979015

    """
    Predict single trajectory
    x-dim: [length, feature_dim] 
    """
    pred = predict(model=model, scaler=scaler, x=x, y=y, pred_horizon=55)



    """
    For mulitple simultaneous predictions either:
    
    1. loop through prediciton using current code
    
    OR: 
    
    2. Update predict function with 3D tensors
        x: [length, batch_size, feature_dim] 
    """











def predict_model():
    """
    Load model
    
        1. Choose previously trained model that you want in ml_runs/0/random_run_code/artifacts/
    
        2. Move all folders and files to test_model to run inference

    """

    """
    Loading pre-trained RNN model
    """
    print("Running prediction...")
    scaler_path= 'dagster_ais/ML_data_pipeline/src/ml_models/test_model/scaler'
    model_path = 'dagster_ais/ML_data_pipeline/src/ml_models/test_model/prediction_model'

    scaler = mlflow.sklearn.load_model(scaler_path) #Load scaler
    model = mlflow.pytorch.load_model(model_path) #Load model
    model.eval()

    """
    Trajectory prediciton example 
    """


    """
    Get data
        x: input data (line_geom.xy)
    crs='EPSG:4326'
    MUST INPUT x of length 10 (i.e. 10 minutes/10 rows with one minute intervals)
    
    """
    x = np.ones((10, 2))
    x[:, 0]=x[:,0]*4347501
    x[:, 1]=x[:, 1]*3979015

    y = np.ones((55, 2))
    y[:,0]=y[:,0]*4347501
    y[:,1]=y[:,1]*3979015

    """
    Predict single trajectory
    x-dim: [length, feature_dim] 
    """
    pred = predict(model=model, scaler=scaler, x=x, y=y, pred_horizon=55)
    print(type(pred))

    """
    For mulitple simultaneous predictions either:
    
    1. loop through prediciton using current code
    
    OR: 
    
    2. Update predict function with 3D tensors
        x: [length, batch_size, feature_dim] 
    """