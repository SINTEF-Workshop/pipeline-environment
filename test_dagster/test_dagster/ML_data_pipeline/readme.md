## Python environment
- Generate Python environment:
   - Conda can be found at https://docs.conda.io/en/latest/miniconda.html
   - If **conda** is used:
      1. ``conda env create -f vesselai_clean.yml``
      2. Activate the environment by using the command``conda activate vesselai_clean``
      

## Training 
- The model is stored in the ``simple_model`` folder.

  - This is a simlified model that only uses dynamic positional data
  - ``rnn_pl_model.py`` contains the model files 
  - ``utils.py`` contains additionl funcitons used
  - You must update the ``prepare_data`` function in the  ``RNNDataModule`` class in ``rnn_pl_model.py`` to get your data
    - Current implementaion gets data from Azure PostgreSQL DB
  - The data should be accessbile in the same format as the script I provided Antoine
    - The only data used are the positional line geometries (line_geom.xy)
    - See the query in ``prepare_data`` function

- Run ``src/train.py`` to train simple model


## Prediction/Inference

 - MLFlow stores the trained models in the ``ml_runs/0/`` folder
    
 - Choose the specific previously trained model that you want in ``ml_runs/0/random_run_code/artifacts/``
  
 - Move all folders and files from this folder to ``test_model`` folder to use model for prediciton

 - Run ``predict.py``
     - Runs prediction for single trajectory
     - Input data, x must be of length 10, i.e. as 10 minute input trajectories (i.e. interpolated at 1 minute inputs)
     - x: input data (line_geom.xy)
     - crs = 'EPSG:4326'
 - For mulitple simultaneous predictions either:
    
    1. loop through prediciton using current code
    
    OR: 
    
    2. Update predict function with 3D tensors
        x: [length, batch_size, feature_dim] 
   
