'''
filename: utils.py
functions: encode_features, get_train_model
creator: shashank.gupta
version: 1
'''

###############################################################################
# Import necessary modules
# ##############################################################################

import pandas as pd
import numpy as np

import sqlite3
from sqlite3 import Error

import mlflow
import mlflow.sklearn

from datetime import datetime
from datetime import date

from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.metrics import precision_score, recall_score
from sklearn.metrics import precision_recall_fscore_support
from sklearn.metrics import f1_score
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix

from Lead_scoring_training_pipeline.constants import *


#helper function
def check_if_table_has_value(cnx, table_name):
    # cnx = sqlite3.connect(db_path+db_file_name)
    check_table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';", cnx).shape[0]
    if check_table == 1:
        return True
    else:
        return False
    
    
def create_sqlit_connection(db_path,db_file):
    """ create a database connection to a SQLite database """
    conn = None
    # opening the conncetion for creating the sqlite db
    try:
        conn = sqlite3.connect(db_path+db_file)
        print(sqlite3.version)
    # return an error if connection not established
    except Error as e:
        print(e)
    # closing the connection once the database is created
    finally:
        if conn:
            conn.close()

def create_mlflow_experiment():
    experiment_name = EXPERIMENT
    
    mlflow.set_tracking_uri(TRACKING_URI)

    try:
        # Creating an experiment
        print("Creating mlflow experiment with name: ", experiment_name)
        mlflow.create_experiment(experiment_name)
    except:
        pass

    # Setting the environment with the created experiment
    print("Setting mlflow experiment to name: ", experiment_name)
    mlflow.set_experiment(experiment_name)


###############################################################################
# Define the function to encode features
# ##############################################################################

def encode_features():
    '''
    This function one hot encodes the categorical features present in our  
    training dataset. This encoding is needed for feeding categorical data 
    to many scikit-learn models.

    INPUTS
        db_file_name : Name of the database file 
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
       

    OUTPUT
        1. Save the encoded features in a table - features
        2. Save the target variable in a separate table - target


    SAMPLE USAGE
        encode_features()
        
    **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline from the pre-requisite module for this.
    '''
    
    cnx = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    print("Loading model_input table")
    df = pd.read_sql('select * from model_input', cnx)
    print("Table model_input columns: ", df.columns)
    print("Converting 'city_tier' column from float to category in model_input dataframe")
    df['city_tier'] = df.city_tier.astype('category')

    print("One hot encoding features")
   # Implement these steps to prevent dimension mismatch during inference
    encoded_df = pd.DataFrame(columns= ONE_HOT_ENCODED_FEATURES) # from constants.py
    placeholder_df = pd.DataFrame()

    # One-Hot Encoding using get_dummies for the specified categorical features
    for f in FEATURES_TO_ENCODE:
        if(f in df.columns):
            encoded = pd.get_dummies(df[f])
            encoded = encoded.add_prefix(f + '_')
            placeholder_df = pd.concat([placeholder_df, encoded], axis=1)
        else:
            print(f + ',Feature not found')

    # Implement these steps to prevent dimension mismatch during inference
    for feature in encoded_df.columns:
        if feature in df.columns:
            encoded_df[feature] = df[feature]
        if feature in placeholder_df.columns:
            encoded_df[feature] = placeholder_df[feature]

    encoded_df.fillna(0, inplace=True)
    print("Encoded dataframe columns: ", encoded_df.columns)

    target = df[['app_complete_flag']]
    print("Shape of target dataframe:", target.shape) 
    print("Storing target features to 'target' table")            
    target.to_sql(name='target', con=cnx, if_exists='replace', index=False)

    print("Shape of feature dataframe:", encoded_df.shape) 
    print("Storing rest of features to 'feature' table")            
    encoded_df.to_sql(name='features', con=cnx, if_exists='replace', index=False)   
    cnx.close()


###############################################################################
# Define the function to train the model
# ##############################################################################

def get_trained_model():
    '''
    This function setups mlflow experiment to track the run of the training pipeline. It 
    also trains the model based on the features created in the previous function and 
    logs the train model into mlflow model registry for prediction. The input dataset is split
    into train and test data and the auc score calculated on the test data and
    recorded as a metric in mlflow run.   

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be


    OUTPUT
        Tracks the run in experiment named 'Lead_Scoring_Training_Pipeline'
        Logs the trained model into mlflow model registry with name 'LightGBM'
        Logs the metrics and parameters into mlflow run
        Calculate auc from the test data and log into mlflow run  

    SAMPLE USAGE
        get_trained_model()
    '''
    print("Set MLflow tracking url and create/set experiment")
    create_mlflow_experiment()
    
    cnx = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    
    print("Loading 'features' table")
    X = pd.read_sql('select * from features', cnx)        

    print("Loading 'target' table")
    y = pd.read_sql('select * from target', cnx)        

    print("Splitting data into train and test")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 100)

    #Model Training

    #make sure to run mlflow server before this. 

    run_name = EXPERIMENT + '_' + date.today().strftime("%d_%m_%Y_%H_%M_%S")

    with mlflow.start_run(run_name=run_name) as run:

        #Model Training
        print("Instantiated LGBMClassifier")
        clf = lgb.LGBMClassifier()
        print("Setting model configurations: ", model_config)
        clf.set_params(**model_config)
        print("Starting LGBMClassifier model training")
        clf.fit(X_train, y_train)

        print("Logging model to mlflow with name as LightGBM")
        mlflow.sklearn.log_model(sk_model=clf,artifact_path="models", registered_model_name='LightGBM')
        print("Logging model params in mlflow")
        mlflow.log_params(model_config)    

        # predict the results on training dataset
        print("Prediction on test data")
        y_pred=clf.predict(X_test)

        #Log metrics
        print("Calculating Metrics scores on test data")
        acc=accuracy_score(y_pred, y_test)
        precision = precision_score(y_pred, y_test,average= 'macro')
        recall = recall_score(y_pred, y_test, average= 'macro')
        f1 = f1_score(y_pred, y_test, average='macro')
        auc = roc_auc_score(y_pred, y_test, average='weighted', multi_class='ovr')
        cm = confusion_matrix(y_test, y_pred)
        tn = cm[0][0]
        fn = cm[1][0]
        tp = cm[1][1]
        fp = cm[0][1]

        print("Accuracy=", acc)
        print("Precision=", precision)
        print("Recall=", recall)
        print("AUC=", auc)
        print("F1 Score=", f1)

        print("Logging metrics scores in MLflow")
        mlflow.log_metric('test_accuracy', acc)            
        mlflow.log_metric("Precision", precision)
        mlflow.log_metric("Recall", recall)
        mlflow.log_metric("f1", f1)           
        mlflow.log_metric("AUC", auc)          
        mlflow.log_metric("True Negative", tn)            
        mlflow.log_metric("False Negative", fn)
        mlflow.log_metric("True Positive", tp)  
        mlflow.log_metric("False Positive", fp)


        runID = run.info.run_uuid
        print("Inside MLflow Run with id {}".format(runID))
            
    print("Closing database connection")        
    cnx.close()

   
