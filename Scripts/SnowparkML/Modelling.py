# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
import pandas as pd
from datetime import datetime
from pycaret.classification import *
import os,sys
from sklearn.model_selection import train_test_split

def main(session: snowpark.Session):
    merged_table = 'CONSUMPTION.DELAY_DATA.FLIGHTS_DELAY_CLEAN'
    merged_df = session.table(merged_table).to_pandas()
    
    merged_df_train, merged_df_test = train_test_split(merged_df, test_size=0.01, stratify=merged_df['IS_DELAYED'])
    #X = merged_df_train.drop('ACCOUNT_NUMBER',axis=1)
    #y = merged_df_train['ACCOUNT_NUMBER']
    data= setup(merged_df,target='IS_DELAYED', train_size = 0.8,fold=5,
           categorical_features=['FORECAST', 'AIRLINE', 'SOURCE_CITY'],
           data_split_stratify=True,
          fix_imbalance=True,fix_imbalance_method = 'ADASYN', session_id=1,remove_outliers=True,
                #feature_selection=True,
                #imputation_type='simple',
                # keep_features=['TENURE','PREFERREDLOGINDEVICE','CITYTIER','WAREHOUSETOHOME','PREFERREDPAYMENTMODE','GENDER',
                #                'HOURSPENDONAPP','NUMBEROFDEVICEREGISTERED','PREFEREDORDERCAT','SATISFACTIONSCORE','MARITALSTATUS',
                #                'NUMBEROFADDRESS','COMPLAIN','ORDERAMOUNTHIKEFROMLASTYEAR','COUPONUSED','ORDERCOUNT',
                #                'DAYSINCELASTORDER','CASHBACKAMOUNT'],
         ignore_features=['ROW_ID','DATE','TIME'])
    top_models = compare_models(sort='AUC')
    
    train_metrics = pull()
    train_metrics_sp = session.create_dataframe(train_metrics)
    train_metrics_sp.write.mode("overwrite").save_as_table("DELAY_MODEL_RESULTS")

    pred = predict_model(top_models)
    test_metrics = pull()
    test_metrics_sp = session.create_dataframe(test_metrics)
    test_metrics_sp.write.mode("overwrite").save_as_table("DELAY_MODEL_RESULTS_TEST")

    
    
    merged_df_test_sp = session.create_dataframe(merged_df_test)
    merged_df_test_sp.write.mode("overwrite").save_as_table("DELAY_TEST_DATA")

    

    
    import_dir = sys._xoptions.get("snowflake_import_directory")
    save_model(top_models, os.path.join(import_dir, '/tmp/delay_model'))

    session.file.put(os.path.join(import_dir, '/tmp/delay_model.pkl'),
    "@MODEL_STAGE",
    auto_compress=False,
    overwrite=True
    )

    plot_model(top_models, save=os.path.join(import_dir, "/tmp"),plot = 'feature')
    session.file.put(
        os.path.join(import_dir, "/tmp/Feature Importance.png"),
        "@MODEL_STAGE",
        auto_compress=False,
        overwrite=True
    )

    
    return "Final model saved successfully"
    
   
