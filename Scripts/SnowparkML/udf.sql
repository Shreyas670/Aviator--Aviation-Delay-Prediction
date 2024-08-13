CREATE OR REPLACE FUNCTION DELAY_DATA.DELAY_SCHEMA.DELAY_PREDICTION("AIRLINE" VARCHAR(16777216),"SOURCE_CITY" VARCHAR(16777216),"DATE" DATE,"HOUR" NUMBER(38,0), "VISIBILITY" NUMBER(38,0),"FORECAST" VARCHAR(16777216),"TEMPERATURE" NUMBER(38,0),"CLOUD_COVER" NUMBER(38,0),"CLOUD_CEILING" NUMBER(38,0),"PRECIPITATION" NUMBER(38,0),"WIND" NUMBER(38,0),"WIND_GUSTS" NUMBER(38,0))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('cachetools==4.2.2','pycaret==3.0.2','snowflake-snowpark-python==*', 'joblib==1.2.0','scikit-learn==1.1.1')
HANDLER = 'main'
IMPORTS = ('@DELAY_DATA.DELAY_SCHEMA.MODEL_STAGE/delay_model.pkl')
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import pandas as pd
from pycaret.classification import *
import os
import sys
from joblib import load
from _snowflake import vectorized
from sklearn.impute import SimpleImputer
@vectorized(input=pd.DataFrame, max_batch_size=1000)
def main(data_df_input: pd.DataFrame):
    data_df_input = data_df_input.rename(columns={
                    0:"AIRLINE",
                    1:"SOURCE_CITY",
                    2:"DATE",
                    3:"HOUR",
                    4:"VISIBILITY",
                    5:"FORECAST",
                    6:"TEMPERATURE",
                    7:"CLOUD_COVER",
                    8:"CLOUD_CEILING",
                    9:"PRECIPITATION",
                    10:"WIND",
                    11:"WIND_GUSTS"
            })
            
    import_dir_name = "snowflake_import_directory"
    import_dir = sys._xoptions[import_dir_name]
    
    model_file = import_dir + ''delay_model.pkl''
    model = load(model_file)
    predictions = predict_model(model, data=data_df_input,raw_score=True)
    # Return value will appear in the Results tab.
    return predictions[''prediction_score_1''].tolist()';


select DELAY_DATA.DELAY_SCHEMA.DELAY_PREDICTION('Vistara','Kolkata',date(2024-08-10),3,5,'Sunny',35,95,1000,25,30,45);

-- 

-- BALANCE_AT_CLOSING NUMBER(38,0),
-- TYPE VARCHAR(16777216),
-- ACCOUNT_DESCRIPTION VARCHAR(16777216),
-- ACCOUNT_TYPE VARCHAR(16777216),
-- AGE FLOAT,
-- ACCOUNT_AGE FLOAT,
-- OD_FREQUENCY FLOAT,
-- RETURNED_CHARGES_RATE FLOAT,
-- TOTAL_TRAN_COUNT NUMBER(38,0),
-- MOST_OCCURING_TRAN_DESCRIPTION VARCHAR(16777216),
-- AVERAGE_TRAN_CREDIT_AMT FLOAT,
-- CNT_CREDIT_TRAN NUMBER(38,0),
-- AVERAGE_TRAN_DEBIT_AMT FLOAT,
-- CNT_DEBIT_TRAN NUMBER(38,0),
-- DAYS_SINCE_MOST_RECENT_TRAN NUMBER(38,0)

SELECT * FROM FIDELITY_CLEANED_V2;