# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
import pandas as pd
#from datetime import datetime
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import OneHotEncoder

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    acc_table = 'DELAY_DATA.DELAY_SCHEMA.DELAY_DATA_TABLE'
    df = session.table(acc_table).to_pandas()
    NUMERICAL_COLS = ['HOUR' ,'TEMPERATURE' ,
	'CLOUD_COVER' ,
	'CLOUD_CEILING' ,
	'VISIBILITY' ,
	'PRECIPITATION' ,
	'WIND' ,
	'WIND_GUSTS' ,
	'IS_DELAYED']

    
    imputer = SimpleImputer(strategy= 'mean')      
    imputer.fit(df[NUMERICAL_COLS])
    df[NUMERICAL_COLS] = (imputer.transform(df[NUMERICAL_COLS])).round(0)

    columns_to_convert = ['HOUR' ,'TEMPERATURE' ,
	'CLOUD_COVER' ,
	'CLOUD_CEILING' ,
	'VISIBILITY' ,
	'PRECIPITATION' ,
	'WIND' ,
	'WIND_GUSTS' ,
	'IS_DELAYED']
    df[columns_to_convert] = df[columns_to_convert].astype(int)

    
    
    '''''scaler = MinMaxScaler()
    scaler.fit(df[NUMERICAL_COLS])
    df[NUMERICAL_COLS] = scaler.transform(df[NUMERICAL_COLS])'''


    
    '''''encoder = OneHotEncoder(handle_unknown = 'ignore', sparse = False)
    encoder.fit(df[CATEGORICAL_COLS])
    ENCODED_COLS = list(encoder.get_feature_names_out(CATEGORICAL_COLS))
    df[ENCODED_COLS] = encoder.transform(df[CATEGORICAL_COLS])'''''

 

    # Moving Closed Column to last
    column_to_move = 'IS_DELAYED'
    other_columns = [col for col in df.columns if col != column_to_move]
    new_column_order = other_columns + [column_to_move]
    df = df[new_column_order]
    
    df = session.create_dataframe(df)
    df.write.mode("overwrite").save_as_table("DELAY_DATA_CLEAN")

    return "Preprocessing Done"


    
    
  
    
    