# %%
# Check if libraries are satisfied
# pip install snowflake-snowpark-python[pandas] 


# %%
# Import all libraries
from snowflake.snowpark import Session
from snowflake.snowpark.functions import * 
from snowflake.snowpark.types import *
from sqlalchemy.engine import create_engine 
import pandas as pd

# %%
# Postgres configuration
pg_dbname = 'defaultdb'
pg_user = 'doadmin'
pg_host = 'db-postgresql-nyc1-45157-do-user-8304997-0.b.db.ondigitalocean.com'
pg_pwd = 'AVNS_ZcJYYS7gzye8fP8vpbf'
pg_port = '25060'

# %%
# Create Postgres
pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_dbname}')


# %%
# Query orders and orders_detail as order_details
query_order_details = """
            select 
                o.order_id, 
                o.order_date,
                od.unit_price,
                od.quantity,
                od.discount 
            from 
                defaultdb.public.orders o 
            inner join
                defaultdb.public.order_details od 
                on o.order_id  = od.order_id 
        """

# %%
# Read PostgresSQL 
df = pd.read_sql(query_order_details,pg_engine)

# %%
# Snowspark Configuration
connection_parameters={
                        'user' : 'company2',
                        'password' : 'Password*1',
                        'account' : 'ie97047.ap-southeast-1',
                        'role' : 'ACCOUNTADMIN',
                        'warehouse' : 'COMPUTE_WH',
                        'database' : 'PROJECT_5',
                        'schema' : 'PUBLIC',
}

# %%
# Create Session
session = Session.builder.configs(connection_parameters).create()

# %%
# Set all columns to be uppercase to fit snowflake format
df.columns = df.columns.str.upper()

# %%
# Convert pandas dataframe to snowpark dataframe
df_spark= session.createDataFrame(df)

# %%
# Write as table in snowflake cloud
df_spark.write.mode('overwrite').saveAsTable('aditya_order_details')

# %%
# Close Session
session.close()

