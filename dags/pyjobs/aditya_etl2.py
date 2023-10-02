# %%
# Check if libraries are satisfied
# !pip install snowflake-snowpark-python[pandas]

# %%
# Import all libraries
from snowflake.snowpark import Session
from snowflake.snowpark.functions import * 
from snowflake.snowpark.types import *

# %%
# Snowflake configuration
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
# Create session
session = Session.builder.configs(connection_parameters).create()

# %%
# Read table from Snowflake datawarehouse
df = session.read.table(name = 'aditya_order_details')

# %%
# Daily gross_revenue aggregation
daily_gross_revenue_df = df.withColumn(
                                        "gross_revenue",
                                        expr('unit_price * quantity * (1 - discount)')
                                        )\
                                .groupBy(('order_date'))\
                                .agg(sum('gross_revenue').alias('gross_revenue'))


# %%
# Save as table in snowflake data mart
daily_gross_revenue_df.write.mode('overwrite').saveAsTable('aditya_gross_revenue')

# %%
# Close the session
session.close()


