# %%
import cx_Oracle
import pandas as pd
import findspark
import datetime
findspark.init()
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
import pandas as pd
import cx_Oracle
spark = SparkSession.builder.appName("Data_Agg_Migration_Prediction").getOrCreate()

# %%
df = spark.read.format("mongodb").option("database","raptor").option("collection", "transaction_stats").load()
cmp_dt = datetime.strftime(df.agg({"ds":"max"}).first()[0],"%y-%b-%d %H:%M:%S")[:12]
print(cmp_dt)
#jdbc:oracle:thin:@alintvisp:1521:intvisp
dsn = cx_Oracle.makedsn("alintvisp", 1521, sid="intvisp")
connection = cx_Oracle.connect(user="sraut", password='Ash#1234pinkdrap', dsn=dsn, encoding="UTF-8")
# #Connect with Oracle database
#df1=spark.read.format("jdbc").option("url","jdbc:oracle:thin:@alintvisp:1521:intvisp").option("dbtable",query).option("user","sraut").option("password","Ash#1234pinkdrap").option("driver","oracle.jdbc.driver.OracleDriver").load()
query = """WITH temp_view AS ( SELECT sender, TO_DATE(to_char(source_insert_date, 'yy-MON-dd HH24'), 'yy-MON-dd HH24') AS ds FROM visibility.transaction WHERE sender IN ( '060704780000700', '060704780500')    AND doc_code = '940' AND source_insert_date>TO_DATE('{}', 'yy-MON-dd HH24')) SELECT sender, ds, COUNT(ds) AS orders FROM temp_view GROUP BY sender, ds""".format(cmp_dt)
df4 = pd.read_sql(query, con=connection)
#see data frame
df4.head()
df4 = spark.createDataFrame(df4)
df4.write.format("mongodb").mode("append").option("database","raptor").option("collection", "transaction_stats").save()

# %%
df = spark.read.format("mongodb").option("database","raptor").option("collection", "transaction").load()
cmp_dt = datetime.strftime(df.agg({"source_insert_date":"max"}).first()[0],"%y-%b-%d %H:%M:%S")
print(cmp_dt)
#Connect with Oracle database
query = "(SELECT * from visibility.raptor2_transaction WHERE source_insert_date>TO_DATE('{}', 'yy-MON-dd HH24:MI:SS')) t".format(cmp_dt)
df1=spark.read.format("jdbc").option("url","jdbc:oracle:thin:@alintvisp:1521:intvisp").option("dbtable",query).option("user","sraut").option("password","Ash#1234pinkdrap").option("driver","oracle.jdbc.driver.OracleDriver").load()
#see data frame
df1.write.format("mongodb").mode("append").option("database","raptor").option("collection", "transaction").save()

# %%
import streamlit as st
st.title('Orders Predictions for Apple')
import numpy as np
import pandas as pd, datetime
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn import metrics
import matplotlib.pyplot as plt
from matplotlib import pyplot
from time import time
from datetime import datetime
import os
from math import sqrt
import itertools
from sklearn import model_selection
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from pandas import DataFrame
from prophet import Prophet
import prophet
import warnings
warnings.filterwarnings('ignore')
from prophet.plot import plot_plotly, plot_components_plotly
from prophet.plot import add_changepoints_to_plot
from prophet.plot import plot_yearly
from prophet.diagnostics import cross_validation
from prophet.diagnostics import performance_metrics
from prophet.plot import plot_cross_validation_metric
from prophet.serialize import model_to_json, model_from_json
# Multi-processing
from multiprocessing import Pool, cpu_count
# Spark
import findspark
import datetime
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, days, hour, date_format
#spark.stop()
# Process bar
from tqdm import tqdm
# Tracking time
from time import time
import matplotlib.pyplot as plt
import plotly
import plotly.tools as tls
from plotly.offline import download_plotlyjs, init_notebook_mode, iplot
import plotly.graph_objects as go

# %%
spark = SparkSession.builder.appName("Prophet_Preditction").getOrCreate()
df = spark.read.format("mongodb").option("database","raptor").option("collection", "transaction_stats").load()
df = df.select("DS","ORDERS","SENDER")
df = df.drop_duplicates()
df.show(5)

# %%
df = df.withColumn("year", year("DS"))
df = df.withColumn("day_in_week", dayofweek("DS"))
df = df.withColumn("day_in_mon", dayofmonth("DS"))
df = df.withColumn("mon", month("DS"))
df = df.withColumn("hour", hour("DS"))
df = df.withColumn('day', date_format('DS', 'EEEE'))
df_pd = df.toPandas()

# %%
# Split the data into train and test sets
#train, test = train_test_split(df, test_size=0.20)
train = df_pd
train['ORDERS'] = train['ORDERS'] * 1.0
# Creating a train dataset
train_prophet = train.copy()
#train_prophet.reset_index(level=0, inplace=True)
# Converting col names to specific names as required by Prophet library
train_prophet = train_prophet.rename(columns = {'DS': 'ds',
                                'ORDERS': 'y'})
# Downsampling to week because modelling on daily basis takes a lot of time
#ts_week_prophet= train_prophet.set_index("ds").resample("H").sum()
train_prophet["SENDER"] = "Apple"
train_prophet = train_prophet.sort_values("ds", ascending=False)
train_prophet = train_prophet.rename(columns={"SENDER":"TP_NAME"})
train_prophet.head()

# %%
# Group the data by ticker
groups_by_tp = train_prophet.groupby('TP_NAME')
tp_list = list(groups_by_tp.groups.keys())
def warm_start_params(m):
    """
    Retrieve parameters from a trained model in the format used to initialize a new Stan model.
    Note that the new Stan model must have these same settings:
        n_changepoints, seasonality features, mcmc sampling
    for the retrieved parameters to be valid for the new model.

    Parameters
    ----------
    m: A trained model of the Prophet class.

    Returns
    -------
    A Dictionary containing retrieved parameters of m.
    """
    res = {}
    for pname in ['k', 'm', 'sigma_obs']:
        if m.mcmc_samples == 0:
            res[pname] = m.params[pname][0][0]
        else:
            res[pname] = np.mean(m.params[pname])
    for pname in ['delta', 'beta']:
        if m.mcmc_samples == 0:
            res[pname] = m.params[pname][0]
        else:
            res[pname] = np.mean(m.params[pname], axis=0)
    return res
def train_and_forecast(group):
    # Initiate the model
    # Fitting data to Prophet model
    #from holidays import holidayss
    #================================================================================ Incremental model training : use when heavy data
    # with open('serialized_model_'+group['TP_NAME'].iloc[0]+'.json', 'r') as fin:
    #     prophet_0 = model_from_json(fin.read())  # Load model
    #================================================================================ 
    # group['floor'] = 0
    # group['cap'] = 1000+group['y'].max()
    prophet_1 = Prophet( seasonality_prior_scale= 0.3, changepoint_prior_scale=0.01, daily_seasonality='auto', weekly_seasonality='auto', yearly_seasonality='auto', seasonality_mode='multiplicative', interval_width = 0.70)
    #m = Prophet() 
    prophet_1.add_country_holidays(country_name='US')
    prophet_1.add_seasonality(name='daily', period=1, fourier_order=10)
    prophet_1.add_seasonality(name='monthly', period=30.5, fourier_order=10)
    #prophet_1.add_seasonality(name='quarterly', period=91.3, fourier_order=10)
    prophet_1.add_seasonality(name='weekly', period=7, fourier_order=10)
    prophet_1.add_seasonality(name='yearly', period=365, fourier_order=10)
    # Fit the model
    #================================================================================ Incremental model training : use when heavy data
    #prophet_1.fit(group,init=warm_start_params(prophet_0))
    #================================================================================ 
    prophet_1.fit(group)
    with open('serialized_model_'+group['TP_NAME'].iloc[0]+'.json', 'w') as fout:
        fout.write(model_to_json(prophet_1))  # Save model
    #df_cv = cross_validation(prophet_1, initial='365 days', period='30 days', horizon = '90 days')
    #print(performance_metrics(df_cv)) 
    #fig = plot_cross_validation_metric(df_cv, metric='rmse')
    future_1 = prophet_1.make_future_dataframe(periods = 500, freq = "H") 
    # future_1['floor'] = 0
    # future_1['cap'] = 1000+group['y'].max()
    forecast_1 = prophet_1.predict(future_1)
    # Make predictions
    forecast_1['TP_NAME'] = group['TP_NAME'].iloc[0]
    # Plot components
    fig = prophet_1.plot_components(forecast_1)
    # Return the forecasted results
    return forecast_1[['ds', 'TP_NAME', 'yhat', 'yhat_upper', 'yhat_lower']]

# %%
# Start time
start_time = time()
# Create an empty dataframe
for_loop_forecast = pd.DataFrame()
# Loop through each ticker
for tp in tp_list:
    print("##################### Trading Partner ", tp,"######################")
    # Get the data for the ticker
    group = groups_by_tp.get_group(tp)  
    # Make forecast
    forecast = train_and_forecast(group)
    # Add the forecast results to the dataframe
    for_loop_forecast = pd.concat((for_loop_forecast, forecast))
print('The time used for the for-loop forecast is ', time()-start_time)
# Take a look at the data
for_loop_forecast.head()

# %%
for_loop_forecast.loc[for_loop_forecast["yhat"]<0, 'yhat'] = 0
for_loop_forecast.loc[for_loop_forecast["yhat_upper"]<0, 'yhat_upper'] = 0
for_loop_forecast.loc[for_loop_forecast["yhat_lower"]<0, 'yhat_lower'] = 0
df_cv = for_loop_forecast.sort_values('ds',ascending=False)
df_final = pd.merge(train_prophet,df_cv, how="inner", on=["ds","TP_NAME"])
df_final["error"] = (df_final["y"].abs()-df_final["yhat"]).abs()
df_final = df_final[["TP_NAME","ds","y","yhat","yhat_upper", "yhat_lower"]]
df_final.sort_values('ds',ascending=False).head()

# %%
df_pred = spark.createDataFrame(df_final.head(1000))
df_pred.write.format("mongodb").mode("overwrite").option("database","raptor").option("collection", "transaction_orders_predictions").save()
df_final.head(1000).to_csv('Apple_orders_predictions.csv',index=False)

# %%
df_latest = df_final.sort_values('ds',ascending=False).head(24)
fig, axes = plt.subplots(nrows = 1, ncols=2, figsize = (25,5))
#df_latest.plot(x = 'ds', kind = 'bar', title = "Apple", ax = axes[0], colormap='Spectral')
df_latest[df_latest["TP_NAME"]=='Apple'][["TP_NAME","ds","y","yhat"]].plot(x = 'ds', ax = axes[0], kind = 'bar')
df_latest[df_latest["TP_NAME"]=='Apple'][["TP_NAME","ds","y","yhat"]].plot(x = 'ds', ax = axes[1], kind = 'line')
# sns.barplot(, x = 'ds', ax = axes[0])
# sns.barplot(df_latest[df_latest["TP_NAME"]=='Starbucks'], x = 'ds', ax = axes[1])

# %%
fr_dt = datetime.datetime.strftime((datetime.datetime.today()-datetime.timedelta(100)), "%Y-%m-%d %H:%M:%S")
to_dt = datetime.datetime.strftime((datetime.datetime.today()+datetime.timedelta(100)), "%Y-%m-%d %H:%M:%S")
df_new_n_old = pd.merge(for_loop_forecast,train_prophet, how="left", on=["ds","TP_NAME"])
df_new_n_old = df_new_n_old.fillna(0).sort_values('ds',ascending=False)
df_new_n_old = df_new_n_old[["ds","TP_NAME","yhat","yhat_upper","yhat_lower","y"]]
df_new_n_old = df_new_n_old.sort_values('ds',ascending=False)
df_plt = df_new_n_old[df_new_n_old["TP_NAME"]=='Apple'][(df_new_n_old['ds']>=fr_dt)  & (df_new_n_old['ds']<=to_dt)].sort_values('ds')
df_plt

# %%
df_apple = df_final[df_final["TP_NAME"]=='Apple']
df_starbuck = df_final[df_final["TP_NAME"]=='Starbucks']
print("***RMSE Apple*** :",  mean_squared_error(df_apple.y, df_apple.yhat, squared=False))
print("***MAE Apple*** :",  mean_absolute_error(df_apple.y, df_apple.yhat))
r2_score(df_apple.y, df_apple.yhat)

# %%
df_plt.loc[df_plt["y"]>0, 'yhat_upper'] = 0
fig, ax = plt.subplots(figsize=(25, 5))
plt.grid(color='gray',  linestyle=':', linewidth=0.3)
#plt.fill_between(df_plt['ds'], df_plt['yhat_upper'], 0)#, alpha=0.2, color='orange', where=df_plt['y']<1, label='Scope')
ax.plot(df_plt['ds'], df_plt['y'], label='Real')
ax.plot(df_plt['ds'], df_plt['yhat'], label='Predicted')
ax.plot()
plt.legend()
plotly_fig = tls.mpl_to_plotly(fig)
plotly_fig.add_trace(go.Scatter(x=df_plt['ds'], y=df_plt['yhat_upper'], fill='tozeroy', line_color='rgb(255, 150, 50)')) # fill down to xaxis
plotly_fig.update_layout(title='Raptor Transactions Predictions for Apple')
iplot(plotly_fig)
plotly.offline.plot(plotly_fig, filename="Raptor Transactions Predictions for Apple")


