# Sales-Forecasting-Analysis
Accurately developed predictive model that ensures precise forecast of the orders and inventory levels, striking a balance between preventing overstock situations and avoiding out-of-stock scenarios.
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/5c64ad35-8f16-4358-81d1-49be7214dada)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/7a32dad3-2add-4fb9-829a-fc32ad58ccca)

**Step 1:** In Use tables were found out from RaptorAPI codes : Entities - Repos (azure.com)
EDI_INFORMATION
RAPTOR2_ERROR_LOG_CURRENT
FILES
FLOW
RAPTOR2_TRANSACTION
RAPTOR2_TRANSACTION_STAGE
RAPTOR2_TRANSACTION_STATS
RAPTOR_SEARCH_FIELDS
RAPTOR_SEARCH_PROFILE
RAPTOR_TP_DOCUMENT
SENDER_RECEIVER_DOCCODE_MV
VISIBILITY_PARAMS
TRANSACTION


Other tables/view which will be useful for model:
TPM_MASTER
TPM_SNDR_RCVR
TREADING_PARTNER
RAPTOR_TRANSACTION_VIEW
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/e2ca1145-0c91-40d1-afb4-ce65925d7293)
Step 2: Understand the relationship between these Raptor tables
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/5c92fba0-ba73-4c1b-ae22-b128ef69e77c)

**Step 3:** Identify the Method to be followed for building ML
I chose Transaction table from prod environment which has 2022 and 2023 data. For prototype, I will focus on Apple2 customer.
VISIBILITY.TRANSACTION


The Conda Environment is created with Sci-kit learn package and linked it in visual studio code. Then used cx-oracle to connect to Oracle SQL for accessing these tables. 
Then to count orders by hours, I grouped by source_insert_date hour and performed EDA and some visualization to understand the data.
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/18bd288e-de26-4afc-afb9-0c517a7aaf8e)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/e6e04c71-1493-4318-afd1-7759e41e6415)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/c4d0dda6-28f1-43bc-9233-6ab97c83cc23)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/a9eefb88-fb90-4192-a86f-3e8cdd943f04)


**PLAN:**

1)	Looking at the data, we can perform time series forecast using Facebook Prophet including the holidays data. Also, we will try traditional ARIMA/SARIMA models for the forecasting. There is DeepAR+ Algorithm by Amazon which can be used as well to select the best model.
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/9c2fbc4d-9d9f-49ba-9a9d-2c2ea08cb667)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/01e40c72-f7ae-4db5-9cb8-036f21358c29)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/a94a141f-e17e-4a55-ba9a-7867853a825d)
Customer Segmentation
Note for Future: 
If we get error log and error log specific to the flow, we can use that data for predicting maintenance in possible flows.

2)	We could do customer segmentation to identify similar pattern customers and categorize them according to their behavior such as loyal customer, new customer or churned customer using RFM(Recency, Frequency, Monetary).
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/26de1b58-7e4f-4bb3-86fc-d54078307be0)

**Implementation**:
**Objective:**
1)	Identify TP <Sender,Receiver>
2)	Identity Document <DocType>
3)	Identify date <TimeStamp>
4)	Need to get the number of orders expected in one hour window that day. 
5)	Consider Day of the week, Month and Hour.


**Week 1 and Week 2:** Select tables that will have useful information for ML model. Understand the relationship between these Raptor tables. Identify the Method to be followed for building ML
Table was selected and imported from Oracle into the visual studio code for EDA. EDA is done on the table. The grouping transactions hourly and calculating the transactions just for an Apple trading partner. The queried data is visualized after sampling on daily, weekly, monthly and yearly basis to observe the trend and seasonality.
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/f38f5dc4-2fb0-44a6-931b-352c63ab0a37)

**Week 3:** Create Prototype Model on available system and Visualize end results.
The Facebook Prophet model was selected to predict the hourly transactions. The model was passed with the hours and their respective transactions. The accuracy was improved after ingesting more data and configuring the seasonality and holidays into the data
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/94ebea1e-6595-41fe-aef8-60d118c4256e)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/2d74a4fc-0fb1-41f0-94e3-d26f3cbf8db6)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/188039ca-89d2-4c22-93ee-f0bd26ed87dc)

**Week 4:** Select the best model with suitable to the data and the use-case.
Prophet is the adaptive and self-configurable model which will be suitable to all the trading partners and that way there won’t be any need to create separate model for each trading partner. So, I am choosing Prophet. The predictions and error scores will be stored to table in sql oracle which will visualized in power-bi and will scheduled using airflow or the MLflows. 
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/bc024acf-91f5-401b-a90c-f348e51e0be4)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/67a897a4-ac34-4fa7-82e5-38a379af3f5e)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/7c779d4e-ccbc-4f1f-8045-d55d12951683)

**Week 5:** VM Setup: Start migrating the data to Hadoop cluster and use Hive/Spark for retrieval and storing predictions. Integrate the Hadoop, ML flow and visualization in one pipeline.
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/f64ad97c-1828-4189-85c4-2ec23a4711ab)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/04bf72f7-eec8-41bc-978b-93a4fd64929f)

**Week 6 & Week 7:** Transferred Oracle data to MongoDB incrementally and Stream the results on the website using Streamlit
I connected to oracle using cx_oracle connector in python and queried stats and transaction data. That data serially loaded into mongoDB using Spark-MongoDB connector through Apache Spark Dataframe.
To start the mongoDB server, use following command on windows command Prompt with admin privileges: (Use “net stop MongoDB” command in case of lock-error)
mongod –dbpath=E:\Mongodb\data\db
Integrated streamlit commands in the code to display all the results on to webapp publicly. 
Use following command on Anaconda Prompt with admin privileges to start the webapp: 
python -m streamlit run e:/test/Final_Script_for_data_ingestion_nd_prediction.py
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/18aead8e-f542-45dc-b36b-820355429f56)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/5789252d-60d5-498a-8cce-60b368f00de1)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/4ad2ccec-1661-4d5a-adcd-932b9cb4a6c1)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/cf00c3f6-3aa7-4b53-bf33-f479531e14d7)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/b53a9f80-7e27-49c1-8032-b05967914be2)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/aa4f2e62-c6a1-4c83-9de5-fd3983ba535e)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/e8ad3522-c3a2-43f6-aa0b-985a4698eb4b)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/598c9940-8702-4518-af13-afc5ae9093c8)
![image](https://github.com/shivaniRaut/Sales-Forecasting-Analysis/assets/30024267/fd49638e-d4dc-4117-ae46-ef3d06683172)

**Week 8:** Research for productionizing (Next week: 10th July 2023)

