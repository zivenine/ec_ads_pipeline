This project extracts marketing data from Google Analytics 4, Facebook Ads and Google Ads, then sends it to BigQuery so that it can be pulled into Looker Studio and reported on. 

**main.py** - this script is scheduled to run on the 2nd of each month as a Cloud Function.

1. Access Facebook app secret, token, and Google Cloud project's service account. 
2. Extract the following monthly data from GA4, store the data as a JSON file, and send it to BigQuery:
   
   a. Total users
   
   b. Active users
   
   c. Add to Carts
   
   d. Checkouts
   
   e. Transactions
   
   f. Conversions
   
   g. User Conversion rate
   
   h. Total revenue
   
   i. Date
   
   j. Session Source
   
   k. Session Medium
   
3. Extract the following monthly data from Facebook Ads, store the data as a dataframe, and send it to BigQuery:
   
   a. Date
   
   b. Account ID
   
   c. Campaign
   
   d. Adset
   
   e. Ad
   
   f. Impressions
   
   g. Clicks
   
   h. CTR
   
   i. CPC
   
   j. Conversions
   
   k. Spend
   


**In BigQuery** 

Copy the data from the monthly tables and append it to storage tables to be queried. 


**Looker Studio**

The data is pulled from BigQuery into Looker Studio so that it can be reviewed and downloaded for further analysis. 
![image](https://github.com/zivenine/ec_ads_pipeline/assets/138107601/e8e0ed09-cf48-4ab4-bff1-2934cda49f22)

![image](https://github.com/zivenine/ec_ads_pipeline/assets/138107601/60b484aa-9209-433c-b8c2-31c4ea436312)

![image](https://github.com/zivenine/ec_ads_pipeline/assets/138107601/049a56e8-0cc7-48f7-aecc-a59dee48aa71)

