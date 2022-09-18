# Assignment 1 - data-engineering-ids-706
The purpose of the assignment is to get exposure to PySpark through performing some EDA and deriving insights from a "Big Data" dataset. The dataset used here is the Zillow Home Value Index (all housing types at a state level). 

In addition, this assignment will also optimize the setup for code spaces. This will also be used in forthcoming assignments. 

The goal of this analysis to identify states that have mostly likely had a housing bubble. Across the country, house prices fell post the various interest rate hikes by the Federal Reserve. By identifying states where the prices fell the most, we can identify the states that were most likely to have had a housing bubble. 

From the analysis, we can see that the following three states have had the largest drop in house values post the hikes. 

### Steps to run the analysis locally 
1. Download the ZVHI file from https://www.zillow.com/research/data/ where date type="ZVHI All Homes" and geography = "Metro & U.S." 
2. Clone the Repo.
3. Install all libraries from the requirements.txt file
4. Update the zhvi_data_loc on #38. 
5. Run zhvi_eda.py



### To Do 
#### Analysis 
1. Adjust for inflation to get more accurate changes in price. This is important given the huge fluctuations in inflation since Covid (March-2020). 

2. Include additional data sources like AirBnB rental data and House Rent data to get a more well-rounded picture. 


##### Programming 
1. Replace numpy and list comprehension operations with PySpark operations. 

2. Utilize multi-core processing in PySpark to speed up the process. 
