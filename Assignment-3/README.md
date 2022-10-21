### Setup kaggle API 
1. Create the JSON account and store it /home/vscode/.kaggle
2. Change the permissions chmod 600 /home/vscode/.kaggle/kaggle.json so that the API key is not readable. 
3. Download the required datasets "kaggle datasets download <<dataset_url>>. For example, "kaggle datasets download jacksoncrow/stock-market-dataset"
4. Unzip the file and note down the location. 

### Using Azure SQL 
1. Create a resource SQL Instance resource on the Azure Portal. 
2. Note down all the identification information like server name, database name, user id and password. (if not, this information is available in the connection strings tab)
3. Using the pyodbc library you can connect to the server instance. 