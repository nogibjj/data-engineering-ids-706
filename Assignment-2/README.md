
In this assignment, the goal is to write a bash script to perform Data Cleaning and ETL operations. 

<ins>Steps to setup Azure Storage and the linux packages:</ins>
1. Download the azcopy linux package from https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json and run the following commands (change the 3rd command to your personal directory)
    - wget https://aka.ms/downloadazcopy-v10-linux
    - sudo tar -xvf downloadazcopy-v10-linux
    - sudo cp /workspaces/data-engineering-ids-706/azcopy_linux_amd64_10.16.0/azcopy /usr/bin/
2. Verify azcopy is installed using "sudo azcopy --help"
3. Create a new storage account resource from the azure home page. 
4. After the storage resource has been created, navigate to containers (under data storage) and create a new container. 
5. Navigate inside the container, choose the required folder and choose Shared access token and copy the token. Ensure you change the following 
    - Change the required permissions (default is Read only).
    - Increase the default expiry time if required (default is 8 hours)
6. Using the "export" command, set a variable to use the connection string (Remember to not add hardcode it in the code while pushing to git.)
    - export AZURE_CONNECTION_TOKEN='<<Paste Token Here>>'