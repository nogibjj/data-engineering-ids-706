
#!/usr/bin/env bash

read -p "Enter the name of the file you want to clean: " full_file_name

# Check if the file exists
if [ ! -f "$full_file_name" ]; then
    echo "File does not exist"
    exit 1
fi

# Select top n rows
read -p "Enter the number of rows you want to select: " n
# check in n is greater than 0
if [ $n -gt 0 ]; then
    IFS='.' read -a file_name <<< "$full_file_name"
    head -n $n $full_file_name > "${file_name[0]}_processed.csv"
    echo "File created: ${file_name[0]}_processed.csv"
fi

sudo azcopy copy "${file_name[0]}_processed.csv" "$AZURE_CONNECTION_STRING"