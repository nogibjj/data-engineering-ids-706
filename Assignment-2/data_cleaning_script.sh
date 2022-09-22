
#!/usr/bin/env bash

read -p "Enter the name of the file you want to clean: " full_file_name

# Check if the file exists
if [ -f "$full_file_name" ]; then
    echo "File exists"
else
    echo "File does not exist"
    exit 1
fi

# Select top n rows
read -p "Enter the number of rows you want to select: " n
IFS='.' read -ra file_name <<< "$full_file_name"
echo $file_name
head -n $n $full_file_name > "${file_name}_processed.csv"