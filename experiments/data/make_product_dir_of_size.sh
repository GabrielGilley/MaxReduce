#!/bin/bash

s=$1 # size in gb

# Directory containing the files
DIRECTORY="product_engagement_dataset/pando_files"
# Target size in bytes (200 GB)
TARGET_SIZE=$(($s * 1024 * 1024 * 1024))

# Initialize variables
total_size=0
selected_files=()





# Get a list of files in the directory and shuffle them
mapfile -t files < <(find "$DIRECTORY" -type f | shuf)


for i in $(seq 1 100); do
    # Construct the file names for product and user tables
    product_file="${DIRECTORY}/product_table.${i}.txt"
    user_file="${DIRECTORY}/user_table.${i}.txt"

    # Check if both files exist
    if [[ -f "$product_file" && -f "$user_file" ]]; then
        # Get the sizes of the current files
        product_size=$(stat -c%s "$product_file")
        user_size=$(stat -c%s "$user_file")
        
        # Calculate the combined size
        combined_size=$((product_size + user_size))

        # Check if adding this pair exceeds the target size
        if (( total_size + combined_size > TARGET_SIZE )); then
            break
        fi

        # Add the files to the selected list and update the total size
        selected_files+=("$product_file" "$user_file")
        total_size=$((total_size + combined_size))
    fi
done

# Output the selected files and their total size
echo "Selected files (total size: $total_size bytes):"
out_dir="${s}G"
mkdir "$out_dir"
for file in "${selected_files[@]}"; do
    echo "$file"
    ln "$file" "$out_dir"
done

echo "Total size in GB: $(echo "scale=2; $total_size / 1024 / 1024 / 1024" | bc)"

