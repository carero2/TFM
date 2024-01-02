#!/usr/bin/env python3

import csv
import math
import os
import pandas as pd

# Function to read a CSV file containing key-value pairs and convert it to a dictionary
def read_dict_csv_tuple(file_path):
    result_dict = {}
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for i, row in enumerate(reader):
            key = eval(row[0])
            value = int(row[1])
            result_dict[key] = value
    return result_dict

# Function to update dictionaries with values from another dictionary
def update_dicts(d1, d2):
    for key in d2.keys():
        if key not in d1:
            d1[key] = d2[key]
        else:
            d1[key] = d1[key] + d2[key]
    return d1

# Function to calculate Pointwise Mutual Information (PMI) for given values and words' frequencies
def calculate_PMI(value, i_word1, i_word2):
    word1 = vocab_id[i_word1]
    word2 = vocab_id[i_word2]
    freq_word1 = int(vocab_dict[word1])
    freq_word2 = int(vocab_dict[word2])
    PMI = math.log2((value/len(vocab_id))/((freq_word1/len(vocab_id))*(freq_word2/len(vocab_id))))
    return PMI

# List all files in the "cooccurrence_info" folder
files = os.listdir("./cooccurrence_info/")
print(files)

# Initialize an empty dictionary to store merged co-occurrence information
merged_dicts = {}

# Loop through each file in the "cooccurrence_info" folder
for file in files:
    # Read co-occurrence information from the current file
    d = read_dict_csv_tuple("./cooccurrence_info/"+file)

    # Update the merged dictionary with the current co-occurrence information
    merged_dicts = update_dicts(merged_dicts, d)
    
    # Clear the current dictionary for the next iteration
    d.clear()
    print("Dict loaded:", file)

# Read vocabulary information from the "vocab_info.csv" file
vocab = pd.read_csv("./vocab_info.csv", header=None)
vocab_dict = vocab[:100000].set_index(0).to_dict()[1]
print("Vocab loaded.")

# Delete the vocabulary DataFrame to free up memory
del vocab

# Create a mapping of index to vocabulary word
vocab_id = {idx + 1: key for idx, key in enumerate(vocab_dict)}

# Write the final co-occurrence information to a CSV file
with open('final_cooccurrence.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    for key in merged_dicts:
        # Check if both words in the key are present in the vocabulary mapping
        if key[0] in vocab_id and key[1] in vocab_id:
            # Calculate PMI and write the line to the CSV file
            line = [key[0], key[1], calculate_PMI(merged_dicts[key], key[0], key[1])]
            writer.writerow(line)
    


