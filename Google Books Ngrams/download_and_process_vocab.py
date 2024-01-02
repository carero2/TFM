#!/usr/bin/env python3
# Import necessary libraries
import requests
import gzip
import datetime
import io
import re
import pandas as pd
import argparse
from rich.progress import Progress
import os

# Set up command-line argument parser
parser = argparse.ArgumentParser(description="Script to download and preprocess data from Google Books 1-grams, in order to obtain the vocabulary and frequency of words.",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-v", "--verbose", action="store_false", help="decrease verbosity")
parser.add_argument("-y", "--range_years", default=False, help="years range to analyze. If default, it will include all years available")
args = parser.parse_args()
config = vars(args)

# Function to print verbose messages if verbosity is enabled
def verbose(*args):
    if config["verbose"]:
        print(*args)

# Initialize vocabulary dictionary
vocab_dict = {}

# The URL base for all n-gram types
url_base = "http://storage.googleapis.com/books/ngrams/books/20200217/eng/1-"

# Create a pattern for checking valid words in grams
pattern = re.compile(r'^_[A-Z]+_$|^[A-Za-z]+(?:_[A-Z]+)?(?:[.,!?:;)])?$')

# Function to check if a word is valid
def check_valid_word(word):
    if not pattern.match(word):
        return False
    return True

# Function to remove some punctuation at the end of a word
def remove_punctuation(text):
    for punc in list([".", ",", "!", "?", ":", ";", ")"]):
        if punc in text:
            text = text.replace(punc, ' ')
    return text.strip().lower()

# Set up which occurrences get (all or years specific)
if config["range_years"]:
    dir_years = config["range_years"]
    years = [int(year) for year in config["range_years"].split("-")]
    def get_value(line):
        occurrences = 0
        for entry in line:
            year = int(entry.split(',')[0])
            if year >= min(years) and year <= max(years):
                value = int(entry.split(',')[1])
                occurrences += value
        return occurrences
else:
    dir_years = "all_years"
    def get_value(line):
        occurrences = 0
        for entry in line:
            value = int(entry.split(',')[1])
            occurrences += value
        return occurrences

# Function to create a folder if it doesn't exist
def create_folder(dir):
    if not os.path.isdir(dir):
        os.mkdir(dir)

# Create folder for the specified years
create_folder(dir_years)

# Function to download and decompress a file
def download_decompress(num_file):
    try:
        completed_url = url_base + str(num_file).zfill(5) + "-of-00024.gz"
        file = completed_url.split("/")[-1].replace(".gz", "")
        verbose("\nDownloading and decompressing file", file, "at", datetime.datetime.now().strftime("%H:%M:%S"), ". This can take up to 5 min.")

        # Read the corpus files
        response = requests.get(completed_url, stream=True)
        size = int(response.headers.get('content-length', 0)) * 0.0073

    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

    decompressed_file = gzip.GzipFile(fileobj=io.BytesIO(response.content))
    verbose("Decompression successful.")
    return decompressed_file, size

# Function to update the word dictionary
def update_counter(word, value):
    if word not in vocab_dict:
        vocab_dict[word] = value
    else:
        vocab_dict[word] += value

# Function to process the decompressed file
def process_file(decompressed_file, size):
    for line in decompressed_file:
        # Read the line
        modified_line = line.decode('utf-8')
        # Split line into grams (first position) and years (following positions)
        line_split = modified_line.split('\t')

        # Get word and check if it is a valid gram
        word = line_split[0]
        if check_valid_word(word):
            # Sum all occurrences among years
            occurrences = get_value(line_split[1:])
            update_counter(remove_punctuation(word), occurrences)

        progress.update(task2, advance=1000/size)

# Function to download, process, and write the data
def download_process_write(num_file):
    decompressed_file, size = download_decompress(num_file)
    process_file(decompressed_file, size)
    print("File loaded successfully.")
    progress.update(task1, advance=1)
    progress.reset(task2)
    
    # Save data after every 2 files are processed
    if num_file % 2 == 0:
        df = pd.DataFrame.from_dict(vocab_dict, orient="index")
        df.to_csv('./'+dir_years+'/vocab_info.csv')
        del df

# Iterate through the files to download and process
with Progress(transient=True) as progress:
    task1 = progress.add_task("[blue]Percentage of total files analyzed...", total=18, visible=config["verbose"])
    task2 = progress.add_task("[red]Processing file...", total=1000, visible=config["verbose"])

    for num_file in range(6, 24):
        download_process_write(num_file)

    print("Saving data into a CSV file. This can take some minutes.")
    vocab_dict = dict(sorted(vocab_dict.items(), key=lambda item: item[1], reverse=True))
    df = pd.DataFrame.from_dict(vocab_dict, orient="index")
    df.to_csv('./'+dir_years+'/vocab_info.csv')