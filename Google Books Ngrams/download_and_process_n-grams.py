#!/usr/bin/env python3

import requests
import gzip
import datetime
from rich.progress import Progress
import io
import re
import os
import csv
import pandas as pd
import argparse
import psutil


 
parser = argparse.ArgumentParser(description="Script to download and preprocess data from Google Books Ngrams and store it as a co-occurrence matrix.",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("n", help="n-gram to analyze. Options are [2,3,4,5]")
parser.add_argument("-v", "--verbose", action="store_false", help="decrease verbosity")
parser.add_argument("-r", "--read",  action="store_true", help="read from last file analyzed extracted from log.csv (default: overwrite)")
parser.add_argument("-y", "--range_years", default=False, help="years range to analyze. If default, it will include all years available")
parser.add_argument("-s", "--start_files", default=None, help="""from which file to start analyzing. 
					If not specified, it will start from the first file that does not include punctuation""")
parser.add_argument("-e", "--end_files", default=None, help="in which file to stop analyzing. If not specified, it will end up in the last file")
args = parser.parse_args()
config = vars(args)



def verbose(*args):
	if config["verbose"]:
		print(*args)

# Save the number of files per n-gram type and where this files start 
# (the first files have punctuation or numbers and we are not interested on that)
number_files_x_ngram = {2: [85, 589], 3:[671, 6881], 4:[515, 6668], 5:[1312, 19423]}

# Ask which type of n-grams to analyze
n_gram_answer = config["n"]

if n_gram_answer not in ["2", "3", "4", "5"]:
    raise Exception("N-gram type provided is not valid. N-gram should be [2,3,4,5]")


verbose(("The n-gram you selected ("+n_gram_answer+"-gram) has "+ str(number_files_x_ngram[int(n_gram_answer)][1])+" files in total."+
      " Files that have grams without punctuation start at file number "+str(number_files_x_ngram[int(n_gram_answer)][0])))

if config["start_files"]:
	start_files = int(config["start_files"])
else:
	start_files = number_files_x_ngram[int(n_gram_answer)][0]

if config["end_files"]:
	end_files = int(config["end_files"])
else:
	end_files = number_files_x_ngram[int(n_gram_answer)][1]

# Define a function to initiate the co-occurrence dict and log
def initialice():
	
	# Create log file
	if not os.path.exists("file_log.csv"):
		with open('file_log.csv', 'w') as f:
			csvwriter = csv.writer(f)
			csvwriter.writerow(["date", "n-gram", "last_file_processed", "years_processed"])

    # Initiate co-occurrence dict
	global word_dict
	word_dict = {}


def create_folder(dir):
	if not os.path.isdir(dir):
		os.mkdir(dir)



def download_decompress(num_file):
	try:
		completed_url= url_base + str(num_file).zfill(5) + "-of-" + str(number_files_x_ngram[int(n_gram_answer)][1]).zfill(5) + ".gz"
		file = completed_url.split("/")[-1].replace(".gz", "")
		verbose("\nDownloading and decompressing file", file, "at", datetime.datetime.now().strftime("%H:%M:%S"), ". This can take up to 5 min.")

        # Read the corpus files
		response = requests.get(completed_url, stream=True)


		size = int(response.headers.get('content-length', 0))*0.0073

	except requests.exceptions.RequestException as e:
		raise SystemExit(e)
	

	decompressed_file = gzip.GzipFile(fileobj=io.BytesIO(response.content))

	verbose("Decompression successful.")

	return decompressed_file, size



if config["range_years"]:
	dir_years = config["range_years"]
	years = [int(year) for year in config["range_years"].split("-")]
	def get_value(line):
		occurrencies = 0
		for entry in line:
			year=int(entry.split(',')[0])
			if year >= min(years) and year <= max(years):
				value=int(entry.split(',')[1])
				occurrencies += value
		return occurrencies
else:
	dir_years = "all_years"
	def get_value(line):
		occurrencies = 0
		for entry in line:
			value=int(entry.split(',')[1])
			occurrencies += value
		return occurrencies

create_folder(dir_years)

def read_dict_csv(file_path):
    result_dict = {}
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            key = row[0]
            value = row[1]
            result_dict[key] = value
    return result_dict

vocab_dict = read_dict_csv("./"+dir_years+"/vocab_info.csv")

vocab_id = {key: idx + 1 for idx, key in enumerate(vocab_dict)}

del vocab_dict

# Getting % usage of virtual_memory ( 3rd field)
print('RAM memory % used:', psutil.virtual_memory()[2])
if psutil.virtual_memory()[2] > 80:
	raise Exception("Memory usage is above 80%. More memory will be needed to execute the code.")


# Check if file already exists
if os.path.exists(dir_years+"/"+n_gram_answer+"-gram/cooccurrence_info.csv"):
	if config["read"]:
		# TODO: Read cooccurrence_info.csv to as dict in word_dict
		word_dict = read_dict_csv(dir_years+"/"+n_gram_answer+"-gram/cooccurrence_info.csv")

		# Read which was the last file processed from the log file and save it as start
		log_df = pd.read_csv("file_log.csv")
		# Read log_df and get last file modified from the specified n-gram
		start_files = int(log_df.loc[log_df['n-gram'] == int(n_gram_answer), "last_file_processed"].iloc[-1])
		del log_df

	else:
		create_folder(dir_years+"/"+n_gram_answer+"-gram")
		initialice()

# If it is the first time executed and it does not exist the n-gram folder
else:
	create_folder(dir_years+"/"+n_gram_answer+"-gram")
	initialice()


# The url base for all n-gram types
url_base = "http://storage.googleapis.com/books/ngrams/books/20200217/eng/" + n_gram_answer + "-"


# Create validation to check desired words in grams
pattern = re.compile(r'^_[A-Z]+_$|^[A-Za-z]+(?:_[A-Z]+)?(?:[.,!?:;)])?$')

def check_valid_gram(words):
    for word in words:
        if not pattern.match(word):
            return False
    return True

# Function to remove some punctuation in the ending of a word
def remove_punctuation(text):
    for punc in list([".", ",", "!", "?", ":", ";",")"]):
        if punc in text:
            text = text.replace(punc, ' ')
    return text.strip().lower()


# Function to update word_dict
def update_occurrences(word1, word2, value):
	id_word1 = vocab_id[word1]
	id_word2 = vocab_id[word2]
	tuple_words = tuple(sorted((id_word1, id_word2)))
	if tuple_words not in word_dict:
		word_dict[tuple_words] = value
	else:
		word_dict[tuple_words] += value

def process_file(decompressed_file, size):
	for line in decompressed_file:
		# Read the line
		modified_line = line.decode('utf-8')
		# Split line in grams (first position) and years (following positions)
		line_split = modified_line.split('\t')
		# Get gram and check if it is a valid gram
		gram = line_split[0].split(" ")
		if check_valid_gram(gram):
		
			# Sum all occurrences among years
			occurrencies = get_value(line_split[1:])
			for i in range(int(n_gram_answer)):
				for j in range(int(n_gram_answer)):
					if i != j and j > i:
						update_occurrences(
							remove_punctuation(gram[i]),
							remove_punctuation(gram[j]),
							occurrencies)
		progress.update(task2, advance=1000/size)

def download_process_write(num_file):
	decompressed_file, size = download_decompress(num_file)
	
	process_file(decompressed_file, size)
						
	# Safe when each file is processed	
	with open(dir_years+"/"+n_gram_answer+'-gram/cooccurrence_info.csv', 'w', newline='') as f:
		writer = csv.writer(f)
		for row in word_dict.items():
			writer.writerow(row)
	# Safe each file processed in log
	with open('file_log.csv', 'a', newline='') as f:
		csvwriter = csv.writer(f)
		csvwriter.writerow([datetime.datetime.now(), n_gram_answer, num_file, dir_years])
	print("File loaded succesfully.")
	progress.update(task1, advance=1)
	progress.reset(task2)


total_files = end_files - start_files

# Iterate through the files to download and process
with Progress(transient=True) as progress:

	task1 = progress.add_task("[blue]Percentage of total files analyzed...", total=total_files, visible=config["verbose"])
	task2 = progress.add_task("[red]Processing file...", total=1000, visible=config["verbose"])

	for num_file in range(start_files, end_files):
		download_process_write(num_file)



# 2815