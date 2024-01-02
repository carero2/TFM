#!/usr/bin/env python3

import requests
import gzip
import datetime
import io
import re
import os
import csv
import pandas as pd
import argparse
import psutil
import concurrent.futures
from rich.progress import Progress
import sys

 
parser = argparse.ArgumentParser(description="Script to download and preprocess data from Google Books Ngrams and store it as a co-occurrence matrix.",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("n", help="n-gram to analyze. Options are [2,3,4,5]")
parser.add_argument("t", default=2, help="number of threads/workers to create. Note: each thread will take 1GB memory usage.")
parser.add_argument("-v", "--verbose", action="store_false", help="decrease verbosity")
parser.add_argument("-r", "--read",  action="store_true", help="read from last file analyzed extracted from log.csv (default: overwrite)")
parser.add_argument("-n", "--n_batch",  default=None, help="from which number of batch read. This will read the previous threads in the specified batch,")
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
	
	global log_df
	log_df = pd.read_csv("file_log.csv", names = ["date", "n-gram", "last_file_processed", "years_processed", "thread", "failed"], index_col=False, skiprows=[0])


def create_folder(dir):
	if not os.path.isdir(dir):
		os.mkdir(dir)



def download_decompress(num_file):
	try:
		completed_url= url_base + str(num_file).zfill(5) + "-of-" + str(number_files_x_ngram[int(n_gram_answer)][1]).zfill(5) + ".gz"
		file = completed_url.split("/")[-1].replace(".gz", "")
		verbose("\nDownloading and decompressing file", file, "at", datetime.datetime.now().strftime("%H:%M:%S"), ". This can take up to 10 min.")

        # Read the corpus files
		response = requests.get(completed_url, stream=True)


		size = int(response.headers.get('content-length', 0))*0.0073

		decompressed_file = gzip.GzipFile(fileobj=io.BytesIO(response.content))

		verbose("Decompression successful.")

		return decompressed_file, size

	except requests.exceptions.RequestException as e:
		print(SystemExit(e))
		return -1, -1
	



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

def read_dict_csv_tuple(file_path):
    result_dict = {}
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            key = eval(row[0])
            value = int(row[1])
            result_dict[key] = value
    return result_dict

vocab_dict = read_dict_csv("./"+dir_years+"/vocab_info.csv")

vocab_id = {key: idx + 1 for idx, key in enumerate(vocab_dict)}

del vocab_dict

# Getting % usage of virtual_memory ( 3rd field)
verbose('RAM memory % used:', psutil.virtual_memory()[2])
if psutil.virtual_memory()[2] > 80:
	raise Exception("Memory usage is above 80%. More memory will be needed to execute the code.")
	


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
def update_occurrences(local_dict, word1, word2, value):
	id_word1 = vocab_id[word1]
	id_word2 = vocab_id[word2]
	tuple_words = tuple(sorted((id_word1, id_word2)))
	if tuple_words not in local_dict:
		local_dict[tuple_words] = value
	else:
		local_dict[tuple_words] += value

def process_file(decompressed_file, size, local_dict, progress, thread_task):
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
				if gram[i] in vocab_id:
					for j in range(int(n_gram_answer)):
						if gram[j] in vocab_id:
							if i != j and j > i:
								update_occurrences(local_dict,
									remove_punctuation(gram[i]),
									remove_punctuation(gram[j]),
									occurrencies)
		progress.update(thread_task, advance=1000/size)
	progress.reset(thread_task)

def download_process_write(num_file, local_dict, progress, thread_task):
	decompressed_file, size = download_decompress(num_file)

	if decompressed_file != -1:
		process_file(decompressed_file, size, local_dict, progress, thread_task)

			# Safe when each file is processed in provisional thread files	
		with open(dir_years+"/"+n_gram_answer+'-gram/'+str(thread_task)+'thread_provisional_cooccurrence_info.csv', 'w', newline='') as f:
			writer = csv.writer(f)
			for row in local_dict.items():
				writer.writerow(row)
		
			# Safe each file processed in log
		with open('file_log.csv', 'a', newline='') as f:
			csvwriter = csv.writer(f)
			csvwriter.writerow([datetime.datetime.now(), n_gram_answer, num_file, dir_years, thread_task])
		verbose("File loaded succesfully.")
	else:
			# Safe each file processed in log
		with open('file_log.csv', 'a', newline='') as f:
			csvwriter = csv.writer(f)
			csvwriter.writerow([datetime.datetime.now(), n_gram_answer, num_file, dir_years, thread_task, "failed"])
		verbose("File loaded succesfully.")

def main_job(start_files, end_files, progress, thread_task):
	
	if os.path.exists(dir_years+"/"+n_gram_answer+'-gram/'+str(thread_task)+'thread_provisional_cooccurrence_info.csv'):
		local_dict = read_dict_csv_tuple(dir_years+"/"+n_gram_answer+'-gram/'+str(thread_task)+'thread_provisional_cooccurrence_info.csv')
	else:
		local_dict = {}

	for num_file in range(start_files, end_files):
		if num_file not in log_df.loc[log_df['n-gram'] == int(n_gram_answer), "last_file_processed"].values:
			download_process_write(num_file, local_dict, progress, thread_task)
		progress.update(task1, advance=1)

	os.remove(dir_years+"/"+n_gram_answer+'-gram/'+str(thread_task)+'thread_provisional_cooccurrence_info.csv')

	return local_dict

def update_dicts(d1, d2):
    for key in d2.keys():
        if key not in d1:
            d1[key] = d2[key]
        else:
            d1[key] = d1[key] + d2[key]
    return d1

def merge_dicts(merged_dict, dicts):
    for d in dicts:
        merged_dict = update_dicts(merged_dict, d)
        d.clear()
    return merged_dict

create_folder(dir_years+"/"+n_gram_answer+"-gram")
initialice()

total_files = end_files - start_files
batch_size = 50
batches = total_files//batch_size

for i_batch in range(batches):
	start_files_batch = start_files + (i_batch * batch_size)
	end_files_batch = ((i_batch + 1) * batch_size) + start_files

	
	threads = int(config["t"])
	# Calculate the range for each thread
	step = batch_size // threads
	ranges = [((i * step)+start_files_batch, ((i + 1) * step)+start_files_batch) for i in range(threads)]

	if ranges[-1][1] != end_files_batch:
		ranges[-1] = (ranges[-1][0], end_files_batch)

	with Progress(transient=True) as progress:
		task1 = progress.add_task("[blue]Percentage of total files analyzed...", total=batch_size, visible=True)

		# Create a list to store individual thread tasks
		thread_tasks = []
		for thread in range(threads):
			task_name = f"[red]Processing file (Thread {thread + 1})..."
			thread_task = progress.add_task(task_name, total=1000, visible=True)
			thread_tasks.append(thread_task)


		# Using ThreadPoolExecutor to parallelize the task
		with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
			# Submit tasks to the executor and get Future objects
			futures  = []
			for (start, end), thread_task in zip(ranges, thread_tasks):
				futures.append(executor.submit(main_job, start, end, progress, thread_task))

			# Wait for all tasks to complete and retrieve results
			results = []
			for future in concurrent.futures.as_completed(futures):
				results.append(future.result())


	# Read the last file created and update based on these results
	if os.path.exists(dir_years+"/"+n_gram_answer+"-gram/cooccurrence_info.csv"):
		merged_dict = read_dict_csv_tuple(dir_years+"/"+n_gram_answer+"-gram/cooccurrence_info.csv")
	else:
		merged_dict = {}

	# Merge the dictionaries from different threads
	merged_dict = merge_dicts(merged_dict, results)
	print("Threads merged")

	# Release memory usage
	for result in results:
		result.clear()
	del results

	
	# Safe when each file is processed	
	with open(dir_years+"/"+n_gram_answer+'-gram/cooccurrence_info.csv', 'w', newline='') as f:
		writer = csv.writer(f)
		for row in merged_dict.items():
			writer.writerow(row)
	
	print("Batch", i_batch, "with range", start_files_batch, end_files_batch, "finished and saved, at",  datetime.datetime.now().strftime("%H:%M:%S"))

	
	
	del merged_dict