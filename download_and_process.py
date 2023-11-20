#!/usr/bin/env python3

import requests
import gzip
import datetime
from tqdm import tqdm
import io
import re
import os
import csv


# Save the number of files per n-gram type and where this files start 
# (the first files have punctuation or numbers and we are not interested on that)
number_files_x_ngram = {2: [85, 589], 3:[671, 6881], 4:[515, 6668], 5:[1312, 19423]}

# Ask which type of n-grams to analyze
n_gram_answer = input("How many N-grams you want to analyze? (Enter one of these integers [2,3,4,5]): ")

if n_gram_answer not in ["2", "3", "4", "5"]:
    raise Exception("N-gram provided is not valid. N-gram should be [2,3,4,5]")

# Ask about the number of files to analyze
num_files_answer = input("Enter the number of files you want to process (Enter int or 0 to process all the files): ")

# Catch some invalid inputs
try:
	int(num_files_answer)
except:
	raise Exception("Number of files provided is not valid. Number of files has to be an integer. ")

if int(num_files_answer) > number_files_x_ngram[int(n_gram_answer)][1]:
	raise Exception("Number of files provided is not valid. Number is higher than existing files to download and process," +
				 str(number_files_x_ngram[int(n_gram_answer)][1]))


# Define in which file start downloading and processing
start_files = number_files_x_ngram[int(n_gram_answer)][0]

# Define how many files to download and process
if int(num_files_answer) == 0:
	# To download and process all existing files
    num_files = number_files_x_ngram[int(n_gram_answer)][1]
else:
	# To download and process desired number of files
    num_files = start_files + int(num_files_answer)




# Ask whether to create an overall co-occurrence matrix or a co-occurrence matrix per year and overall. # TODO
create_cooccurrence_years = input("Do you want to create the co-occurrence matrices from every different year? Notice that could take long times (Enter Yes/No): ")

# The url base for all n-gram types
url_base = "http://storage.googleapis.com/books/ngrams/books/20200217/eng/" + n_gram_answer + "-"

# Define a function to initiate the co-occurrence dict and log
def initialice():
	# Create log file
	with open(n_gram_answer+'gram_log.csv', 'w') as f:
		csvwriter = csv.writer(f)
		csvwriter.writerow(["date", "last_file_processed"])

    # Initiate co-occurrence dict
	word_dict = {}

# Check if file already exists
if os.path.exists(n_gram_answer+"gram_cooccurrence_dict.csv"):
	create_or_overwrite = " "
	while create_or_overwrite.lower()[0] not in ["o", "r"]:
		create_or_overwrite = input("It already exists a ", n_gram_answer, "gram_cooccurrence_dict.csv. Do you want to overwrite it or read and work with it? (Enter Overwrite or Read): ")

	if create_or_overwrite.lower()[0] == "o":
		initialice()

	if create_or_overwrite.lower()[0] == "r":
		word_dict = {}
		with open(n_gram_answer+'gram_cooccurrence_dict.csv', 'r') as file:
			csv_reader = csv.reader(file, delimiter=',')
			for row in csv_reader:
				# Convert the first column to a tuple
				key = eval(row[0])
				value = int(row[1])  # Convert the second column to an integer (assuming it contains integers)
				word_dict[key] = value

		# Read which was the last file processed from the log file and save it as start
		with open(n_gram_answer+'gram_log.csv', "r") as f:
			last_line = f.readlines()[-1]
			start_files = int(last_line.split(',')[1]) + 1
			num_files = start_files + int(num_files_answer)

else:
	initialice()



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
def update_coocurrences(word1, word2, value):
	tuple_words = tuple(sorted((remove_punctuation(word1), remove_punctuation(word2))))
	if tuple_words not in word_dict:
		word_dict[tuple_words] = value
    
	else:
		word_dict[tuple_words] += value

# Iterate through the files to download and process
for num_file in tqdm(range(start_files, num_files), desc="Processing files..."):
	try:
		completed_url= url_base + str(num_file).zfill(5) + "-of-" + str(number_files_x_ngram[int(n_gram_answer)][1]).zfill(5) + ".gz"
		file = completed_url.split("/")[-1].replace(".gz", "")
		print("\nDownloading file", file, "at", datetime.datetime.now().strftime("%H:%M:%S"))
		
		# Read the corpus files and print them line by line
		response = requests.get(completed_url)
	except requests.exceptions.RequestException as e:
		raise SystemExit(e)
	
	print("Request successful after", response.elapsed.total_seconds(), "seconds.")
	decompressed_file = gzip.GzipFile(fileobj=io.BytesIO(response.content))

	print("Decompression successful. Starting processing the file:")

	for line in tqdm(decompressed_file):

		# Read the line
		modified_line = line.decode('utf-8')

		# Split line in grams (first position) and years (following positions)
		line_split = modified_line.split('\t')

		# Get gram and check if it is a valid gram
		gram = line_split[0].split(" ")
		if check_valid_gram(gram):

			# Sum all occurrences among years
			occurrencies = sum(int(entry.split(',')[1]) for entry in line_split[1:] if entry)
			
			# Get the occurrences for each year to construct co-occurrence per year 
			# TODO
			# for i in range(len(line_split) - 1):
			#     year = line_split[i + 1].split(',')[0]
			#     if create_cooccurrence_years.lower()[0] == "y":
			#         pass  

			# For each pair of words within the window gram, update the co-occurrence dict
			for i in range(int(n_gram_answer)):
				for j in range(int(n_gram_answer)):
					if i != j and j > i:
						update_coocurrences(gram[i], gram[j], occurrencies)

	# Safe when each file is processed	
	with open(n_gram_answer+'gram_cooccurrence_dict.csv', 'w', newline='') as f:
		writer = csv.writer(f)
		for row in word_dict.items():
			writer.writerow(row)

	# Safe each file processed in log
	with open(n_gram_answer+'gram_log.csv', 'a', newline='') as f:
		csvwriter = csv.writer(f)
		csvwriter.writerow([datetime.datetime.now(), num_file])
	print("File", file, "loaded succesfully.")