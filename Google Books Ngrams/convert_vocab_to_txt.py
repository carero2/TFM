import csv

with open("vocab_info.csv", 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    vocab = list(csv_reader)

# Replace commas with spaces and write to a new text file
with open("vocab.txt", 'w') as txt_file:
    for row in vocab[:100000]:
        txt_file.write(" ".join(row) + "\n")