import struct
import pandas as pd


data = pd.read_csv("final_cooccurrence.csv", header=None, names=["col1", "col2", "col3"])

data = list(data.itertuples(index=False, name=None))

# Path to the binary file
binary_file_path = "new_cooccurrence.bin"

# Open the binary file in write mode
with open(binary_file_path, "wb") as bin_file:
    # Iterate over each tuple of human-readable data
    for data_tuple in data:
        # Pack the data into binary format
        binary_data = struct.pack('iid', *data_tuple)
        
        # Write the binary data to the file
        bin_file.write(binary_data)