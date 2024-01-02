# N-gram Co-occurrence Analysis

## Overview

This repository contains a Python script, `download_and_process.py`, designed to download and process n-gram data from Google Books. The script allows users to specify the type of n-grams (2 to 5), the number of files to analyze, and whether to create co-occurrence matrices for each different year.

## Prerequisites

- Python 3
- [Git LFS](https://git-lfs.github.com/) (for cloning the repository)
- Required Python libraries:
  - `requests`
  - `gzip`
  - `datetime`
  - `tqdm`
  - `io`
  - `re`
  - `os`
  - `csv`

Ensure you have Python 3 installed and Git LFS downloaded and installed from [Git LFS website](https://git-lfs.com/) in order to download large files like `5gram_cooccurrence_dict.csv`. Install the required Python libraries using pip:

```bash
pip install requests tqdm gzip io
```

## Cloning the Repository
To clone this repository, make sure you have Git LFS installed. If not, download and install Git LFS from [here](https://git-lfs.com/) and run:


```bash
git lfs install  # Run this command once you downloaded git lfs from website (follow steps there)
git clone https://github.com/carero2/TFM.git
cd TFM
```


## Usage
1. Run the script:

```bash
python download_and_process.py
```

2. Follow the prompts to configure the analysis:

- Choose the type of n-grams (2, 3, 4, or 5).
- Enter the number of files you want to process (enter 0 to process all files).
- Specify whether to create co-occurrence matrices for each different year.

3. Review the results:

- Co-occurrence matrices are saved in `2gram_cooccurrence_dict.csv`, `3gram_cooccurrence_dict.csv`, etc.
- Log information is stored in `2gram_log.csv`, `3gram_log.csv`, etc.


## Notes
- The script downloads n-gram data from Google Books and processes it to create co-occurrence matrices.
- Use caution when choosing to create co-occurrence matrices for each different year, as it may take a significant amount of time.
- The script logs the last processed file, allowing you to resume processing from where you left off.
- Feel free to explore and analyze the generated co-occurrence matrices to gain insights into the relationships between n-grams.
