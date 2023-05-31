# Get Github stats

Tool to download and scrape Github Archive logs and extract interesting information about all the repos and users

## Download CSVS

To download the CSVs with the results you need Git Large File Storage. **Run `git lfs pull` inside the cloned repository** to download the compressed CSVs.

## Usage

```bash

# Generate Github Archive URLs
python3 gen_gh_urls.py # This will populate the urls_list.txt file in the local directory

# Dowload log files
python3 gh_downloader.py -t 5 -i urls_list.txt -o /tmp/gh/jsons/

# Download and scrape the logs
python3 gh_scraper.py -i /tmp/gh/jsons/ -o /tmp/gh/

# Get extra information of the logs
python3 gh_enhancer.py -T <github_token> -u /tmp/gh/users.csv -r /tmp/gh/repos.csv -o /tmp/gh/

# Get interesting information
python3 gh_investigator.py -u /tmp/gh/users.csv -r /tmp/gh/repos.csv -o /tmp/gh/
```
