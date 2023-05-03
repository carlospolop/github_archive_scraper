# Get Github stats

Tool to download and scrape Github Archive logs and extract interesting information about all the repos and users


```bash

# Generate Github Archive URLs
current_year=$(date +%Y)
current_month=$(date +%m)
current_day=$(date +%d)
current_hour=$(date +%H)

for year in {2015.."$current_year"}; do
  [ "$year" -gt "$current_year" ] && break
  for month in {01..12}; do
    [ "$year" -ge "$current_year" ] && [ "$month" -gt "$current_month" ] && break
    for day in {01..31}; do
      [ "$year" -ge "$current_year" ] && [ "$month" -ge "$current_month" ] && [ "$day" -gt "$current_day" ] && break
      for hour in {0..23}; do
        [ "$year" -ge "$current_year" ] && [ "$month" -ge "$current_month" ] && [ "$day" -ge "$current_day" ] && [ "$hour" -gt "$current_hour" ] && break

        printf "https://data.gharchive.org/%s-%02d-%02d-%02d.json.gz\n" "$year" "$month" "$day" "$hour"
      done
    done
  done
done | shuf > urls_list.txt

# Download and scrape the logs
python3 gh_scraper.py -t 10 urls_list.txt /tmp/

# Get extra information of the logs
python3 gh_enhancer.py -t 10 -T <github_token> .

# Get interesting information
python3 gh_investigator .
```
