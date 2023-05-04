import datetime
import random

current_date = datetime.datetime.now()

# Function to generate URLs
def generate_urls(current_date):
    urls = []
    for year in range(2015, current_date.year + 1):
        for month in range(1, 13):
            if year == current_date.year and month > current_date.month:
                break
            for day in range(1, 32):
                if year == current_date.year and month == current_date.month and day > current_date.day:
                    break
                for hour in range(0, 24):
                    if year == current_date.year and month == current_date.month and day == current_date.day and hour > current_date.hour:
                        break

                    url = f"https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour:02d}.json.gz"
                    urls.append(url)
    return urls

urls = generate_urls(current_date)

# Shuffle the URLs and get the first 100
random.shuffle(urls)
selected_urls = urls

# Write the selected URLs to the output file
with open("urls_list.txt", "w") as output_file:
    for url in selected_urls:
        output_file.write(f"{url}\n")