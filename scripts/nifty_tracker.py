from http.client import responses

import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
from pandas import date_range

url = f'https://www.google.com/finance/quote/NIFTY_50:INDEXNSE'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
}
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, 'html.parser')

close_v = soup.find(class_="YMlKec").text.strip() #Nifty 50 closing value
prev_close_v = soup.find(class_="P6K39c").text.strip() #Nifty 50 previous closing value
change_perc_v = soup.find(class_="JwB6zf").text.strip() #Nifty 50 change value
#change_perc = soup.find(class_="WlRRw IsqQVc fw-price-dn").text.strip() #Nifty 50 change percentage
range_v = soup.find_all(class_="P6K39c")[1].text.strip()     #Nifty 50 range value

print(f'Nifty 50 closing value: {close_v}')
print(f'Nifty 50 previous closing value: {prev_close_v}')
print(f'Nifty 50 change value: {change_perc_v}')
print(f'Nifty 50 range value: {range_v}')

range_day_values = range_v.split(" - ")
day_low = range_day_values[0].strip()
day_high = range_day_values[1].strip()

close_v = float(close_v.replace(',', ''))
prev_close_v = float(prev_close_v.replace(',', ''))
day_low = float(day_low.replace(',', ''))
day_high = float(day_high.replace(',', ''))
day_range = round((day_high - day_low),2)
trading_date = time.strftime("%Y-%m-%d")
index_change = round((close_v - prev_close_v),2)

data = {
    'Trading Date': trading_date,
    'Close': close_v,
    'Prev Close': prev_close_v,
    'Day Low': day_low,
    'Day High': day_high,
    'Change': index_change,
    'Day Range': day_range,
    'change in index' : index_change,
    'Change percent': change_perc_v}

df = pd.DataFrame([data])

csv_file = 'data/nifty_50_tracker.csv'

existing_df = pd.read_csv(csv_file)
df = pd.concat([existing_df, df], ignore_index=True)

df.to_csv(csv_file, index=False)


