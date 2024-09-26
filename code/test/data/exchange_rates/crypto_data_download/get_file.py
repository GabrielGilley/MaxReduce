# First import the libraries that we need to use
import pandas as pd
import requests
import time
from datetime import datetime, timedelta


def fetch_all_daily_data(symbol):
    pair_split = symbol.split('/')  # symbol must be in format XXX/XXX ie. BTC/EUR
    symbol = pair_split[0] + '-' + pair_split[1]
    api_url = f'https://api.pro.coinbase.com'

    bar_size = "86400"
    time_end = datetime.now().replace(microsecond=0)

    # Get 250 days at a time
    delta = timedelta(days=250)

    time_start = datetime.fromisoformat("2012-01-01")
    df = None
    while True:
        print(time_start.isoformat())
        print((time_start + delta).isoformat())
        parameters = {
            "start": time_start.isoformat(),
            "end": (time_start + delta).isoformat(),
            "granularity": bar_size
        }

        response = requests.get(
            f'{api_url}/products/{symbol}/candles',
            params = parameters,
            headers={"content-type": "application/json"},
            verify=False,
            timeout=200,
        )
        if response.status_code == 200:  # check to make sure the response from server is good

            df_tmp = pd.DataFrame(response.json(), columns=['time', 'low', 'high', 'open', 'close', 'volume'])
            df_tmp["date"] = pd.to_datetime(df_tmp["time"], unit="s")  # convert to a readable date
        else:
            print(f"Did not receieve OK response from Coinbase API: {response}")
            time.sleep(10)
            continue

        df = pd.concat([df, df_tmp])
        if time_start > time_end:
            # Write the file
            df = df.dropna()
            df.to_csv(f'Coinbase_{pair_split[0]}_dailydata.csv', index=False)
            break
        time_start = time_start + delta

if __name__ == "__main__":
# we set which pair we want to retrieve data for
    pair = "BAT/USD"
    fetch_all_daily_data(symbol=pair)
