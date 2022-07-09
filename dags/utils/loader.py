import pandas as pd
import requests
import json

def get_data_by_symbols_and_period(
        symbols: list,
        start_date: str,
        end_date: str
) -> pd.DataFrame:
    url = f"https://api.exchangerate.host/timeseries?start_date={start_date}&end_date={end_date}&symbols={','.join(symbols)}"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame.from_dict(data)
    
    #убрал лишние строки, колонки
    df = df.drop(index=['msg', 'url'], columns=['motd', 'success','timeseries', 'start_date', 'end_date'])
    
    #убрал индес с даты
    df = df.rename_axis('date').reset_index()
    
    #convert dict to json, некрасивая лямбда -_-
    df['rates'] = df['rates'].apply(lambda x: json.dumps(x))
    
    return df