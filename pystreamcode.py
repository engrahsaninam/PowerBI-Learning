import requests,json
from datetime import date , timedelta
import time


def post_data_to_powerbi(payload):
    url='https://api.powerbi.com/beta/6d1fb765-0c5a-475c-b7b6-fd2fafa1371e/datasets/ac989f7b-96b2-4adb-a6a8-8874800a287a/rows?experience=power-bi&key=7uQRzP3hnMfHlvh5Yo0JJkd2aq1NGQi%2Fol9VoOB%2F5HtMfNUk%2B%2FQzDBikJ9Fs9q4O5bEpIHedylONxhPmVZYeWA%3D%3D'
    headers={'content-type':'application/json'}
    payload= json.dumps(payload)
    response=requests.request("POST",url,headers=headers,data=payload)

    print(response)
    # print(response.text)


def get_ticker_data_from_polygon(ticker,start_date,end_date,delta=timedelta(days=1)):
    while start_date<=end_date:
        
        dt= start_date.strftime('%Y-%m-%d')
        
        url = 'https://api.polygon.io/v1/open-close/{}/{}?adjusted=true&apiKey=ZhJlPWhmvVhflE8mvm6e5fFL3Ra596KV'.format(ticker,dt)

        headers = {'content-type':'application/json'}

        response = requests.request("GET",url,headers=headers)

        start_date+=delta
        resp=response.json()
        try:
            yield {'symbol':resp['symbol'],'from':resp['from'],'open':resp['open']}
        except Exception as e:
            print(e)
            print(resp)
            time.sleep(5)
            continue

        time.sleep(15)

def get_tickers_from_polygon():
    
    start_date = date(2023,5,1)

    end_date = date(2023,6,1)

    delta=timedelta(days=1)

    url = 'https://api.polygon.io/v3/reference/tickers?active=true&apiKey=ZhJlPWhmvVhflE8mvm6e5fFL3Ra596KV'

    headers={'content-type':'application/json'}

    response=requests.request("GET",url,headers=headers)

    tickers_data = json.loads(response.text)['results']

    for td in tickers_data:
        for data in get_ticker_data_from_polygon(td['ticker'],start_date,end_date,delta):
            post_data_to_powerbi([data])
            print(data)

def main():
    get_tickers_from_polygon()


main()




