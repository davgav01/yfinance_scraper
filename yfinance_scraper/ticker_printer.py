from yahoo_fin import stock_info as si

def get_combined_tickers():
    tickers = set()
    tickers.update(si.tickers_sp500())
    tickers.update(si.tickers_nasdaq())
    tickers.update(si.tickers_dow())
    return sorted(tickers)

if __name__ == "__main__":
    combined_tickers = get_combined_tickers()
    print("Combined Ticker List:")
    print(combined_tickers)
    print(len(combined_tickers))
    

