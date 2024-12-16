from bybit_api import TokenHistory


token = TokenHistory(start_date='2019-01-01', end_date='2024-01-01')

candle_bars_data = token.get_coin_candle_bars_data(convert_time_to_dt=True)

result = token.convert_history_to_pandas_df(candle_bars_data)

print(result.to_csv('bitcoin_price_history.csv'))

