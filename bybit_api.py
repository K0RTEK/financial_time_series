import datetime as dt
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from pybit.unified_trading import HTTP
from typing import List, Tuple, Union, Optional


class TokenHistory:
    def __init__(self, token_symbol: str = 'BTCUSD', start_date: str = '2020-01-01',
                 end_date: str = '2024-01-01', interval: Union[int, str] = 'D'):
        """
        Инициализация класса TokenHistory.

        :param token_symbol: Символ токена (по умолчанию 'BTCUSD')
        :param start_date: Дата начала (в формате 'YYYY-MM-DD')
        :param end_date: Дата окончания (в формате 'YYYY-MM-DD')
        :param interval: Интервал запроса ('D' - день, 'M' - месяц и т.д.)
        """
        self.token_symbol = token_symbol
        self.interval = interval
        self.start_date = self.__validate_date(start_date)
        self.end_date = self.__validate_date(end_date)

    @staticmethod
    def __validate_date(date_str: str) -> datetime:
        """Проверяет и преобразует строку даты в объект datetime."""
        try:
            return dt.datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError as e:
            raise ValueError(f"Некорректный формат даты: {date_str}. Ожидается формат 'YYYY-MM-DD'") from e

    @staticmethod
    def __convert_dates_to_ms(date: datetime) -> str:
        """Преобразует дату в миллисекунды."""
        return str(int(date.timestamp() * 1000))

    @staticmethod
    def __split_date_interval(start_date: datetime, end_date: datetime, months: int = 3) -> List[
        Tuple[datetime, datetime]]:
        """
        Разбивает интервал дат на подинтервалы по заданному количеству месяцев.

        :param start_date: Начальная дата (datetime)
        :param end_date: Конечная дата (datetime)
        :param months: Количество месяцев в каждом интервале (по умолчанию 3)
        :return: Список кортежей (начало интервала, конец интервала)
        """
        intervals = []
        current_start = start_date

        while current_start < end_date:
            current_end = min(current_start + relativedelta(months=months) - timedelta(days=1), end_date)
            intervals.append((current_start, current_end))
            current_start = current_end + timedelta(days=1)

        return intervals

    def get_coin_candle_bars_data(self, convert_time_to_dt: bool = False) -> Optional[dict]:
        """
        Получает данные свечей для заданного токена.

        :param convert_time_to_dt: Преобразовать метки времени в datetime (по умолчанию False)
        :return: Словарь с данными свечей или None в случае ошибки
        """
        session = HTTP(testnet=True)
        intervals = self.__split_date_interval(self.start_date, self.end_date)
        aggregated_data = None

        try:
            for start, end in intervals:
                result = session.get_kline(
                    category='inverse',
                    symbol=self.token_symbol,
                    interval=self.interval,
                    start=self.__convert_dates_to_ms(start),
                    end=self.__convert_dates_to_ms(end)
                )
                if aggregated_data:
                    aggregated_data['result']['list'] += result['result']['list']
                else:
                    aggregated_data = result

            if convert_time_to_dt and aggregated_data:
                aggregated_data['result']['list'] = [
                    [dt.datetime.fromtimestamp(int(x[0]) / 1000)] + x[1:]
                    for x in aggregated_data['result']['list']
                ]

            return aggregated_data

        except Exception as e:
            print(f"Ошибка при получении данных: {e}")
            return None

    def convert_history_to_pandas_df(self, data: dict) -> pd.DataFrame:
        """
        Преобразует данные свечей в DataFrame.

        :param data: Словарь с данными свечей
        :return: DataFrame с историей цен токена
        """
        if not data or 'result' not in data or 'list' not in data['result']:
            raise ValueError("Некорректные данные для преобразования в DataFrame.")

        token_history_df = pd.DataFrame(
            data['result']['list'],
            columns=['startTime', 'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'volume', 'turnover']
        )

        token_history_df['symbol'] = data['result']['symbol']

        return token_history_df


if __name__ == "__main__":
    history = TokenHistory(token_symbol='BTCUSD', start_date='2020-01-01', end_date='2024-01-01', interval='D')
    data = history.get_coin_candle_bars_data(convert_time_to_dt=True)

    if data:
        df = history.convert_history_to_pandas_df(data)
        print(df.head())
