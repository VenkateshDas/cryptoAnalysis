import ta
import sys
import os
import pprint
import json
import numpy as np  # linear algebra
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
from datetime import datetime
import matplotlib.pyplot as plt
import re

plt.style.use("seaborn-darkgrid")


def read_parquet_file(file_path):
    df_list = []
    for files in os.listdir(file_path):
        if re.search("^day=.", str(files)):
            # print(files)
            for file in os.listdir(file_path + files):
                if file.endswith(".parquet"):
                    # print(file)
                    parquet_path = file_path + files + "/" + file
                    parquet_df = pd.read_parquet(parquet_path, engine="pyarrow")
                    df_list.append(parquet_df)

    parquet_df = df_list[0].append(df_list[1:])

    return parquet_df


def standard_deviation(df, n):
    """Calculate Standard Deviation for given data.

    :param df: pandas.DataFrame
    :param n:
    :return: pandas.DataFrame
    """
    df = df.join(
        pd.Series(df["close"].rolling(n, min_periods=n).std(), name="STD_" + str(n))
    )
    return df


def read_csv_file(file_path):

    df = pd.read_csv(file_path)

    return df


def fill_missing(df):
    """
    function to impute missing values using interpolation

    close,currency,high,low,name,open,symbol,VOLUME_COIN,VOLUME_CURRENCY,START_TIME_UNIX_TS,START_TIME,day

    """
    df["close"] = df["close"].interpolate()
    df["VOLUME_COIN"] = df["VOLUME_COIN"].interpolate()
    df["VOLUME_CURRENCY"] = df["VOLUME_CURRENCY"].interpolate()
    df["high"] = df["high"].interpolate()
    df["low"] = df["low"].interpolate()
    df["open"] = df["open"].interpolate()

    return df


def preprocess(df, Freq):

    # Converting the Timestamp column from string to datetime
    df["START_TIME_UNIX_TS"] = [
        datetime.fromtimestamp(x) for x in df["START_TIME_UNIX_TS"]
    ]
    df = df.set_index("START_TIME_UNIX_TS")
    df = df.resample(Freq).mean()  # daily resampling
    df = fill_missing(df)

    return df


def technical_analysis(df):
    # Add all ta features filling nans values
    df = ta.add_all_ta_features(
        df, "open", "high", "low", "close", "VOLUME_CURRENCY", fillna=True
    )
    print("The number of Technical Analysis Indicators : {}".format(len(df.columns)))
    print("The technical analysis : {}".format(df.columns))
    N_sd = 50
    df = standard_deviation(df, N_sd)

    return df


def plot_ema(df, file_path):
    """
    This function is to plot EMA
    """
    ax = df[["close", "trend_ema_fast", "trend_ema_slow"]].plot(
        figsize=(12, 6), alpha=0.8
    )
    ax.figure.savefig(file_path)


def plot_macd(df, file_path):
    """
    This plots the macd
    """
    ax = df[["trend_macd", "trend_macd_signal", "trend_macd_diff"]].plot(
        figsize=(12, 6), alpha=0.8
    )
    ax.figure.savefig(file_path)


def plot_rsi(df, file_path):
    """
    This plots RSI
    """
    fig = plt.figure(constrained_layout=True)
    spec = fig.add_gridspec(ncols=1, nrows=2)

    f_ax1 = fig.add_subplot(spec[0, 0])
    f_ax2 = fig.add_subplot(spec[1, 0])

    f_ax1.plot(df["close"])
    f_ax2.plot(df["momentum_rsi"])

    fig.savefig(file_path)


def plot_sma(df, file_path):
    """
    This plots SMA
    """
    ax = df[["close", "trend_sma_fast", "trend_sma_slow"]].plot(
        figsize=(12, 6), alpha=0.8
    )
    ax.figure.savefig(file_path)


def plot_bb(df, file_path):
    """
    This plots Bollinger Bands
    """
    ax = df[["close", "volatility_bbh", "volatility_bbl", "volatility_bbm"]].plot(
        figsize=(12, 6), alpha=0.8
    )
    ax.figure.savefig(file_path)


def plot_sd(df, file_path):
    """
    This plots SD
    """
    fig = plt.figure(constrained_layout=True)
    spec = fig.add_gridspec(ncols=1, nrows=2)

    f_ax1 = fig.add_subplot(spec[0, 0])
    f_ax2 = fig.add_subplot(spec[1, 0])

    f_ax1.plot(df["close"])
    f_ax2.plot(df["STD_50"])

    fig.savefig(file_path)


def plot_adx(df, file_path):
    """
    This plots SD
    """
    fig = plt.figure(constrained_layout=True)
    spec = fig.add_gridspec(ncols=1, nrows=2)

    f_ax1 = fig.add_subplot(spec[0, 0])
    f_ax2 = fig.add_subplot(spec[1, 0])

    f_ax1.plot(df["close"])
    f_ax2.plot(df["trend_adx"])

    fig.savefig(file_path)


def plot_aaron(df, file_path):
    """
    This plots Aroon indicator
    """
    fig = plt.figure(constrained_layout=True)
    spec = fig.add_gridspec(ncols=1, nrows=4)

    f_ax1 = fig.add_subplot(spec[0, 0], title="Close")
    f_ax2 = fig.add_subplot(spec[1, 0], title="Aroon Indicator")
    f_ax3 = fig.add_subplot(spec[2, 0], title="Aroon Indicator Down")
    f_ax4 = fig.add_subplot(spec[3, 0], title="Aroon Indicator Up")

    f_ax1.plot(df["close"])
    f_ax2.plot(df["trend_aroon_ind"])
    f_ax3.plot(df["trend_aroon_down"])
    f_ax4.plot(df["trend_aroon_up"])

    fig.savefig(file_path)


def get_current_tech_analysis(df):
    """
    This function returns the latest values of the technical analysis
    """
    columns = df.columns
    latest_value_dict = {}
    for column in columns:
        latest_value_dict[column] = df[column].iloc[-1]

    return latest_value_dict


def plot_graphs(df, file_path):
    plot_ema(df, file_path + "ema_indicator.png")  # trend
    plot_macd(df, file_path + "macd+indicator.png")  # trend
    plot_rsi(df, file_path + "rsi_indicator.png")  # momentum
    plot_sma(df, file_path + "sma_indicator.png")  # trend
    plot_bb(df, file_path + "bb_indicator.png")  # volatility
    plot_sd(df, file_path + "sd_indicator.png")  # volatility
    plot_adx(df, file_path + "adx_indicator.png")  # trend
    plot_aaron(df, file_path + "aaron.png")  # trend


def trading_strategy(latest_ta_value):

    bearish = 0
    bullish = 0
    neutral = 0
    analysis_dict = {}
    indicators_dict = {}
    bullish_indicators = []
    bearish_indicators = []
    neutral_indicators = []
    analyis_report_list = []

    """
    TREND : Based on EMA , SMA and MACD.

    """
    # If MAI is positive it is already in a bullish market and look for a drop in price, if Fast MA overcomes Slow MA from down then it is Bullish.
    # if Slow MA overcomes Fast MA from down then it is Bearish.

    exponential_moving_average_indicator = (
        latest_ta_value["trend_ema_fast"] - latest_ta_value["trend_ema_slow"]
    )

    if latest_ta_value["trend_ema_fast"] > latest_ta_value["trend_ema_slow"]:
        analyis_report_list.append("The market is bullish based on EMA. ")
        bullish = bullish + 1
        bullish_indicators.append("EMA")

    else:
        analyis_report_list.append("The market is bearish based on EMA. ")
        bearish = bearish + 1
        bearish_indicators.append("EMA")

    indicators_dict["EMA Fast"] = latest_ta_value["trend_ema_fast"]
    indicators_dict["EMA Slow"] = latest_ta_value["trend_ema_slow"]
    indicators_dict["EMA Diff"] = exponential_moving_average_indicator

    moving_average_indicator = (
        latest_ta_value["trend_sma_fast"] - latest_ta_value["trend_sma_slow"]
    )

    if latest_ta_value["trend_sma_fast"] > latest_ta_value["trend_sma_slow"]:
        analyis_report_list.append("The market is bullish based on SMA. ")
        bullish = bullish + 1
        bullish_indicators.append("SMA")

    else:
        analyis_report_list.append("The market is bearish based on SMA. ")
        bearish = bearish + 1
        bullish_indicators.append("SMA")

    indicators_dict["SMA Fast"] = latest_ta_value["trend_sma_fast"]
    indicators_dict["SMA Slow"] = latest_ta_value["trend_sma_slow"]
    indicators_dict["SMA Diff"] = moving_average_indicator

    """
    MACD is the abbreviation of moving average convergence/divergence
    The MACD histogram will help you determine who is stronger – the bulls or the bears?
    Moreover, this indicator shows if the current market sentiment is getting better or worse.
    """

    if latest_ta_value["trend_macd_diff"] > 0:
        analyis_report_list.append("The market is bullish in the time interval. ")
        bullish = bullish + 1
        bullish_indicators.append("MACD")
    else:
        analyis_report_list.append("The market is bearish in the time interval. ")
        bearish = bearish + 1
        bearish_indicators.append("MACD")

    indicators_dict["MACD Diff"] = latest_ta_value["trend_macd_diff"]
    indicators_dict["MACD (26)"] = latest_ta_value["trend_macd"]
    indicators_dict["MACD Signal(9)"] = latest_ta_value["trend_macd_signal"]

    """
    Volume : Based on Price , Volume and On Balance Volume. ---> Refine this strategy

    """

    latest_money_flow_index = latest_ta_value["volume_mfi"]
    indicators_dict["Price"] = latest_ta_value["close"]
    indicators_dict["VOLUME_CURRENCY"] = latest_ta_value["VOLUME_CURRENCY"]
    indicators_dict["On Balance Volume"] = latest_ta_value["volume_obv"]
    indicators_dict["Money Flow Index"] = latest_ta_value["volume_mfi"]
    indicators_dict["Accumulation Distribution Index"] = latest_ta_value["volume_adi"]

    """
    Find a way to indicate the market based on current and previous Price, Volume and OBV.
    MFI measures the flow of money into a security, whether that money is positive or negative.
    RSI compares the magnitude of a stock's recent gains to its recent losses.
    """

    # https://currency.com/how-to-read-and-use-the-on-balance-volume-trading-indicator
    if latest_money_flow_index >= 80:
        analyis_report_list.append(
            "Based on MFI the asset is overbought. LOOK OUT! The price is expected to be bearish. "
        )
        bullish = bullish + 1
        bullish_indicators.append("MFI")
    elif latest_money_flow_index <= 20:
        analyis_report_list.append(
            "Based on MFI the asset is oversold. LOOK OUT! The price is expected to be bullish. "
        )
        bearish = bearish + 1
        bearish_indicators.append("MFI")
    else:
        analyis_report_list.append(
            "Based on MFI the price is within the range of 20-80. Look at the value. "
        )
        neutral = neutral + 1
        neutral_indicators.append("MFI")

    """
    MOMENTUM : Based on RSI.

    """

    if latest_ta_value["momentum_rsi"] >= 70:
        analyis_report_list.append(
            "Based on RSI currently Bullish and expected to be bearish. LOOK OUT! The coin is currently overbought and a potential fall in the price is expected. "
        )
        bullish = bullish + 1
        bullish_indicators.append("RSI")

    elif latest_ta_value["momentum_rsi"] <= 30:
        analyis_report_list.append(
            "Based on RSI currently Bearish and expected to be Bullish. LOOK OUT! The coin is currently oversold and a potential rise in the price is expected. "
        )
        bearish = bearish + 1
        bearish.append("RSI")

    else:
        analyis_report_list.append(
            "The RSI is within the band of 30 - 70. Indicating a good momentum in the current price. "
        )
        neutral = neutral + 1
        neutral_indicators.append("RSI")
    indicators_dict["Relative Strength Index"] = latest_ta_value["momentum_rsi"]

    """
    VOLATILITY : Based on Bollingers Band.
    """
    """
    Find an optimal threshold value for defining lower and higher gaps in the band.
    """

    indicators_dict["Bollingers Band High"] = latest_ta_value["volatility_bbh"]
    indicators_dict["Bollingers Band Middle"] = latest_ta_value["volatility_bbm"]
    indicators_dict["Bollingers Band Low"] = latest_ta_value["volatility_bbl"]
    analyis_report_list.append(
        "If the band values are closer then the volatility is low , else the price volatility is High.Which is subjected to increase or decrease. "
    )

    analysis_dict["Bullish"] = bullish
    analysis_dict["Bearish"] = bearish
    analysis_dict["Neutral"] = neutral
    analysis_dict["Indicators"] = indicators_dict
    analysis_dict["Report"] = "".join(analyis_report_list)
    analysis_dict["Bullish Indicators"] = bullish_indicators
    analysis_dict["Bearish Indicators"] = bearish_indicators
    analysis_dict["Neutral_indicators"] = neutral_indicators

    return analysis_dict


def main():

    # "./main/Data/ohlc-hourly/parquet/" "4H" "./main/graphs/" "./main/output/analysis_output.json"
    coin_historic_data_file_path = sys.argv[1]
    Freq = sys.argv[2]  # for resampling into daily data
    graph_file_path = sys.argv[3]
    analysis_output_file_path = sys.argv[4]
    coin_df = read_parquet_file(coin_historic_data_file_path)
    coin_interval_df = preprocess(coin_df, Freq)
    Time = coin_df["START_TIME_UNIX_TS"].iloc[-1]
    coin_interval_tech_analysis_df = technical_analysis(coin_interval_df)
    latest_ta_value = get_current_tech_analysis(coin_interval_tech_analysis_df)
    plot_graphs(coin_interval_tech_analysis_df, graph_file_path)
    coin_analysis_dict = trading_strategy(latest_ta_value)
    coin_analysis_dict["Time"] = str(Time)
    with open(analysis_output_file_path, "w") as out:
        json.dump(coin_analysis_dict, out)


if __name__ == "__main__":
    main()
