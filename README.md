# cryptoAnalysis

Prerequisite: 
  1. Crypto currency OHLCV data in spark Parquet format
  2. TA python library

cd into the src folder
Command to run the program :
    python crypto_analysis.py "./main/Data/ohlc-hourly/parquet/" "4H" "./main/graphs/" "./main/output/analysis_output.json"

First argument : Input OHLCV data in spark Parquet format
Second argument : Time interval to resample the data
Third argument : File path to save the graphs
Fourth argument : File path to save the output 

Sample output : 

{'Bearish': 1,
 'Bullish': 2,
 'Indicators': {'Accumulation Distribution Index': 133599709.0993388,
                'Bollingers Band High': 706.7414536690325,
                'Bollingers Band Low': 625.5037844262052,
                'Bollingers Band Middle': 666.1226190476189,
                'EMA Diff': 2.875257111623114,
                'EMA Fast': 659.9249773366723,
                'EMA Slow': 657.0497202250492,
                'MACD (26)': 2.875257111623114,
                'MACD Diff': -1.6943742719074448,
                'MACD Signal(9)': 4.569631383530559,
                'Money Flow Index': 44.89474010679928,
                'On Balance Volume': 254868716.28738856,
                'Price': 654.6984126984127,
                'Relative Strength Index': 49.292083210716385,
                'SMA Diff': 10.081908831910368,
                'SMA Fast': 662.8326719576729,
                'SMA Slow': 652.7507631257625,
                'VOLUME_CURRENCY': 3323232.904761905},
 'Neutral': 2,
 'Report': 'The market is bullish based on EMA. The market is bullish based on '
           'SMA. The market is bearish in the time interval. The price is '
           'within the range of 20-80. Look at the value. The RSI is within '
           'the band of 30 - 70. Indicating a good momentum in the current '
           'price. If the band values are closer then the volatility is low , '
           'else the price volatility is High.Which is subjected to increase '
           'or decrease. '}
