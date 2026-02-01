# forecast_utils.py
import pandas as pd
import numpy as np
from keras.models import Sequential
from keras.layers import LSTM, Dense

def build_forecast_model(data: pd.Series, window=6, future=6):
    X, y = [], []
    for i in range(len(data) - window):
        X.append(data[i:i+window])
        y.append(data[i+window])
    X, y = np.array(X), np.array(y)
    X = X.reshape((X.shape[0], X.shape[1], 1))

    model = Sequential()
    model.add(LSTM(64, input_shape=(window, 1)))
    model.add(Dense(1))
    model.compile(loss='mae', optimizer='adam')
    model.fit(X, y, epochs=30, batch_size=8, verbose=0)

    forecast_input = np.array(data[-window:], dtype=np.float32).reshape((1, window, 1))

    predictions = []
    for _ in range(future):
        pred = model.predict(forecast_input)[0][0]
        predictions.append(pred)
        forecast_input = np.roll(forecast_input, -1)
        forecast_input[0, -1, 0] = pred

    return predictions
