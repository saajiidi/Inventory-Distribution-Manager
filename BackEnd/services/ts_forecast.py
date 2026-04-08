import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import warnings

warnings.filterwarnings("ignore")

def generate_forecasts(daily_df: pd.DataFrame, metric: str = "revenue", horizon: int = 7) -> dict:
    if len(daily_df) < 14:
        return {"error": "Insufficient data points for robust ML prediction. Switch to 'Last Month' or a longer Custom Date Range (needs at least 14 days of history)."}
    
    df = daily_df.sort_values("order_day").copy()
    df.set_index("order_day", inplace=True)
    # Fill missing days
    df = df.asfreq('D', fill_value=0)
    y = df[metric].astype(float)
    
    # Train/Test Split to find the best model
    test_size = 7 if len(y) > 28 else (3 if len(y) > 14 else 0)
    
    models_predictions = {}
    future_dates = pd.date_range(start=y.index[-1] + pd.Timedelta(days=1), periods=horizon, freq='D')
    
    # 1. ARIMA
    try:
        model_arima = ARIMA(y, order=(2, 1, 0)) 
        fit_arima = model_arima.fit()
        fc_arima = fit_arima.forecast(steps=horizon)
        models_predictions["ARIMA"] = fc_arima.clip(lower=0)
    except Exception:
        pass
        
    # 2. SARIMA
    try:
        model_sarima = SARIMAX(y, order=(1, 1, 1), seasonal_order=(1, 1, 0, 7))
        fit_sarima = model_sarima.fit(disp=False)
        fc_sarima = fit_sarima.forecast(steps=horizon)
        models_predictions["SARIMA"] = fc_sarima.clip(lower=0)
    except Exception:
        pass
        
    # 3. Exponential Smoothing (Holt-Winters)
    try:
        seasonal_periods = min(7, len(y) // 2)
        model_hw = ExponentialSmoothing(y, seasonal='add', seasonal_periods=seasonal_periods, trend='add', initialization_method="estimated")
        fit_hw = model_hw.fit()
        fc_hw = fit_hw.forecast(horizon)
        models_predictions["Holt-Winters"] = fc_hw.clip(lower=0)
    except Exception:
        pass
        
    # 4. Linear Trend
    try:
        X = np.arange(len(y)).reshape(-1, 1)
        lr = LinearRegression()
        lr.fit(X, y)
        X_pred = np.arange(len(y), len(y) + horizon).reshape(-1, 1)
        fc_lr = pd.Series(lr.predict(X_pred), index=future_dates)
        models_predictions["Linear Trend"] = fc_lr.clip(lower=0)
    except Exception:
        pass

    if not models_predictions:
         return {"error": "All ML models failed to converge on this dataset. Try selecting a broader time window."}
         
    # To find "best", evaluate on the trailing history
    best_model = None
    best_mse = float('inf')
    
    if test_size > 0:
        y_train, y_test = y.iloc[:-test_size], y.iloc[-test_size:]
        for name in list(models_predictions.keys()):
            try:
                if name == "ARIMA":
                    m = ARIMA(y_train, order=(2,1,0)).fit()
                    preds = m.forecast(steps=test_size)
                elif name == "SARIMA":
                    m = SARIMAX(y_train, order=(1,1,1), seasonal_order=(1,1,0,7)).fit(disp=False)
                    preds = m.forecast(steps=test_size)
                elif name == "Holt-Winters":
                    # Must ensure len > 2 * periods
                    sp = min(7, len(y_train) // 2 - 1)
                    if sp < 2: raise ValueError()
                    m = ExponentialSmoothing(y_train, seasonal='add', seasonal_periods=sp, trend='add', initialization_method="estimated").fit()
                    preds = m.forecast(test_size)
                else:
                    lr_m = LinearRegression().fit(np.arange(len(y_train)).reshape(-1,1), y_train)
                    preds = lr_m.predict(np.arange(len(y_train), len(y_train)+test_size).reshape(-1,1))
                
                mse = mean_squared_error(y_test, preds)
                if mse < best_mse:
                    best_mse = mse
                    best_model = name
            except:
                continue

    if not best_model and models_predictions:
        best_model = list(models_predictions.keys())[0]

    return {
        "history": y,
        "forecasts": models_predictions,
        "best_model": best_model
    }
