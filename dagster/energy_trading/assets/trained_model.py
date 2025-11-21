"""Trained model asset - trains ML model to predict energy prices"""

import joblib
import pandas as pd
from dagster import asset, AssetExecutionContext
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split


@asset(deps=["weather_data"])
def trained_model(
    context: AssetExecutionContext, weather_data: pd.DataFrame
) -> LinearRegression:
    """Train a simple model to predict energy price based on weather"""
    X = weather_data[["temperature", "humidity", "wind_speed"]]
    y = weather_data["energy_price"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    model = LinearRegression()
    model.fit(X_train, y_train)

    joblib.dump(model, "/tmp/energy_model.pkl")

    score = model.score(X_test, y_test)
    context.log.info(
        f"Model trained with R² score: {score:.4f} on {len(X_train)} training samples"
    )

    return model
