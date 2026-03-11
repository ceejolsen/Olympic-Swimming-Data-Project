import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from pdf import time_to_seconds #was originally coded in the other file, ended up actually using it here


FEATURE_SPLITS = ["split_50m", "split_100m", "split_150m", "split_200m", "split_250m"]
INTERVAL_COLS = [f"interval_{c}" for c in FEATURE_SPLITS[1:]]
FEATURE_COLS = FEATURE_SPLITS + INTERVAL_COLS + ["reaction_time"]
TIME_COLS  = ["split_50m", "split_100m", "split_150m", "split_200m",
                   "split_250m", "split_300m", "split_350m", "final_time"]


def train_model(men_input, women_input) -> tuple[RandomForestRegressor, float, pd.Series]:
    """
    Load scraped swim results, engineer features, and train a
    RandomForestRegressor to predict final_time from the 250m splits.
    Parameters
        men_csv: path to the men's scraped results CSV
        women_csv: path to the women's scraped results CSV
    
    Return arguments
    tuple:
        (model: fitted RandomForestRegressor
        mae: mean absolute error on the held-out test set (seconds)
        importances  : pd.Series of feature importances, sorted descending)
    """
    def normalize_input(data_input):
        if isinstance(data_input, pd.DataFrame):
            # Convert DataFrame to a CSV-formatted buffer
            buf = io.StringIO()
            data_input.to_csv(buf, index=False)
            buf.seek(0)
            return buf
        return data_input

    men_processed = normalize_input(men_input)
    women_processed = normalize_input(women_input)

    # Load and concatenate
    df = pd.concat([
        pd.read_csv(men_processed), 
        pd.read_csv(women_processed)
    ], ignore_index=True)

    # ── 2. Convert time strings → float seconds ──────────────────────────────
    for col in TIME_COLS:
        df[col] = df[col].apply(time_to_seconds)

    df = df.dropna(subset=TIME_COLS)

    # ── 3. Engineer interval (lap-by-lap pace) features ──────────────────────
    for i, col in enumerate(FEATURE_SPLITS[1:], 1):
        df[f"interval_{col}"] = df[col] - df[FEATURE_SPLITS[i - 1]]

    df["reaction_time"] = (pd.to_numeric(df["reaction_time"], errors="coerce")
                           .fillna(df["reaction_time"].median()))
    df = df.dropna(subset=FEATURE_COLS)

    # ── 4. Train / test split ────────────────────────────────────────────────
    X = df[FEATURE_COLS]
    y = df["final_time"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # ── 5. Train & evaluate ──────────────────────────────────────────────────
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    mae = mean_absolute_error(y_test, model.predict(X_test))
    importances = pd.Series(model.feature_importances_, index=FEATURE_COLS).sort_values(ascending=False)

    print(f"Average error: {mae:.2f} seconds")
    print(f"\nTop features:\n{importances.head(8)}")

    return model, mae, importances


def predict(model: RandomForestRegressor, splits: dict) -> float:
    """
    Predict final_time for a single swimmer given their first 250m splits.

    Parameters
    ----------
    model  : fitted model returned by train_model()
    splits : dict with keys — split_50m, split_100m, split_150m,
             split_200m, split_250m, reaction_time (all in seconds)

    Returns
    -------
    Predicted final time in seconds.

    Example
    -------
    >>> predict(model, {
    ...     "split_50m": 26.5, "split_100m": 55.1, "split_150m": 1*60+24.0,
    ...     "split_200m": 1*60+53.2, "split_250m": 2*60+22.8, "reaction_time": 0.72
    ... })
    """
    row = dict(splits)
    for i, col in enumerate(FEATURE_SPLITS[1:], 1):
        row[f"interval_{col}"] = row[col] - row[FEATURE_SPLITS[i - 1]]

    X = pd.DataFrame([row])[FEATURE_COLS]
    return round(float(model.predict(X)[0]), 2)