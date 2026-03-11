import io
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from pdf import time_to_seconds #was created in another file, actually dned up being used here


FEATURE_SPLITS = ["split_50m", "split_100m", "split_150m", "split_200m", "split_250m"]
INTERVAL_COLS  = [f"interval_{c}" for c in FEATURE_SPLITS[1:]]
FEATURE_COLS   = FEATURE_SPLITS + INTERVAL_COLS + ["reaction_time"]
TIME_COLS      = ["split_50m", "split_100m", "split_150m", "split_200m",
                  "split_250m", "split_300m", "split_350m", "final_time"]

# Splits used for the progressive accuracy experiment
PROGRESSIVE_SPLITS = [
    ["split_50m"],
    ["split_50m", "split_100m"],
    ["split_50m", "split_100m", "split_150m"],
    ["split_50m", "split_100m", "split_150m", "split_200m"],
    ["split_50m", "split_100m", "split_150m", "split_200m", "split_250m"],
]


def _normalize_input(data_input):
    """Accept a DataFrame or a CSV filepath string."""
    if isinstance(data_input, pd.DataFrame):
        buf = io.StringIO()
        data_input.to_csv(buf, index=False)
        buf.seek(0)
        return buf
    return data_input


def _convert_times(df: pd.DataFrame) -> pd.DataFrame:
    """Convert all M:SS.ss string columns to float seconds in-place."""
    for col in TIME_COLS:
        if col in df.columns:
            df[col] = df[col].apply(time_to_seconds)
    return df


def _add_intervals(df: pd.DataFrame, splits: list) -> pd.DataFrame:
    """Add lap-by-lap interval columns derived from cumulative splits."""
    for i, col in enumerate(splits[1:], 1):
        df[f"interval_{col}"] = df[col] - df[splits[i - 1]]
    return df


def _build_features(df: pd.DataFrame, splits: list) -> list:
    """Return feature column names for a given set of input splits."""
    interval_cols = [f"interval_{c}" for c in splits[1:]]
    return splits + interval_cols + ["reaction_time"]

def prepare_data(men_input, women_input) -> pd.DataFrame:
    """
    Load, clean, and feature-engineer the combined dataset.
    Only requires the 5 feature splits + final_time to be non-null
    (split_300m / split_350m are often missing and are NOT required).

    Parameters
    ----------
    men_input / women_input : str (CSV path) or pd.DataFrame

    Returns
    -------
    pd.DataFrame ready for modelling, with times in seconds and
    interval features added.
    """
    df = pd.concat([
        pd.read_csv(_normalize_input(men_input)),
        pd.read_csv(_normalize_input(women_input)),
    ], ignore_index=True)

    df = _convert_times(df)

    required = FEATURE_SPLITS + ["final_time"]
    df = df.dropna(subset=required)

    df = _add_intervals(df, FEATURE_SPLITS)

    df["reaction_time"] = (
        pd.to_numeric(df["reaction_time"], errors="coerce")
        .fillna(df["reaction_time"].median())
    )
    df = df.dropna(subset=FEATURE_COLS)
    return df


def train_model(men_input, women_input) -> tuple[RandomForestRegressor, float, pd.Series]:
    """
    Prepare data and train a RandomForestRegressor predicting final_time
    from the first 250m of splits.

    Parameters
    ----------
    men_input / women_input : str (CSV path) or pd.DataFrame

    Returns
    -------
    model        : fitted RandomForestRegressor
    mae          : mean absolute error on held-out test set (seconds)
    importances  : pd.Series of feature importances, sorted descending
    """
    df = prepare_data(men_input, women_input)

    X = df[FEATURE_COLS]
    y = df["final_time"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    mae = mean_absolute_error(y_test, model.predict(X_test))
    importances = (
        pd.Series(model.feature_importances_, index=FEATURE_COLS)
        .sort_values(ascending=False)
    )

    print(f"Average error: {mae:.2f} seconds")
    print(f"\nTop features:\n{importances.head(8)}")
    return model, mae, importances


def predict(model: RandomForestRegressor, splits) -> float:
    """
    Predict final_time for a single swimmer.

    Parameters
    ----------
    model  : fitted model from train_model()
    splits : dict or pd.Series with keys:
               split_50m, split_100m, split_150m, split_200m, split_250m,
               reaction_time — all as floats (seconds).
             Strings like '1:52.53' are auto-converted.

    Returns
    -------
    Predicted final time in seconds (float).

    Example
    -------
    >>> predict(model, {
    ...     "split_50m": 26.5, "split_100m": 55.1, "split_150m": 84.0,
    ...     "split_200m": 113.2, "split_250m": 142.8, "reaction_time": 0.72
    ... })
    """
    row = dict(splits)

    # Auto-convert any string times (e.g. '1:52.53' -> 112.53)
    for col in FEATURE_SPLITS:
        if isinstance(row.get(col), str):
            row[col] = time_to_seconds(row[col])

    row.setdefault("reaction_time", 0.72)  # median fallback if not provided

    for i, col in enumerate(FEATURE_SPLITS[1:], 1):
        row[f"interval_{col}"] = row[col] - row[FEATURE_SPLITS[i - 1]]

    X = pd.DataFrame([row])[FEATURE_COLS]
    return round(float(model.predict(X)[0]), 2)


def predict_batch(model: RandomForestRegressor, df: pd.DataFrame) -> pd.DataFrame:
    """
    Run predictions across every row of a prepared DataFrame and attach residuals.
    Use this for pacing analysis across the full dataset.

    Parameters
    ----------
    model : fitted model from train_model()
    df    : DataFrame from prepare_data() (times in seconds, intervals added)

    Returns
    -------
    df copy with two new columns:
        predicted_time : model's predicted final time (seconds)
        residual       : predicted - actual
                         positive = model over-predicted = swimmer finished stronger than expected
                         negative = model under-predicted = swimmer faded worse than expected
    """
    out = df.copy()
    out["predicted_time"] = model.predict(out[FEATURE_COLS])
    out["residual"] = out["predicted_time"] - out["final_time"]
    return out


def progressive_accuracy_experiment(men_input, women_input) -> pd.DataFrame:
    """
    Train 5 separate models using increasingly more splits (50m through 250m)
    and return the MAE at each stage.

    This answers: "At what point does the model become accurate?"
    The result is intended to be plotted (splits_used vs mae).

    Parameters
    ----------
    men_input / women_input : str (CSV path) or pd.DataFrame

    Returns
    -------
    pd.DataFrame with columns:
        splits_used : label of the furthest split used (e.g. 'split_100m')
        n_features  : total number of features at that stage
        mae         : mean absolute error in seconds
    """
    df = prepare_data(men_input, women_input)
    y  = df["final_time"]
    results = []

    for splits in PROGRESSIVE_SPLITS:
        df_stage     = _add_intervals(df.copy(), splits)
        feature_cols = _build_features(df_stage, splits)

        X = df_stage[feature_cols]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        m = RandomForestRegressor(n_estimators=100, random_state=42)
        m.fit(X_train, y_train)
        mae = mean_absolute_error(y_test, m.predict(X_test))

        results.append({
            "splits_used": splits[-1],
            "n_features":  len(feature_cols),
            "mae":         round(mae, 3),
        })
        print(f"  Up to {splits[-1]:>15s} → MAE = {mae:.3f}s")

    return pd.DataFrame(results)