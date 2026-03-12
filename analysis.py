import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def format_seconds(seconds):
    """Convert seconds to readable seconds + mm:ss.ss format."""
    minutes = int(seconds // 60)
    remainder = seconds % 60
    return f"{seconds:.2f}s ({minutes}:{remainder:05.2f})"


def run_prediction_example(model, predict_fn):
    """Run a sample prediction using early split data."""
    example = {
        "split_50m": 26.69,
        "split_100m": 55.38,
        "split_150m": 83.74,
        "split_200m": 112.53,
        "split_250m": 140.97,
        "reaction_time": 0.73,
    }

    predicted = predict_fn(model, example)
    print(f"Predicted final time: {format_seconds(predicted)}")


def print_biggest_surprises(df_results, n=5):
    """Print swimmers whose finishes were strongest relative to prediction."""
    if "residual" not in df_results.columns:
        raise ValueError("df_results must contain a 'residual' column.")

    print("\nStrongest finishers (model underestimated them most):")
    print(
        df_results.nlargest(n, "residual")[
            ["last_name", "first_name", "final_time", "predicted_time", "residual"]
        ]
    )


def plot_progressive_accuracy(prog_df):
    """Plot model MAE as more split information becomes available."""
    if "mae" not in prog_df.columns:
        raise ValueError("prog_df must contain a 'mae' column.")

    distances = [50, 100, 150, 200, 250]
    maes = prog_df["mae"].tolist()

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(distances, maes, marker="o", linewidth=2.5)
    ax.fill_between(distances, maes, alpha=0.12)

    for x, y in zip(distances, maes):
        ax.annotate(f"{y:.2f}s", (x, y), textcoords="offset points", xytext=(0, 10), ha="center")

    ax.set_xlabel("Split distance used (m)")
    ax.set_ylabel("MAE (seconds)")
    ax.set_title("Model accuracy vs. how much race data is available")
    ax.set_xticks(distances)
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
    ax.grid(axis="y")

    plt.tight_layout()
    plt.show()


def plot_residual_distribution(df_results):
    """Plot histogram of model residuals."""
    if "residual" not in df_results.columns:
        raise ValueError("df_results must contain a 'residual' column.")

    residuals = df_results["residual"].dropna()

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(residuals, bins=40)
    ax.axvline(0, linestyle="--", label="Perfect prediction")
    ax.set_xlabel("Residual (predicted − actual, seconds)")
    ax.set_ylabel("Number of swimmers")
    ax.set_title("Pacing surprise distribution")
    ax.legend()

    plt.tight_layout()
    plt.show()



def plot_feature_importance(importances):
    """Plot feature importance scores from the trained model."""
    if not hasattr(importances, "sort_values"):
        raise ValueError("importances should be a pandas Series.")

    importances = importances.sort_values(ascending=True)

    fig, ax = plt.subplots(figsize=(8,4))
    colors = plt.cm.viridis(importances.values)
    ax.barh(importances.index, importances.values, color=colors)
    for i, v in enumerate(importances.values):
        ax.text(v + 0.002, i, f"{v:.3f}", va="center")
    ax.set_xlabel("Importance")
    ax.set_ylabel("Feature")
    ax.set_title("Feature importance for predicting 400m freestyle final time")

    
    plt.tight_layout()
    plt.show()


def plot_split_correlations(df_prepared):
    """Plot correlation matrix for split features and final time."""

    cols = [
        "reaction_time",
        "split_50m",
        "split_100m",
        "split_150m",
        "split_200m",
        "split_250m",
        "final_time"
    ]

    available_cols = [c for c in cols if c in df_prepared.columns]
    if len(available_cols) < 2:
        raise ValueError("Not enough columns available to compute correlations.")

    corr = df_prepared[available_cols].corr(numeric_only=True)

    fig, ax = plt.subplots(figsize=(8, 7))
    im = ax.imshow(corr, cmap="coolwarm", vmin=-1, vmax=1)
    for i in range(len(available_cols)):
        for j in range(len(available_cols)):
            ax.text(j, i, f"{corr.iloc[i, j]:.2f}",
                    ha="center", va="center", color="black", fontsize=8)
    ax.set_xticks(range(len(available_cols)))
    ax.set_xticklabels(available_cols, rotation=45, ha="right")
    ax.set_yticks(range(len(available_cols)))
    ax.set_yticklabels(available_cols)

    ax.set_title("Correlation matrix of race features")
    fig.colorbar(im, ax=ax)

    plt.tight_layout()
    plt.show()