import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from sklearn.preprocessing import LabelEncoder

from plugins.database import select_data


def load_game_data(table: str):
    """Load game data from the database and preprocess features and target."""
    df = select_data(table, "analytics")
    df["HOME_WIN"] = (df["HOME_SCORE"] > df["AWAY_SCORE"]).astype(int)

    feature_template = [
        "{team}_NM_LBL", "{team}_RANK", "{team}_PA", "{team}_AB", "{team}_R", "{team}_H", 
        "{team}_2B", "{team}_3B", "{team}_HR", "{team}_RBI", "{team}_SB", "{team}_CS", 
        "{team}_BB", "{team}_HBP", "{team}_SO", "{team}_GDP", "{team}_AVG", "{team}_TBF", 
        "{team}_IP", "{team}_PH", "{team}_PHR", "{team}_PBB", "{team}_PHBP", "{team}_PSO", 
        "{team}_PR", "{team}_ER", "{team}_ERA"
    ]

    encoder = LabelEncoder()
    for team_col in ["HOME_NM", "AWAY_NM"]:
        df[f"{team_col}_LBL"] = encoder.fit_transform(df[team_col])

    df["G_DT_NUM"] = pd.to_datetime(df["G_DT"]).map(lambda x: x.toordinal())

    feature_columns = (
        ["SEASON_ID", "G_DT_NUM"] +
        [col.format(team="HOME") for col in feature_template] +
        [col.format(team="AWAY") for col in feature_template]
    )

    X = df[feature_columns]
    y = df["HOME_WIN"]

    return df, X, y


def plot_confusion_matrices(results_df: pd.DataFrame):
    """Plot confusion matrices for each model in the results DataFrame."""
    plt.figure(figsize=(8, 8))

    for idx, row in results_df.iterrows():
        cm = row["cm"]
        if len(results_df) > 1:
            plt.subplot(2, 2, idx + 1)
        sns.heatmap(
            cm, annot=True, fmt="d", cmap="Blues",
            xticklabels=["Predicted 0", "Predicted 1"],
            yticklabels=["Actual 0", "Actual 1"]
        )
        plt.title(f"{row["name"]} - Confusion Matrix")
        plt.xlabel("Predicted")
        plt.ylabel("Actual")

    plt.tight_layout()
    plt.show()


def plot_roc_curves(results_df: pd.DataFrame):
    """Plot ROC curves for each model in the results DataFrame."""
    plt.figure(figsize=(8, 8))

    for idx, row in results_df.iterrows():
        fpr, tpr, _ = row["roc"]
        if len(results_df) > 1:
            plt.subplot(2, 2, idx + 1)
        plt.plot(fpr, tpr, label=row["name"])
        plt.plot([0, 1], [0, 1], "k--")
        plt.title(f"{row["name"]} - ROC Curve")
        plt.xlabel("False Positive Rate")
        plt.ylabel("True Positive Rate")

    plt.tight_layout()
    plt.show()


def plot_model_metrics(results_df: pd.DataFrame):
    """Plot bar charts for evaluation metrics of multiple models."""
    plt.figure(figsize=(16, 8))

    metric_data = [
        ("Accuracy", results_df["acc"], "lightblue"),
        ("AUC", results_df["auc"], "lightgreen"),
        ("Precision", results_df["prec"], "lightcoral"),
        ("Recall", results_df["rec"], "lightyellow"),
        ("F1 Score", results_df["f1"], "lightpink")
    ]

    for idx, (title, values, color) in enumerate(metric_data):
        plt.subplot(2, 3, idx + 1)
        plt.bar(results_df["name"], values, color=color, alpha=0.7)
        plt.title(title)
        plt.title(f"Model {title}")
        plt.xlabel("Model")
        plt.ylabel(title)

    plt.tight_layout()
    plt.show()

