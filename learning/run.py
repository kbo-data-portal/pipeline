import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

import joblib
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.metrics import confusion_matrix, roc_curve, auc, precision_score, recall_score, f1_score

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier

from xgboost import XGBClassifier
from lightgbm import LGBMClassifier

RANDOM_STATE = 1982


def load_and_preprocess_data(table="ml_game_dataset") -> tuple:
    """Load dataset from PostgreSQL, preprocess it, and split into train/test sets."""
    engine = create_engine("postgresql://postgres:postgres@localhost:5432/postgres")
    df = pd.read_sql(f"SELECT * FROM analytics.{table};", engine)
    
    df["HOME_WIN"] = (df["HOME_SCORE"] > df["AWAY_SCORE"]).astype(int)

    feature_cols = [
        "{team}_NM_LBL", "{team}_RANK", "{team}_PA", "{team}_AB", "{team}_R", "{team}_H", 
        "{team}_2B", "{team}_3B", "{team}_HR", "{team}_RBI", "{team}_SB", "{team}_CS", 
        "{team}_BB", "{team}_HBP", "{team}_SO", "{team}_GDP", "{team}_AVG", "{team}_TBF", 
        "{team}_IP", "{team}_PH", "{team}_PHR", "{team}_PBB", "{team}_PHBP", "{team}_PSO", 
        "{team}_PR", "{team}_ER", "{team}_ERA"
    ]

    le = LabelEncoder()
    for col in ["HOME_NM", "AWAY_NM"]:
        df[f"{col}_LBL"] = le.fit_transform(df[col])

    df["G_DT_NUM"] = pd.to_datetime(df["G_DT"]).map(lambda x: x.toordinal())

    X = df[["SEASON_ID", "G_DT_NUM"] + 
           [col.format(team="HOME") for col in feature_cols] + 
           [col.format(team="AWAY") for col in feature_cols]]
    y = df["HOME_WIN"]

    return df, X, y


def train_and_evaluate_models(X_train, X_test, y_train, y_test) -> pd.DataFrame:
    """Train multiple classifiers, evaluate them, and return accuracy/AUC lists."""
    medels = {
        "Logistic Regression": LogisticRegression(max_iter=10000, solver="liblinear", random_state=RANDOM_STATE),
        "Random Forest": RandomForestClassifier(n_estimators=100, random_state=RANDOM_STATE),
        "XGBoost": XGBClassifier(eval_metric="logloss", random_state=RANDOM_STATE),
        "LightGBM": LGBMClassifier(random_state=RANDOM_STATE)
    }
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=RANDOM_STATE)

    results = []
    for name, model in medels.items():
        train_model = model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        y_probs = model.predict_proba(X_test)[:, 1]

        acc_score = cross_val_score(model, X_train, y_train, cv=cv, scoring="accuracy")
        auc_score = cross_val_score(model, X_train, y_train, cv=cv, scoring="roc_auc")

        cm = confusion_matrix(y_test, y_pred)

        fpr, tpr, thresholds = roc_curve(y_test, y_probs)
        roc_auc = auc(fpr, tpr)

        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)

        results.append({
            "Model": name,
            "Accuracy Mean": acc_score.mean(),
            "Accuracy Std": acc_score.std(),
            "AUC Mean": auc_score.mean(),
            "AUC Std": auc_score.std(),
            "Precision": precision,
            "Recall": recall,
            "F1 Score": f1,
            "Confusion Matrix": cm,
            "ROC Curve": [fpr, tpr, roc_auc]
        })
        joblib.dump(train_model, f"{name.lower().replace(" ", "_")}.joblib")
        
    results = pd.DataFrame(results)
    return results.sort_values(by="F1 Score", ascending=False)


def plot_model_performance(results) -> None:
    """Plot bar charts comparing model accuracies, AUC scores, and other metrics."""

    confusion_matrix = results[["Model", "Confusion Matrix"]]
    plt.figure(figsize=(12, 12))

    for i, row in confusion_matrix.iterrows():
        cm = row["Confusion Matrix"]
        plt.subplot(2, 2, i + 1)
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", 
                    xticklabels=["Predicted 0", "Predicted 1"], 
                    yticklabels=["Actual 0", "Actual 1"])
        plt.title(f"{row["Model"]} Confusion Matrix")
        plt.xlabel("Predicted")
        plt.ylabel("Actual")

    plt.show()
    
    roc_curve = results[["Model", "ROC Curve"]]
    plt.figure(figsize=(12, 12))

    for i, row in roc_curve.iterrows():
        plt.subplot(2, 2, i + 1)
        rc = row["ROC Curve"]
        plt.plot(rc[0], rc[1])
        plt.plot([0, 1], [0, 1], "k--")
        plt.xlabel("False Positive Rate")
        plt.ylabel("True Positive Rate")
        plt.title(f"{row["Model"]} ROC Curve")

    plt.show()

    x_labels = results["Model"]
    accuracy_means = results["Accuracy Mean"]
    auc_means = results["AUC Mean"]
    precision = results["Precision"]
    recall = results["Recall"]
    f1_score = results["F1 Score"]

    plt.figure(figsize=(16, 8))

    plt.subplot(2, 3, 1)
    plt.bar(x_labels, accuracy_means, color="lightblue", alpha=0.7)
    plt.title("Model Accuracy Comparison")
    plt.xlabel("Model")
    plt.ylabel("Accuracy")

    plt.subplot(2, 3, 2)
    plt.bar(x_labels, auc_means, color="lightgreen", alpha=0.7)
    plt.title("Model AUC Comparison")
    plt.xlabel("Model")
    plt.ylabel("AUC")

    plt.subplot(2, 3, 3)
    plt.bar(x_labels, precision, color="lightcoral", alpha=0.7)
    plt.title("Model Precision Comparison")
    plt.xlabel("Model")
    plt.ylabel("Precision")

    plt.subplot(2, 3, 4)
    plt.bar(x_labels, recall, color="lightyellow", alpha=0.7)
    plt.title("Model Recall Comparison")
    plt.xlabel("Model")
    plt.ylabel("Recall")

    plt.subplot(2, 3, 5)
    plt.bar(x_labels, f1_score, color="lightpink", alpha=0.7)
    plt.title("Model F1 Score Comparison")
    plt.xlabel("Model")
    plt.ylabel("F1 Score")

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    df, X, y = load_and_preprocess_data()
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=RANDOM_STATE)

    results = train_and_evaluate_models(X_train, X_test, y_train, y_test)

    model = joblib.load(f"{results.loc[0, "Model"].lower().replace(" ", "_")}.joblib")
    df, X, y = load_and_preprocess_data("ml_game_predict")

    predictions = model.predict(X)
    df["Predictions"] = predictions

    plot_model_performance(results)

