import pandas as pd
from sklearn.base import ClassifierMixin
from sklearn.metrics import (
    confusion_matrix, roc_curve, auc,
    precision_score, recall_score, f1_score, accuracy_score, roc_auc_score
)


def train_game_model(
    model: ClassifierMixin,
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series
) -> tuple[ClassifierMixin, dict]:
    """Train a classification model on game data and evaluate performance."""
    train_model = model.fit(X_train, y_train)
    y_pred = train_model.predict(X_test)
    y_proba = train_model.predict_proba(X_test)[:, 1]

    cm = confusion_matrix(y_test, y_pred)
    fpr, tpr, _ = roc_curve(y_test, y_proba)
    roc_auc = auc(fpr, tpr)

    metrics = {
        "acc": accuracy_score(y_test, y_pred),
        "auc": roc_auc_score(y_test, y_proba),
        "prec": precision_score(y_test, y_pred),
        "rec": recall_score(y_test, y_pred),
        "f1": f1_score(y_test, y_pred),
        "cm": cm,
        "roc": [fpr, tpr, roc_auc]
    }

    return train_model, metrics

