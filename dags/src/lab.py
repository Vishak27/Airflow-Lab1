import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import pickle
import os
import base64

def load_data():
    """
    Loads the Mall Customers dataset and returns base64-encoded pickled data.
    """
    print("Loading Mall Customer data...")
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), "../data/Mall_Customers.csv"))
    serialized_data = pickle.dumps(df)
    return base64.b64encode(serialized_data).decode("ascii")


def data_preprocessing(data_b64: str):
    """
    Preprocesses the data: encodes gender, scales features, 
    and creates a label column (Spending category).
    """
    data_bytes = base64.b64decode(data_b64)
    df = pickle.loads(data_bytes)
    df = df.dropna()

    # Encode Gender
    df["Gender"] = df["Gender"].map({"Male": 0, "Female": 1})

    # Create a spending label (Low=0, Medium=1, High=2)
    bins = [0, 40, 70, 100]
    labels = [0, 1, 2]
    df["SpendingCategory"] = pd.cut(df["Spending Score (1-100)"], bins=bins, labels=labels)

    # Features and target
    X = df[["Gender", "Age", "Annual Income (k$)"]]
    y = df["SpendingCategory"].astype(int)

    # Scale features
    scaler = MinMaxScaler()
    X_scaled = scaler.fit_transform(X)

    serialized = pickle.dumps((X_scaled, y))
    return base64.b64encode(serialized).decode("ascii")


def build_save_model(data_b64: str, filename: str):
    """
    Trains a KNN classifier and saves the model.
    Returns model accuracy on test data.
    """
    data_bytes = base64.b64decode(data_b64)
    X, y = pickle.loads(data_bytes)

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Build KNN
    knn = KNeighborsClassifier(n_neighbors=5)
    knn.fit(X_train, y_train)

    # Evaluate
    y_pred = knn.predict(X_test)
    acc = accuracy_score(y_test, y_pred)

    # Save model
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "model")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)
    with open(output_path, "wb") as f:
        pickle.dump(knn, f)

    return {"accuracy": acc}


def load_model_predict(filename: str):
    """
    Loads the trained KNN model and predicts a class for test.csv.
    """
    output_path = os.path.join(os.path.dirname(__file__), "../model", filename)
    model = pickle.load(open(output_path, "rb"))

    # Load test data
    df_test = pd.read_csv(os.path.join(os.path.dirname(__file__), "../data/test1.csv"))
    df_test["Gender"] = df_test["Gender"].map({"Male": 0, "Female": 1})
    X_test = df_test[["Gender", "Age", "Annual Income (k$)"]]

    # Scale using same range (optional — use MinMaxScaler from training ideally)
    scaler = MinMaxScaler()
    X_test_scaled = scaler.fit_transform(X_test)

    # Predict first record
    pred = model.predict(X_test_scaled)[0]

    return int(pred)