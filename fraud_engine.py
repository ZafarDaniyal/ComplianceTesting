import math
import os
import threading
from datetime import datetime

import pandas as pd
import xgboost as xgb

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_MODEL_PATH = os.path.join(BASE_DIR, "data", "fraud", "fraud_xgboost_model.json")
FALLBACK_MODEL_PATH = "/Users/daniyalzafar/python journey/Data Science Road Map/XGboost/fraud_xgboost_model.json"

FEATURES = [
    "amt",
    "category_enc",
    "gender_enc",
    "city_pop",
    "age",
    "hour",
    "day_of_week",
    "month",
    "is_weekend",
    "is_night",
    "distance_from_home",
    "lat",
    "long",
    "merch_lat",
    "merch_long",
    "zip",
]

CATEGORY_CLASSES = [
    "entertainment",
    "food_dining",
    "gas_transport",
    "grocery_net",
    "grocery_pos",
    "health_fitness",
    "home",
    "kids_pets",
    "misc_net",
    "misc_pos",
    "personal_care",
    "shopping_net",
    "shopping_pos",
    "travel",
]
GENDER_CLASSES = ["F", "M"]

DEFAULT_THRESHOLD = 0.90
TRAINING_METRICS = {
    "roc_auc": 0.9970,
    "pr_auc": 0.9565,
    "precision": 0.9691,
    "recall": 0.8571,
    "f1": 0.9097,
    "threshold": DEFAULT_THRESHOLD,
    "true_positives": 282,
    "false_negatives": 47,
    "false_positives": 9,
}

FRIENDLY_FEATURE_NAMES = {
    "amt": "Amount",
    "category_enc": "Merchant category",
    "gender_enc": "Gender code",
    "city_pop": "City population",
    "age": "Customer age",
    "hour": "Hour of day",
    "day_of_week": "Day of week",
    "month": "Month",
    "is_weekend": "Weekend flag",
    "is_night": "Night flag",
    "distance_from_home": "Distance from home",
    "lat": "Home latitude",
    "long": "Home longitude",
    "merch_lat": "Merchant latitude",
    "merch_long": "Merchant longitude",
    "zip": "ZIP code",
}

DAY_NAMES = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

EARTH_RADIUS_MILES = 3958.8

SAMPLE_TRANSACTIONS = [
    {
        "id": "commuter-gas",
        "name": "Commuter Gas Stop",
        "tone": "low",
        "payload": {
            "amount": 48.32,
            "category": "gas_transport",
            "gender": "F",
            "city_pop": 182000,
            "age": 41,
            "transaction_at": "2024-03-12T08:14",
            "zip": 60610,
            "home_lat": 41.9028,
            "home_long": -87.6296,
            "merchant_lat": 41.9042,
            "merchant_long": -87.6371,
        },
    },
    {
        "id": "weekend-grocery",
        "name": "Weekend Grocery Swipe",
        "tone": "low",
        "payload": {
            "amount": 126.77,
            "category": "grocery_pos",
            "gender": "M",
            "city_pop": 94000,
            "age": 36,
            "transaction_at": "2024-04-13T14:27",
            "zip": 78704,
            "home_lat": 30.2500,
            "home_long": -97.7600,
            "merchant_lat": 30.2674,
            "merchant_long": -97.7428,
        },
    },
    {
        "id": "night-shopping",
        "name": "Late-Night Designer Cart",
        "tone": "medium",
        "payload": {
            "amount": 1148.09,
            "category": "shopping_net",
            "gender": "M",
            "city_pop": 172817,
            "age": 36,
            "transaction_at": "2024-05-19T23:41",
            "zip": 91206,
            "home_lat": 34.1556,
            "home_long": -118.2322,
            "merchant_lat": 33.877454,
            "merchant_long": -118.317885,
        },
    },
    {
        "id": "threshold-edge",
        "name": "Threshold Edge Online Cart",
        "tone": "high",
        "payload": {
            "amount": 846.12,
            "category": "shopping_net",
            "gender": "F",
            "city_pop": 123373,
            "age": 26,
            "transaction_at": "2024-01-16T23:02",
            "zip": 64058,
            "home_lat": 39.1412,
            "home_long": -94.3515,
            "merchant_lat": 38.682679,
            "merchant_long": -93.896562,
        },
    },
    {
        "id": "grocery-burst",
        "name": "1:36 AM Grocery Burst",
        "tone": "high",
        "payload": {
            "amount": 334.28,
            "category": "grocery_pos",
            "gender": "F",
            "city_pop": 976,
            "age": 19,
            "transaction_at": "2024-01-08T01:36",
            "zip": 23106,
            "home_lat": 37.7184,
            "home_long": -77.1860,
            "merchant_lat": 37.751241,
            "merchant_long": -78.156340,
        },
    },
]

_CACHE = {
    "mtime": None,
    "path": None,
    "booster": None,
    "summary": None,
}
_LOCK = threading.Lock()


def _candidate_model_paths():
    override = os.environ.get("FRAUD_MODEL_PATH", "").strip()
    candidates = [LOCAL_MODEL_PATH]
    if override:
        candidates.insert(0, override)
    candidates.append(FALLBACK_MODEL_PATH)
    return candidates


def _resolve_model_path():
    for candidate in _candidate_model_paths():
        if candidate and os.path.exists(candidate):
            return candidate
    raise FileNotFoundError("Fraud model JSON was not found")


def _load_booster():
    path = _resolve_model_path()
    mtime = os.path.getmtime(path)
    with _LOCK:
        if _CACHE["booster"] is not None and _CACHE["mtime"] == mtime and _CACHE["path"] == path:
            return _CACHE["booster"]

        booster = xgb.Booster()
        booster.load_model(path)
        _CACHE["mtime"] = mtime
        _CACHE["path"] = path
        _CACHE["booster"] = booster
        _CACHE["summary"] = None
        return booster


def _to_float(value, default):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _to_int(value, default):
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return int(default)


def _clamp(value, low, high):
    return max(low, min(high, value))


def _normalize_category(value):
    candidate = str(value or "").strip().lower()
    if candidate in CATEGORY_CLASSES:
        return candidate
    return "grocery_pos"


def _normalize_gender(value):
    candidate = str(value or "").strip().upper()
    if candidate in GENDER_CLASSES:
        return candidate
    return "F"


def _parse_transaction_time(value):
    text = str(value or "").strip()
    if not text:
        return datetime(2024, 3, 12, 14, 15)
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return datetime(2024, 3, 12, 14, 15)


def _distance(lat, lon, merch_lat, merch_lon):
    return math.sqrt((lat - merch_lat) ** 2 + (lon - merch_lon) ** 2)


def _haversine_miles(lat1, lon1, lat2, lon2):
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS_MILES * c


def _humanize_category(value):
    return str(value or "").replace("_", " ").title()


def _sigmoid(value):
    if value >= 0:
        scaled = math.exp(-value)
        return 1.0 / (1.0 + scaled)
    scaled = math.exp(value)
    return scaled / (1.0 + scaled)


def _risk_band(probability):
    if probability >= DEFAULT_THRESHOLD:
        return "critical"
    if probability >= 0.50:
        return "elevated"
    if probability >= 0.20:
        return "guarded"
    return "routine"


def _encode_transaction(payload):
    amount = round(max(_to_float(payload.get("amount"), 120.0), 1.0), 2)
    category = _normalize_category(payload.get("category"))
    gender = _normalize_gender(payload.get("gender"))
    city_pop = _clamp(_to_int(payload.get("city_pop"), 120000), 500, 10000000)
    age = _clamp(_to_int(payload.get("age"), 38), 18, 95)
    timestamp = _parse_transaction_time(payload.get("transaction_at"))
    zip_code = _clamp(_to_int(payload.get("zip"), 10001), 10000, 99999)

    home_lat = _to_float(payload.get("home_lat"), 40.7128)
    home_long = _to_float(payload.get("home_long"), -74.0060)
    distance_miles_input = max(_to_float(payload.get("distance_miles"), 0.0), 0.0)

    if "merchant_lat" in payload or "merchant_long" in payload:
        merchant_lat = _to_float(payload.get("merchant_lat"), home_lat + 0.03)
        merchant_long = _to_float(payload.get("merchant_long"), home_long - 0.02)
    else:
        merchant_lat = home_lat + (distance_miles_input / 69.0)
        merchant_long = home_long

    hour = timestamp.hour
    day_of_week = timestamp.weekday()
    month = timestamp.month
    is_weekend = 1 if day_of_week >= 5 else 0
    is_night = 1 if hour >= 22 or hour <= 5 else 0
    distance_from_home = _distance(home_lat, home_long, merchant_lat, merchant_long)
    distance_miles = _haversine_miles(home_lat, home_long, merchant_lat, merchant_long)

    encoded_row = {
        "amt": amount,
        "category_enc": CATEGORY_CLASSES.index(category),
        "gender_enc": GENDER_CLASSES.index(gender),
        "city_pop": city_pop,
        "age": age,
        "hour": hour,
        "day_of_week": day_of_week,
        "month": month,
        "is_weekend": is_weekend,
        "is_night": is_night,
        "distance_from_home": distance_from_home,
        "lat": home_lat,
        "long": home_long,
        "merch_lat": merchant_lat,
        "merch_long": merchant_long,
        "zip": zip_code,
    }

    normalized = {
        "amount": amount,
        "category": category,
        "category_label": _humanize_category(category),
        "gender": gender,
        "city_pop": city_pop,
        "age": age,
        "transaction_at": timestamp.isoformat(timespec="minutes"),
        "hour": hour,
        "day_of_week": day_of_week,
        "day_name": DAY_NAMES[day_of_week],
        "month": month,
        "is_weekend": is_weekend,
        "is_night": is_night,
        "zip": zip_code,
        "home_lat": home_lat,
        "home_long": home_long,
        "merchant_lat": merchant_lat,
        "merchant_long": merchant_long,
        "distance_from_home": distance_from_home,
        "distance_miles": distance_miles,
    }
    return encoded_row, normalized


def _feature_value_for_display(feature_name, normalized):
    if feature_name == "category_enc":
        return normalized["category_label"]
    if feature_name == "gender_enc":
        return normalized["gender"]
    if feature_name == "distance_from_home":
        return f"{normalized['distance_miles']:.1f} miles"
    if feature_name == "amt":
        return f"${normalized['amount']:.2f}"
    if feature_name == "city_pop":
        return f"{normalized['city_pop']:,}"
    if feature_name == "age":
        return str(normalized["age"])
    if feature_name == "hour":
        return f"{normalized['hour']:02d}:00"
    if feature_name == "day_of_week":
        return normalized["day_name"]
    if feature_name == "month":
        return str(normalized["month"])
    if feature_name in {"is_weekend", "is_night"}:
        return "Yes" if normalized[feature_name] else "No"
    if feature_name == "zip":
        return str(normalized["zip"])
    if feature_name == "lat":
        return f"{normalized['home_lat']:.4f}"
    if feature_name == "long":
        return f"{normalized['home_long']:.4f}"
    if feature_name == "merch_lat":
        return f"{normalized['merchant_lat']:.4f}"
    if feature_name == "merch_long":
        return f"{normalized['merchant_long']:.4f}"
    return str(normalized.get(feature_name, ""))


def _feature_importance():
    booster = _load_booster()
    gain_scores = booster.get_score(importance_type="gain")
    total_gain = sum(gain_scores.values()) or 1.0
    rows = []
    for feature_name in FEATURES:
        gain = float(gain_scores.get(feature_name, 0.0))
        rows.append(
            {
                "feature": feature_name,
                "label": FRIENDLY_FEATURE_NAMES.get(feature_name, feature_name),
                "gain": round(gain, 6),
                "gain_pct": round(gain / total_gain * 100.0, 2),
            }
        )
    rows.sort(key=lambda item: item["gain"], reverse=True)
    return rows


def score_transaction(payload):
    booster = _load_booster()
    encoded_row, normalized = _encode_transaction(payload)

    frame = pd.DataFrame([[encoded_row[name] for name in FEATURES]], columns=FEATURES)
    matrix = xgb.DMatrix(frame, feature_names=FEATURES)
    probability = float(booster.predict(matrix)[0])
    contributions = booster.predict(matrix, pred_contribs=True)[0].tolist()

    bias = float(contributions[-1])
    feature_contribs = contributions[:-1]
    margin = float(sum(contributions))
    probability_from_margin = _sigmoid(margin)

    details = []
    for feature_name, contribution in zip(FEATURES, feature_contribs):
        details.append(
            {
                "feature": feature_name,
                "label": FRIENDLY_FEATURE_NAMES.get(feature_name, feature_name),
                "value": _feature_value_for_display(feature_name, normalized),
                "contribution": round(float(contribution), 4),
                "impact": "raises fraud risk" if contribution >= 0 else "lowers fraud risk",
                "abs_contribution": round(abs(float(contribution)), 4),
            }
        )

    details.sort(key=lambda item: item["abs_contribution"], reverse=True)
    positive = [item for item in details if item["contribution"] > 0][:4]
    negative = [item for item in details if item["contribution"] < 0][:4]

    verdict = "Fraud" if probability >= DEFAULT_THRESHOLD else "Legitimate"
    delta_to_threshold = probability - DEFAULT_THRESHOLD

    return {
        "verdict": verdict,
        "probability": round(probability, 6),
        "probability_pct": round(probability * 100.0, 2),
        "probability_from_margin": round(probability_from_margin, 6),
        "threshold": DEFAULT_THRESHOLD,
        "threshold_pct": round(DEFAULT_THRESHOLD * 100.0, 1),
        "distance_to_threshold": round(delta_to_threshold, 6),
        "risk_band": _risk_band(probability),
        "normalized": normalized,
        "engineered_features": {
            "distance_from_home": round(normalized["distance_from_home"], 4),
            "distance_miles": round(normalized["distance_miles"], 1),
            "is_weekend": normalized["is_weekend"],
            "is_night": normalized["is_night"],
            "hour": normalized["hour"],
            "day_name": normalized["day_name"],
            "month": normalized["month"],
            "encoded_category": encoded_row["category_enc"],
            "encoded_gender": encoded_row["gender_enc"],
        },
        "math": {
            "bias_log_odds": round(bias, 4),
            "raw_log_odds": round(margin, 4),
            "formula": "p = 1 / (1 + e^-z), where z = bias + sum(feature contributions)",
            "decision_rule": f"Fraud if p >= {DEFAULT_THRESHOLD:.2f}, else Legitimate",
            "top_positive": positive,
            "top_negative": negative,
            "all_features": details,
        },
    }


def get_fraud_model_summary():
    with _LOCK:
        if _CACHE["summary"] is not None:
            return _CACHE["summary"]

    importance = _feature_importance()[:8]
    samples = []
    for sample in SAMPLE_TRANSACTIONS:
        scored = score_transaction(sample["payload"])
        samples.append(
            {
                "id": sample["id"],
                "name": sample["name"],
                "tone": sample["tone"],
                "payload": {
                    **sample["payload"],
                    "distance_miles": scored["engineered_features"]["distance_miles"],
                },
                "probability_pct": scored["probability_pct"],
                "verdict": scored["verdict"],
                "risk_band": scored["risk_band"],
            }
        )

    summary = {
        "model_name": "XGBoost Fraud Detection Showcase",
        "algorithm": "XGBoost binary classifier",
        "threshold": DEFAULT_THRESHOLD,
        "metrics": TRAINING_METRICS,
        "feature_importance": importance,
        "category_options": [
            {"value": value, "label": _humanize_category(value)}
            for value in CATEGORY_CLASSES
        ],
        "gender_options": [{"value": value, "label": value} for value in GENDER_CLASSES],
        "sample_transactions": samples,
        "math_blurb": "The app exposes XGBoost feature contributions in log-odds space, then converts the total through the logistic function into fraud probability.",
    }

    with _LOCK:
        _CACHE["summary"] = summary
    return summary
