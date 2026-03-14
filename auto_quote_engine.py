import json
import os
from typing import Dict, List, Tuple

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
AUTO_QUOTE_MODEL_PATH = os.path.join(BASE_DIR, "data", "auto_quote_model.json")

_MODEL_CACHE = {"mtime": None, "model": None}


def _to_float(value, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _to_int(value, default: int) -> int:
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return int(default)


def _band_from_rules(value: float, rules: List[Tuple[float, float, str]], fallback: str) -> str:
    x = _to_float(value, float("nan"))
    if x != x:  # NaN check without math
        return fallback
    for low, high, label in rules:
        if low <= x <= high:
            return str(label)
    return fallback


def _normalize_text(value, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text if text else fallback


def _pick_factor_level(level: str, mapping: Dict[str, float], default_level: str) -> str:
    if level in mapping:
        return level
    if default_level in mapping:
        return default_level
    if mapping:
        return sorted(mapping.keys())[0]
    return default_level


def _load_model() -> dict:
    mtime = os.path.getmtime(AUTO_QUOTE_MODEL_PATH)
    if _MODEL_CACHE["model"] is not None and _MODEL_CACHE["mtime"] == mtime:
        return _MODEL_CACHE["model"]

    with open(AUTO_QUOTE_MODEL_PATH, "r", encoding="utf-8") as f:
        model = json.load(f)

    _MODEL_CACHE["mtime"] = mtime
    _MODEL_CACHE["model"] = model
    return model


def get_auto_quote_model_summary() -> dict:
    model = _load_model()
    return {
        "model_name": model.get("model_name", ""),
        "created_at_utc": model.get("created_at_utc", ""),
        "training_summary": model.get("training_summary", {}),
        "formula": model.get("formula", ""),
        "feature_levels": {
            feat: list(levels.keys())
            for feat, levels in model.get("features", {}).items()
        },
    }


def estimate_quote(payload: dict) -> dict:
    model = _load_model()
    defaults = model.get("defaults", {})
    feature_maps = model.get("features", {})

    band_defs = model.get("band_definitions", {})
    age_rules = [tuple(row) for row in band_defs.get("driver_age_band", [])]
    veh_age_rules = [tuple(row) for row in band_defs.get("vehicle_age_band", [])]
    power_rules = [tuple(row) for row in band_defs.get("vehicle_power_band", [])]
    bm_rules = [tuple(row) for row in band_defs.get("bonus_malus_band", [])]
    density_rules = [tuple(row) for row in band_defs.get("density_band", [])]

    driver_age = _to_int(payload.get("driver_age"), 35)
    vehicle_age = _to_int(payload.get("vehicle_age"), 3)
    vehicle_power = _to_int(payload.get("vehicle_power"), 7)
    bonus_malus = _to_int(payload.get("bonus_malus"), 90)
    density = _to_int(payload.get("density"), 300)

    fuel_type_raw = _normalize_text(payload.get("fuel_type"), "Regular").capitalize()
    area_raw = _normalize_text(payload.get("area"), "C").upper()
    gender_raw = _normalize_text(payload.get("gender"), "prefer_not_to_say").lower()

    parsed_features = {
        "driver_age_band": _band_from_rules(driver_age, age_rules, defaults.get("driver_age_band", "35-49")),
        "vehicle_age_band": _band_from_rules(vehicle_age, veh_age_rules, defaults.get("vehicle_age_band", "2-4")),
        "vehicle_power_band": _band_from_rules(vehicle_power, power_rules, defaults.get("vehicle_power_band", "6-7")),
        "bonus_malus_band": _band_from_rules(bonus_malus, bm_rules, defaults.get("bonus_malus_band", "81-100")),
        "density_band": _band_from_rules(density, density_rules, defaults.get("density_band", "201-500")),
        "area": area_raw,
        "fuel_type": fuel_type_raw,
    }

    multipliers = []
    product = 1.0

    for feature_name, chosen in parsed_features.items():
        mapping = feature_maps.get(feature_name, {})
        default_level = defaults.get(feature_name, "")
        final_level = _pick_factor_level(chosen, mapping, default_level)
        factor = float(mapping.get(final_level, 1.0))
        product *= factor
        multipliers.append(
            {
                "factor": feature_name,
                "input_level": chosen,
                "applied_level": final_level,
                "multiplier": round(factor, 6),
            }
        )

    gender_factor_map = model.get("gender_factor", {})
    gender_factor = float(gender_factor_map.get(gender_raw, gender_factor_map.get("prefer_not_to_say", 1.0)))
    multipliers.append(
        {
            "factor": "gender",
            "input_level": gender_raw,
            "applied_level": gender_raw if gender_raw in gender_factor_map else "prefer_not_to_say",
            "multiplier": round(gender_factor, 6),
        }
    )

    pure_base = float(model.get("base_annual_pure_premium", 0.0))
    expense_load = float(model.get("expense_load_multiplier", 1.0))

    estimated_pure = pure_base * product * gender_factor
    quoted_annual = estimated_pure * expense_load
    quoted_monthly = quoted_annual / 12.0

    equation_terms = [f"{pure_base:.2f}"]
    for m in multipliers:
        equation_terms.append(f"{m['multiplier']:.6f}")
    equation_terms.append(f"{expense_load:.6f}")

    equation_text = "quoted_annual = " + " * ".join(equation_terms)

    return {
        "quote": {
            "annual": round(quoted_annual, 2),
            "monthly": round(quoted_monthly, 2),
            "pure_premium_annual": round(estimated_pure, 2),
            "currency": model.get("currency", "USD"),
        },
        "equation": {
            "text": equation_text,
            "base_annual_pure_premium": round(pure_base, 4),
            "expense_load_multiplier": round(expense_load, 6),
            "feature_multipliers": multipliers,
            "final_multiplier_excl_expense": round(product * gender_factor, 6),
        },
        "inputs_normalized": {
            "driver_age": driver_age,
            "vehicle_age": vehicle_age,
            "vehicle_power": vehicle_power,
            "bonus_malus": bonus_malus,
            "density": density,
            "fuel_type": parsed_features["fuel_type"],
            "area": parsed_features["area"],
            "gender": gender_raw,
        },
        "model": {
            "model_name": model.get("model_name", ""),
            "created_at_utc": model.get("created_at_utc", ""),
            "training_summary": model.get("training_summary", {}),
            "data_sources": model.get("data_sources", []),
            "notes": model.get("notes", []),
        },
    }
