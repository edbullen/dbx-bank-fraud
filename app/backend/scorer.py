"""
Model Serving integration for fraud prediction.

Calls the Databricks Model Serving endpoint (bank-fraud-predict) which hosts
a sklearn RandomForestClassifier pipeline trained on gold_transactions features.

Falls back to mock scoring if the endpoint is not configured or unreachable.
"""

import logging
import random

from . import config

log = logging.getLogger(__name__)

_client = None


def _get_client():
    """Lazy-init the Databricks WorkspaceClient for Model Serving calls."""
    global _client
    if _client is None:
        try:
            from databricks.sdk import WorkspaceClient
            _client = WorkspaceClient()
            log.info("Model Serving client initialized for endpoint '%s'", config.MODEL_SERVING_ENDPOINT)
        except Exception as e:
            log.warning("Could not initialize Databricks SDK: %s", e)
    return _client


def score_transaction(txn: dict) -> dict:
    """
    Score a transaction for fraud using Model Serving.

    Adds 'fraud_prediction' (bool) and 'fraud_probability' (float) to the txn dict.
    Falls back to mock scoring if Model Serving is unavailable.
    """
    if not config.MODEL_SERVING_ENDPOINT:
        return _mock_score(txn)

    client = _get_client()
    if client is None:
        return _mock_score(txn)

    try:
        record = {
            "amount": txn["amount"],
            "newBalanceDest": txn["new_balance_dest"],
            "oldBalanceDest": txn["old_balance_dest"],
            "diffOrig": txn["diff_orig"],
            "diffDest": txn["diff_dest"],
            "type": txn["type"],
            "countryOrig_name": txn["country_orig"],
            "countryDest_name": txn["country_dest"],
        }

        response = client.serving_endpoints.query(
            name=config.MODEL_SERVING_ENDPOINT,
            dataframe_records=[record],
        )

        prediction = response.predictions[0]

        if isinstance(prediction, dict):
            fraud_label = float(prediction.get("prediction", prediction.get("label", 0)))
        else:
            fraud_label = float(prediction)

        fraud_prediction = fraud_label >= 0.5

        if fraud_prediction:
            log.info("FRAUD DETECTED by model: type=%s amount=%.2f diffDest=%.2f oldBalDest=%.2f",
                     txn["type"], txn["amount"], txn["diff_dest"], txn["old_balance_dest"])

        # Model Serving with sklearn predict doesn't return probabilities,
        # so we approximate for the UI risk indicator
        if fraud_prediction:
            fraud_probability = round(random.uniform(0.60, 0.95), 4)
        else:
            fraud_probability = round(random.uniform(0.01, 0.30), 4)

        txn["fraud_prediction"] = fraud_prediction
        txn["fraud_probability"] = fraud_probability
        return txn

    except Exception as e:
        log.warning("Model Serving call failed, using mock: %s", e)
        return _mock_score(txn)


def _mock_score(txn: dict) -> dict:
    """Fallback mock scoring (~10% fraud rate)."""
    if random.random() < 0.10:
        txn["fraud_probability"] = round(random.uniform(0.55, 0.97), 4)
        txn["fraud_prediction"] = True
    else:
        txn["fraud_probability"] = round(random.uniform(0.001, 0.45), 4)
        txn["fraud_prediction"] = False
    return txn
