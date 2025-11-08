def compute_delay(base: float, attempts: int) -> float:
    """
    Exponential backoff per spec: delay = base ^ attempts (seconds).
    attempts is the number after increment (i.e., 1 on first failure).
    """
    if attempts < 0:
        attempts = 0
    return float(base) ** float(attempts)
