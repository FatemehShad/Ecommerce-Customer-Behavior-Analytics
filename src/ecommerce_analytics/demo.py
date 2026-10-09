"""Streaming synthetic event generator; no real customer data is included."""

import csv
import random
from pathlib import Path


def generate_events(path: str, rows: int = 10000, seed: int = 42) -> None:
    if rows <= 0:
        raise ValueError("rows must be positive")
    target = Path(path)
    if target.exists():
        raise FileExistsError(f"Refusing to overwrite existing input: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    emitted = 0
    journey = 0
    with target.open("x", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "visitorid", "event", "itemid", "transactionid"])
        while emitted < rows:
            journey += 1
            user = journey % 100_000
            product = rng.randrange(1, 5001)
            # Repeated customers return on later days, keeping journeys in separate sessions.
            base = 1735689600000 + (journey // 100_000) * 86400000 + (journey % 3600) * 1000
            kinds = ["view"]
            if rng.random() < 0.35:
                kinds.append("addtocart")
                if rng.random() < 0.4:
                    kinds.append("transaction")
            for index, kind in enumerate(kinds):
                if emitted == rows:
                    break
                writer.writerow(
                    [
                        base + index * 60000,
                        user,
                        kind,
                        product,
                        f"order-{journey}" if kind == "transaction" else "",
                    ]
                )
                emitted += 1
