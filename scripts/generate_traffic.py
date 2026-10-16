"""Generate realistic OLTP traffic on the source DB: inserts, status transitions, deletes.

    python scripts/generate_traffic.py --dsn postgresql://shop:shop@localhost:5432/shop --seconds 60
"""

from __future__ import annotations

import argparse
import random
import time

import psycopg

TRANSITIONS = {"placed": ["paid", "cancelled"], "paid": ["shipped", "cancelled"], "shipped": ["delivered"]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="postgresql://shop:shop@localhost:5432/shop")
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--rate", type=float, default=20.0, help="operations per second")
    args = ap.parse_args()

    rng = random.Random(42)
    with psycopg.connect(args.dsn, autocommit=True) as conn:
        cur = conn.cursor()
        for i in range(50):
            cur.execute(
                "INSERT INTO customers (email, country, tier) VALUES (%s, %s, %s)",
                (f"user{i}@example.com", rng.choice(["IN", "US", "SG", "DE"]), rng.choice(["standard", "gold"])),
            )
        deadline = time.time() + args.seconds
        ops = 0
        while time.time() < deadline:
            r = rng.random()
            if r < 0.5:
                cur.execute(
                    "INSERT INTO orders (customer_id, status, amount, currency) "
                    "SELECT customer_id, 'placed', %s, %s FROM customers ORDER BY random() LIMIT 1",
                    (round(rng.uniform(5, 500), 2), rng.choice(["INR", "USD"])),
                )
            elif r < 0.95:
                cur.execute("SELECT order_id, status FROM orders WHERE status <> ALL(%s) ORDER BY random() LIMIT 1",
                            (["delivered", "cancelled"],))
                row = cur.fetchone()
                if row:
                    cur.execute("UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s",
                                (rng.choice(TRANSITIONS[row[1]]), row[0]))
            else:
                cur.execute("DELETE FROM orders WHERE order_id = (SELECT order_id FROM orders "
                            "WHERE status = 'cancelled' ORDER BY random() LIMIT 1)")
            ops += 1
            time.sleep(1 / args.rate)
        print(f"generated {ops} operations")


if __name__ == "__main__":
    main()
