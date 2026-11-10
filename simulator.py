"""
Transaction Simulator (Producer).

Generates a continuous synthetic stream of realistic payment transactions,
while injecting specific anomalous fraud patterns (Velocity Spikes, Geo-Jumps, High-Value Drains)
at configurable intervals and upon manual request.
"""

from collections import deque
import random
import threading
import time
import uuid
from typing import Callable, Dict, Generator, List, Optional

from models import AnomalyType, Transaction, UserProfile


MERCHANT_CATEGORIES = [
    "grocery",
    "food_delivery",
    "ride_hailing",
    "streaming_subscription",
    "electronics",
    "apparel",
    "travel",
    "crypto_exchange",
    "luxury_retail",
    "online_gambling",
]

COUNTRIES = ["US", "GB", "IN", "DE", "SG", "JP", "BR", "AU", "CA", "FR"]


class TransactionSimulator:
    """
    Simulates production transaction streams with realistic background traffic
    and controlled fraudulent behavioral injections.
    """

    def __init__(
        self,
        num_users: int = 50,
        transactions_per_sec: float = 3.0,
        fraud_probability: float = 0.15,
        on_transaction_generated: Optional[Callable[[Transaction], None]] = None,
    ):
        self.num_users = num_users
        self.transactions_per_sec = max(0.2, transactions_per_sec)
        self.fraud_probability = fraud_probability
        self.on_transaction_generated = on_transaction_generated

        self._user_profiles: Dict[str, UserProfile] = {}
        self._user_ids: List[str] = []
        self._initialize_user_profiles()

        # Priority queue / buffer for pending burst transactions (e.g., velocity spikes)
        self._pending_burst_queue: deque = deque()
        self._lock = threading.Lock()

        # Streaming control flags
        self.is_running = False
        self._worker_thread: Optional[threading.Thread] = None

    def _initialize_user_profiles(self) -> None:
        """Seed realistic user baseline spending profiles."""
        for i in range(1, self.num_users + 1):
            uid = f"USR-{i:03d}"
            home_country = random.choice(COUNTRIES)
            typical_avg = random.choice([25.0, 45.0, 65.0, 95.0, 140.0])
            std_dev = max(5.0, typical_avg * random.uniform(0.15, 0.35))
            fav_cats = random.sample(
                ["grocery", "food_delivery", "ride_hailing", "streaming_subscription", "apparel"],
                k=3
            )
            ip_prefix = f"192.168.{random.randint(10, 99)}.{random.randint(2, 254)}"

            self._user_profiles[uid] = UserProfile(
                user_id=uid,
                home_country=home_country,
                typical_avg_amount=typical_avg,
                typical_std_amount=std_dev,
                favorite_categories=fav_cats,
                device_ip_prefix=ip_prefix,
            )
            self._user_ids.append(uid)

    def get_user_profile(self, user_id: str) -> Optional[UserProfile]:
        return self._user_profiles.get(user_id)

    def generate_normal_transaction(self, user_id: Optional[str] = None) -> Transaction:
        """Generate a single benign baseline payment transaction."""
        if not user_id:
            user_id = random.choice(self._user_ids)
        profile = self._user_profiles[user_id]

        # Log-normal or gaussian amount distribution around profile mean
        amount = max(3.50, random.gauss(profile.typical_avg_amount, profile.typical_std_amount))
        
        # 95% from favorite or benign categories, rare high-risk
        if random.random() < 0.90:
            category = random.choice(profile.favorite_categories)
        else:
            category = random.choice(["electronics", "travel", "apparel"])

        # 96% transactions originate from home country
        if random.random() < 0.96:
            country = profile.home_country
        else:
            # Benign occasional travel to another country
            country = random.choice([c for c in COUNTRIES if c != profile.home_country])

        ip = f"{profile.device_ip_prefix[:-2]}.{random.randint(1, 254)}"

        return Transaction(
            transaction_id=f"TXN-{uuid.uuid4().hex[:10].upper()}",
            user_id=user_id,
            amount=round(amount, 2),
            timestamp=time.time(),
            merchant_category=category,
            location=country,
            ip_address=ip,
            is_synthetic_fraud=False,
            injected_anomaly_type=AnomalyType.NONE,
        )

    # -------------------------------------------------------------
    # Explicit Fraud Pattern Injectors
    # -------------------------------------------------------------
    def inject_velocity_burst(self, user_id: Optional[str] = None, burst_count: int = 5) -> List[Transaction]:
        """
        Pattern 1: Velocity Spike.
        A user executes 4+ transactions within a few seconds (violates 60s sliding window limit).
        """
        if not user_id:
            user_id = random.choice(self._user_ids)
        profile = self._user_profiles[user_id]

        burst = []
        base_time = time.time()
        for idx in range(burst_count):
            txn = Transaction(
                transaction_id=f"TXN-VEL-{uuid.uuid4().hex[:8].upper()}",
                user_id=user_id,
                amount=round(random.uniform(40.0, 180.0), 2),
                timestamp=base_time + (idx * 0.4),  # rapid bursts 0.4s apart
                merchant_category=random.choice(["electronics", "gift_cards", "digital_goods"]),
                location=profile.home_country,
                ip_address=profile.device_ip_prefix,
                is_synthetic_fraud=True,
                injected_anomaly_type=AnomalyType.VELOCITY_SPIKE,
            )
            burst.append(txn)

        with self._lock:
            # Enqueue all burst events into the streaming buffer
            self._pending_burst_queue.extend(burst)

        return burst

    def inject_geo_anomaly(self, user_id: Optional[str] = None) -> List[Transaction]:
        """
        Pattern 2: Geo-Velocity / Impossible Travel.
        Transactions from two geographically distant countries within a few seconds.
        """
        if not user_id:
            user_id = random.choice(self._user_ids)
        profile = self._user_profiles[user_id]

        home_country = profile.home_country
        distant_candidates = [c for c in COUNTRIES if c != home_country]
        distant_country = random.choice(distant_candidates)

        now = time.time()
        txn1 = Transaction(
            transaction_id=f"TXN-GEO1-{uuid.uuid4().hex[:8].upper()}",
            user_id=user_id,
            amount=round(random.uniform(25.0, 80.0), 2),
            timestamp=now,
            merchant_category="grocery",
            location=home_country,
            ip_address=profile.device_ip_prefix,
            is_synthetic_fraud=False,  # first leg is normal
            injected_anomaly_type=AnomalyType.NONE,
        )

        txn2 = Transaction(
            transaction_id=f"TXN-GEO2-{uuid.uuid4().hex[:8].upper()}",
            user_id=user_id,
            amount=round(random.uniform(400.0, 950.0), 2),
            timestamp=now + 1.2,  # 1.2s later from distant continent
            merchant_category="luxury_retail",
            location=distant_country,
            ip_address=f"104.{random.randint(10, 250)}.{random.randint(1, 250)}.{random.randint(1, 250)}",
            is_synthetic_fraud=True,
            injected_anomaly_type=AnomalyType.GEO_ANOMALY,
        )

        with self._lock:
            self._pending_burst_queue.extend([txn1, txn2])

        return [txn1, txn2]

    def inject_high_value_drain(self, user_id: Optional[str] = None) -> Transaction:
        """
        Pattern 3: High-Value Drain.
        Sudden transaction exceeding 6x to 15x the user's historical average.
        """
        if not user_id:
            user_id = random.choice(self._user_ids)
        profile = self._user_profiles[user_id]

        multiplier = random.uniform(6.5, 14.0)
        drain_amount = max(850.0, profile.typical_avg_amount * multiplier)

        txn = Transaction(
            transaction_id=f"TXN-DRAIN-{uuid.uuid4().hex[:8].upper()}",
            user_id=user_id,
            amount=round(drain_amount, 2),
            timestamp=time.time(),
            merchant_category=random.choice(["crypto_exchange", "luxury_retail", "jewelry_store"]),
            location=profile.home_country,
            ip_address=profile.device_ip_prefix,
            is_synthetic_fraud=True,
            injected_anomaly_type=AnomalyType.HIGH_VALUE_DRAIN,
        )

        with self._lock:
            self._pending_burst_queue.append(txn)

        return txn

    def step(self) -> Transaction:
        """
        Produce next transaction from pending burst or synthesize new traffic.
        """
        with self._lock:
            if self._pending_burst_queue:
                return self._pending_burst_queue.popleft()

        # Randomly inject fraud patterns based on fraud_probability
        if random.random() < self.fraud_probability:
            pattern = random.choice(["velocity", "geo", "drain"])
            if pattern == "velocity":
                self.inject_velocity_burst()
            elif pattern == "geo":
                self.inject_geo_anomaly()
            else:
                self.inject_high_value_drain()

            with self._lock:
                if self._pending_burst_queue:
                    return self._pending_burst_queue.popleft()

        # Otherwise produce normal baseline traffic
        return self.generate_normal_transaction()

    def start_streaming(self) -> None:
        """Start streaming in a dedicated background producer thread."""
        if self.is_running:
            return
        self.is_running = True
        self._worker_thread = threading.Thread(target=self._stream_loop, daemon=True)
        self._worker_thread.start()

    def stop_streaming(self) -> None:
        """Stop background producer streaming."""
        self.is_running = False
        if self._worker_thread:
            self._worker_thread.join(timeout=1.0)
            self._worker_thread = None

    def _stream_loop(self) -> None:
        """Internal continuous streaming loop."""
        while self.is_running:
            txn = self.step()
            if self.on_transaction_generated:
                try:
                    self.on_transaction_generated(txn)
                except Exception as e:
                    print(f"Error delivering transaction to consumer: {e}")
            
            # Rate limiting based on target TPS
            delay = 1.0 / max(0.5, self.transactions_per_sec)
            time.sleep(delay)
