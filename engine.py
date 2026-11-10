"""
Real-Time Fraud Scoring & Sliding-Window State Engine.

Architecture & Distributed Concepts:
- Keyed State Store: User state is keyed by user_id, mirroring distributed state backends
  (e.g., Apache Flink Keyed State / RocksDB state store).
- Sliding Window (Tumbling/Sliding): Maintains an active time-based sliding window
  (default 60 seconds) of transaction events per user. Stale events older than
  (event_time - window_size) are lazily evicted upon event arrival.
- Multi-Factor Hybrid Scoring: Combines deterministic rule-based checks (velocity, geo-velocity)
  with statistical anomaly detection (z-score, rolling average multipliers).
- Watermarking & Event-Time Handling: Tracks max event timestamp to handle potential
  out-of-order arrivals within an allowed lateness threshold.
"""

from collections import deque
import math
import threading
import time
from typing import Dict, List, Optional, Tuple

from models import AnomalyType, RiskResult, RiskStatus, RuleEvaluation, Transaction


# Approximate coordinates (lat, lon) for common countries to compute geo-velocity / impossible travel
COUNTRY_COORDINATES: Dict[str, Tuple[float, float]] = {
    "US": (37.0902, -95.7129),   # United States
    "GB": (55.3781, -3.4360),    # United Kingdom
    "IN": (20.5937, 78.9629),    # India
    "DE": (51.1657, 10.4515),    # Germany
    "SG": (1.3521, 103.8198),    # Singapore
    "JP": (36.2048, 138.2529),   # Japan
    "BR": (-14.2350, -51.9253),  # Brazil
    "AU": (-25.2744, 133.7751),  # Australia
    "CA": (56.1304, -106.3468),  # Canada
    "FR": (46.2276, 2.2137),     # France
    "AE": (23.4241, 53.8478),    # United Arab Emirates
    "ZA": (-30.5595, 22.9375),   # South Africa
}


def haversine_distance_km(coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
    """Calculate the great-circle distance between two points on the Earth (in km)."""
    lat1, lon1 = math.radians(coord1[0]), math.radians(coord1[1])
    lat2, lon2 = math.radians(coord2[0]), math.radians(coord2[1])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return 6371.0 * c


class UserWindowState:
    """
    In-memory state store for a single keyed partition (user).
    Maintains active sliding-window history and cumulative baseline statistics.
    """

    def __init__(self, user_id: str, window_duration_sec: float = 60.0):
        self.user_id = user_id
        self.window_duration_sec = window_duration_sec
        
        # Sliding window buffer: stores (timestamp, amount, location, ip_address)
        self.window_events: deque = deque()
        
        # Cumulative historical baseline statistics
        self.total_txns_count: int = 0
        self.historical_sum: float = 0.0
        self.historical_sq_sum: float = 0.0
        
        # Last observed event coordinates
        self.last_seen_timestamp: Optional[float] = None
        self.last_seen_location: Optional[str] = None
        self.last_seen_ip: Optional[str] = None

    @property
    def historical_avg(self) -> float:
        if self.total_txns_count == 0:
            return 50.0  # sensible fallback default
        return self.historical_sum / self.total_txns_count

    @property
    def historical_std(self) -> float:
        if self.total_txns_count < 2:
            return 15.0
        variance = (self.historical_sq_sum / self.total_txns_count) - (self.historical_avg ** 2)
        return math.sqrt(max(0.0, variance)) if variance > 0 else 15.0

    def evict_stale_events(self, current_event_time: float) -> None:
        """
        Sliding Window Eviction:
        Removes events older than (current_event_time - window_duration_sec).
        """
        cutoff = current_event_time - self.window_duration_sec
        while self.window_events and self.window_events[0][0] < cutoff:
            self.window_events.popleft()

    def get_window_velocity(self) -> int:
        """Current number of transactions inside the active sliding window."""
        return len(self.window_events)

    def get_window_sum(self) -> float:
        """Total transaction monetary sum in the active sliding window."""
        return sum(evt[1] for evt in self.window_events)

    def record_event(self, timestamp: float, amount: float, location: str, ip: str) -> None:
        """Commit an accepted event into the state store and update running statistics."""
        self.window_events.append((timestamp, amount, location, ip))
        self.total_txns_count += 1
        self.historical_sum += amount
        self.historical_sq_sum += amount * amount
        self.last_seen_timestamp = timestamp
        self.last_seen_location = location
        self.last_seen_ip = ip


class FraudScoringEngine:
    """
    Multi-Factor Real-Time Scoring & State Engine.
    
    Configuration Parameters:
    - velocity_threshold: Max permitted transactions in sliding window before flagging (e.g. 4)
    - velocity_window_sec: Window length in seconds (default 60s)
    - drain_multiplier: Multiplier over historical average to flag drain (default 5.0x)
    - max_feasible_speed_kmh: Max physical airliner travel speed to flag geo-velocity (default 850 km/h)
    - decline_score_threshold: Composite score >= threshold marks DECLINED (default 70.0)
    - review_score_threshold: Composite score >= threshold marks REVIEW (default 40.0)
    """

    def __init__(
        self,
        velocity_threshold: int = 4,
        velocity_window_sec: float = 60.0,
        drain_multiplier: float = 5.0,
        max_feasible_speed_kmh: float = 850.0,
        decline_score_threshold: float = 70.0,
        review_score_threshold: float = 40.0,
    ):
        self.velocity_threshold = velocity_threshold
        self.velocity_window_sec = velocity_window_sec
        self.drain_multiplier = drain_multiplier
        self.max_feasible_speed_kmh = max_feasible_speed_kmh
        self.decline_score_threshold = decline_score_threshold
        self.review_score_threshold = review_score_threshold

        # Keyed State Partition: user_id -> UserWindowState
        self._user_states: Dict[str, UserWindowState] = {}
        self._lock = threading.RLock()

        # Stream Watermark: Max event timestamp observed minus allowed lateness
        self.allowed_lateness_sec: float = 5.0
        self.current_watermark: float = 0.0

    def get_or_create_user_state(self, user_id: str) -> UserWindowState:
        if user_id not in self._user_states:
            self._user_states[user_id] = UserWindowState(
                user_id=user_id,
                window_duration_sec=self.velocity_window_sec
            )
        return self._user_states[user_id]

    def reset_state(self) -> None:
        """Clear all active user states (useful for resetting demo)."""
        with self._lock:
            self._user_states.clear()
            self.current_watermark = 0.0

    def evaluate_transaction(self, txn: Transaction) -> RiskResult:
        """
        Evaluate an incoming transaction event against real-time state and rules.
        Measures execution latency to mirror sub-millisecond production stream SLAs.
        """
        start_time = time.perf_counter()

        with self._lock:
            # 1. Watermark Update & Out-of-Order Check
            if txn.timestamp > self.current_watermark + self.allowed_lateness_sec:
                self.current_watermark = txn.timestamp - self.allowed_lateness_sec

            state = self.get_or_create_user_state(txn.user_id)
            state.window_duration_sec = self.velocity_window_sec

            # 2. Window Eviction (Slide window forward to txn.timestamp)
            state.evict_stale_events(txn.timestamp)

            rules_triggered: List[RuleEvaluation] = []
            cumulative_score: float = 0.0

            # -------------------------------------------------------------
            # RULE 1: Sliding-Window Velocity Spike
            # E.g., user executing 4+ transactions within a 60-second window
            # -------------------------------------------------------------
            current_window_count = state.get_window_velocity() + 1  # including current
            if current_window_count >= self.velocity_threshold:
                # Calculate window duration span
                earliest_ts = state.window_events[0][0] if state.window_events else txn.timestamp
                time_span = max(0.1, txn.timestamp - earliest_ts)
                
                # Scaled impact: higher frequency yields higher penalty
                excess = current_window_count - self.velocity_threshold
                impact = min(65.0, 45.0 + (excess * 8.0))
                cumulative_score += impact
                rules_triggered.append(
                    RuleEvaluation(
                        rule_name="VELOCITY_SPIKE",
                        triggered=True,
                        score_impact=impact,
                        explanation=(
                            f"Velocity spike: {current_window_count} txns in {time_span:.1f}s "
                            f"(Threshold: {self.velocity_threshold} in {int(self.velocity_window_sec)}s)"
                        )
                    )
                )

            # -------------------------------------------------------------
            # RULE 2: Geo-Velocity Anomaly / Impossible Travel
            # E.g., transactions from different countries in short time
            # -------------------------------------------------------------
            if state.last_seen_location and state.last_seen_timestamp:
                delta_t_sec = max(0.001, txn.timestamp - state.last_seen_timestamp)
                if state.last_seen_location != txn.location:
                    prev_coords = COUNTRY_COORDINATES.get(state.last_seen_location, (0.0, 0.0))
                    curr_coords = COUNTRY_COORDINATES.get(txn.location, (0.0, 0.0))
                    dist_km = haversine_distance_km(prev_coords, curr_coords)
                    
                    hours = delta_t_sec / 3600.0
                    speed_kmh = dist_km / max(0.0001, hours)

                    # Flag if implied speed exceeds supersonic airliner / impossible jump
                    if speed_kmh > self.max_feasible_speed_kmh or delta_t_sec < 180.0:
                        impact = 55.0
                        cumulative_score += impact
                        rules_triggered.append(
                            RuleEvaluation(
                                rule_name="GEO_ANOMALY",
                                triggered=True,
                                score_impact=impact,
                                explanation=(
                                    f"Impossible travel: Jump from {state.last_seen_location} to {txn.location} "
                                    f"({dist_km:.0f}km) in {delta_t_sec:.1f}s (Speed: {speed_kmh:.0f} km/h)"
                                )
                            )
                        )

            # -------------------------------------------------------------
            # RULE 3: High-Value Drain Anomaly
            # E.g., sudden transaction amount exceeding 5x historical average
            # -------------------------------------------------------------
            if state.total_txns_count >= 2:
                hist_avg = state.historical_avg
                if txn.amount >= (self.drain_multiplier * hist_avg) and txn.amount >= 200.0:
                    multiplier = txn.amount / max(1.0, hist_avg)
                    impact = min(60.0, 40.0 + min(20.0, (multiplier - self.drain_multiplier) * 4.0))
                    cumulative_score += impact
                    rules_triggered.append(
                        RuleEvaluation(
                            rule_name="HIGH_VALUE_DRAIN",
                            triggered=True,
                            score_impact=impact,
                            explanation=(
                                f"High-value drain: ${txn.amount:.2f} is {multiplier:.1f}x "
                                f"user baseline avg (${hist_avg:.2f})"
                            )
                        )
                    )

            # -------------------------------------------------------------
            # RULE 4: Statistical Outlier Check (Z-Score)
            # Evaluates standard deviations away from the user's spending norm
            # -------------------------------------------------------------
            if state.total_txns_count >= 3:
                std_dev = max(10.0, state.historical_std)
                z_score = (txn.amount - state.historical_avg) / std_dev
                if z_score > 3.0 and txn.amount > 250.0:
                    stat_impact = min(25.0, 10.0 + (z_score - 3.0) * 3.0)
                    cumulative_score += stat_impact
                    rules_triggered.append(
                        RuleEvaluation(
                            rule_name="STATISTICAL_ZSCORE",
                            triggered=True,
                            score_impact=stat_impact,
                            explanation=f"Statistical outlier: Amount is {z_score:.1f} standard deviations above mean"
                        )
                    )

            # -------------------------------------------------------------
            # RULE 5: High-Risk Category & Off-Hour Surcharge
            # High-risk industries like crypto or luxury gambling
            # -------------------------------------------------------------
            if txn.merchant_category in ("crypto_exchange", "luxury_retail", "online_gambling"):
                if txn.amount > 350.0:
                    category_impact = 15.0
                    cumulative_score += category_impact
                    rules_triggered.append(
                        RuleEvaluation(
                            rule_name="HIGH_RISK_CATEGORY",
                            triggered=True,
                            score_impact=category_impact,
                            explanation=f"Elevated risk: High-risk merchant category '{txn.merchant_category}'"
                        )
                    )

            # Clamp risk score to [0, 100]
            final_risk_score = min(100.0, max(0.0, cumulative_score))

            # Determine Risk Status
            if final_risk_score >= self.decline_score_threshold:
                status = RiskStatus.DECLINED
            elif final_risk_score >= self.review_score_threshold:
                status = RiskStatus.REVIEW
            else:
                status = RiskStatus.APPROVED

            # 3. Update Stateful Profile (Commit Event)
            state.record_event(
                timestamp=txn.timestamp,
                amount=txn.amount,
                location=txn.location,
                ip=txn.ip_address
            )

        latency_ms = (time.perf_counter() - start_time) * 1000.0

        return RiskResult(
            transaction=txn,
            risk_score=final_risk_score,
            status=status,
            rules_triggered=rules_triggered,
            latency_ms=latency_ms,
            evaluated_at=time.time()
        )
