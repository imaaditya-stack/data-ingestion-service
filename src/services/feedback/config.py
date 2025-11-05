from dataclasses import dataclass, field


@dataclass
class FeedbackServiceConfig:
    max_retries: int = field(default=3)
