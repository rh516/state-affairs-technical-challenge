from dataclasses import dataclass
from datetime import date

@dataclass
class Video:
    source: str
    external_id: str
    date: date
    url: str
