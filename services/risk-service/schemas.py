"""Wire schema for events this service consumes.

BuyerJoined mirrors internal/kafkax.BuyerJoined (Go) field-for-field,
including JSON key names -- this is the one place the two languages'
representations of the same event have to agree, so any change to the
Go struct needs a matching change here.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class BuyerJoined(BaseModel):
    item_id: str
    user_id: str
    remote_addr: str
    joined_at: datetime
