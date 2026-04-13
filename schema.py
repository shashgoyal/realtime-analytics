from pydantic import BaseModel

class Event(BaseModel):
  user_id: str
  event_type: str
  timestamp: str
  session_id: str | None = None
  page_url: str | None = None
  device: str | None = None
  ip_address: str | None = None
  metadata: dict | None = None