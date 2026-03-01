from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional

class VesselPosition(BaseModel):
    mmsi: int = Field(..., description="Identificador único del barco (Maritime Mobile Service Identity)")
    ship_name: str
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    speed: float = Field(..., ge=0)
    heading: float = Field(..., ge=0, le=360)
    status: str = "Under way using engine"
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    @field_validator('mmsi')
    def validate_mmsi(cls, v):
        if len(str(v)) != 9:
            raise ValueError('El MMSI debe tener 9 dígitos')
        return v