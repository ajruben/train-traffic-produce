
from pydantic import BaseModel

class Train(BaseModel):
    treinNummer: int
    ritId: str
    lat: float
    lng: float
    snelheid: float
    richting: float
    type: str
    bron: str

class TrainPayload(BaseModel):
    treinen: list[dict]

class Trains(BaseModel):
    payload: TrainPayload


