from pydantic import BaseModel

class Chunk(BaseModel):
    id: str
    chunk: str
    order: int
    isLastChunk: bool
