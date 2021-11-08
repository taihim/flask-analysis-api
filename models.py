from pydantic import BaseModel


class QueryData(BaseModel):
    id: int
    name = 'Jane Doe'
