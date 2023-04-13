
from pydantic import BaseModel
from typing import Optional


class Product_name(BaseModel):
    product_category_name: str
    product_category_name_english: str
    product_category_name_french: str

    class Config:
        orm_mode = True


class User(BaseModel):
    username: str
    password: str

    class Config:
        orm_mode = True


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None
