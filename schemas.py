from pydantic import BaseModel


class Product_name(BaseModel):
    product_category_name: str
    product_category_name_english: str
    product_category_name_french: str

    class Config:
        orm_mode = True