from pydantic import BaseModel


class Person(BaseModel):
    name: str
    age: int
    email: str


# Exemple d'utilisation du modèle de données
person_data = {
    "name": "John Doe",
    "age": "30",
    "email": "john.doe@example.com",
}

person = Person(**person_data)
print(person)
