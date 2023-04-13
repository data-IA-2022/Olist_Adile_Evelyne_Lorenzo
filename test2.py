from fastapi import FastAPI, Depends, HTTPException, status, Request, Form
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
import bcrypt
import models
from database import get_db
from starlette.templating import Jinja2Templates

app = FastAPI()
security = HTTPBasic()
templates = Jinja2Templates(directory="templates")

# Configuration de la base de données
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={
                       "check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# Route d'enregistrement
@app.route("/register", methods=["GET", "POST"])
async def register_user(request: Request, db: Session = Depends(get_db), username: Form(str) = None, password: Form(str) = None):
    if request.method == "POST":
        # Vérifier si l'utilisateur existe déjà
        if db.query(models.User).filter_by(username=username).first():
            raise HTTPException(
                status_code=400, detail="L'utilisateur existe déjà")
        # Hasher le mot de passe
        password_hash = bcrypt.hashpw(
            password.encode('utf-8'), bcrypt.gensalt())
        # Ajouter l'utilisateur à la base de données
        new_user = models.User(username=username, password_hash=password_hash)
        db.add(new_user)
        db.commit()
        return {"message": "L'utilisateur a été enregistré avec succès"}

    # Afficher le formulaire d'enregistrement
    return templates.TemplateResponse("auth/register.html", {"request": request})


# Route de connexion


@app.post("/login")
def login(credentials: HTTPBasicCredentials = Depends(security), db: Session = Depends(get_db)):
    # Rechercher l'utilisateur dans la base de données
    user = db.query(models.User).filter_by(
        username=credentials.username).first()
    # Vérifier si les informations d'identification sont valides
    if not user or not bcrypt.checkpw(credentials.password.encode('utf-8'), user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Identifiants invalides")
    return {"message": "Connexion réussie"}

# Dépendance pour obtenir une session de base de donnée
