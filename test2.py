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


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")


async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password"
            )
        user = db.query(models.User).filter_by(username=username).first()
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password"
            )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password"
        )
    return user


@app.get("/register")
async def register_form(request: Request):
    return templates.TemplateResponse("auth/register.html", {"request": request})


@app.post("/register", response_model=None)
async def register_user(username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    # Vérifier si l'utilisateur existe déjà
    if db.query(models.User).filter_by(username=username).first():
        raise HTTPException(
            status_code=400, detail="L'utilisateur existe déjà")
    # Générer le salt pour le hashage de mot de passe
    salt = bcrypt.gensalt()
    # Hasher le mot de passe avec le salt généré
    password_hash = bcrypt.hashpw(password.encode('utf-8'), salt)
    # Ajouter l'utilisateur à la base de données avec le salt utilisé pour le hashage
    new_user = models.User(
        username=username, password_hash=password_hash, salt=salt)
    db.add(new_user)
    db.commit()
    return {"message": "L'utilisateur a été enregistré avec succès"}


@app.get("/login")
async def login_form(request: Request):
    return templates.TemplateResponse("auth/login.html", {"request": request})


@app.post("/login")
async def login(request: Request, form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(models.User).filter_by(username=form_data.username).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password"
        )
    else:
        stored_password_hash = user.password_hash
        if not bcrypt.checkpw(form_data.password.encode('utf-8'), stored_password_hash.decode('utf-8').encode('utf-8')):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password"
            )

    access_token = create_access_token(data={"sub": user.username})
    response = JSONResponse(content={"message": "Login successful"})
    response.set_cookie(
        key="access_token",
        value=f"Bearer {access_token}",
        httponly=True,
        max_age=604800,  # une semaine en secondes
        expires=604800,
    )
    return response


def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


@app.get("/protected-route")
async def protected_route(request: Request, current_user: models.User = Depends(get_current_user)):
    access_token = request.cookies.get("access_token")
    print(request.cookies)
    if access_token:
        return {"message": f"Bienvenue, {current_user.username}"}
        # ...
    else:
        raise HTTPException(status_code=401, detail="Not authenticated")


@app.get("/login")
async def login_form(request: Request):
    return templates.TemplateResponse("auth/login.html", {"request": request})


@app.post("/login")
async def login(request: Request, form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(models.User).filter_by(username=form_data.username).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password"
        )
    else:
        stored_password_hash = user.password_hash
        if not bcrypt.checkpw(form_data.password.encode('utf-8'), stored_password_hash.decode('utf-8').encode('utf-8')):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password"
            )

    access_token = create_access_token(data={"sub": user.username})
    response = JSONResponse(content={"message": "Login successful"})
    response.set_cookie(
        key="access_token",
        value=f"Bearer {access_token}",
        httponly=True,
        max_age=604800,  # une semaine en secondes
        expires=604800,
    )
    return response


def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


@app.get("/protected-route")
async def protected_route(request: Request, current_user: models.User = Depends(get_current_user)):
    access_token = request.cookies.get("access_token")
    print(request.cookies)
    if access_token:
        return {"message": f"Bienvenue, {current_user.username}"}
        # ...
    else:
        raise HTTPException(status_code=401, detail="Not authenticated")


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def get_user(db: Session, username: str):
    return db.query(models.User).filter(models.User.username == username).first()


def authenticate_user(db: Session, username: str, password: str):
    user = get_user(db, username)
    if not user:
        return False
    if not verify_password(password, user.password_hash):
        return False
    return user


def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(SessionLocal)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password"
            )
        user = get_user(db, username)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password"
            )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password"
        )
    return user


async def get_current_active_user(current_user: schemas.User = Depends(get_current_user)):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


@app.post("/register")
async def register_user(username: str = Form(...), password: str = Form(...), db: Session = Depends(SessionLocal)):
    # Check if the user already exists
    if db.query(models.User).filter_by(username=username).first():
        raise HTTPException(
            status_code=400, detail="L'utilisateur existe déjà")
    # Hash the password
    hashed_password = get_password_hash(password)
    # Add the user to the database
    new_user = models.User(username=username, password_hash=hashed_password)
    db.add(new_user)
    db.commit()
    return {"message": "L'utilisateur a été enregistré avec succès"}


@app.post("/login")
async def login(request: Request, form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(models.User).filter_by(username=form_data.username).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Nom d'utilisateur ou mot de passe invalide"
        )

    if not verify_password(form_data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Nom d'utilisateur ou mot de passe invalide"
        )

    access_token = create_access_token(data={"sub": user.username})
    response = JSONResponse(content={"message": "Connexion réussie"})
    response.set_cookie(
        key="access_token",
        value=f"Bearer {access_token}",
        httponly=True,
        max_age=604800,  # une semaine en secondes
        expires=604800,
    )
    return response


@app.get("/protected-route")
async def protected_route(request: Request, current_user: models.User = Depends(get_current_user)):
    access_token = request.cookies.get("access_token")
    if access_token:
        return {"message": f"Bienvenue, {current_user.username}"}
    else:
        raise HTTPException(status_code=401, detail="Non authentifié")


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")


async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password"
            )
        user = db.query(models.User).filter_by(username=username).first()
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password"
            )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password"
        )
    return user
