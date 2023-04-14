from fastapi import FastAPI, Depends, HTTPException, status, Response
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from fastapi.templating import Jinja2Templates
from fastapi import FastAPI, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse

from pydantic import BaseModel
from typing import Optional

from sqlalchemy.orm import Session

from passlib.context import CryptContext
from jose import jwt, JWTError

from database import engine, get_db
import models

from datetime import datetime, timedelta


SECRET_KEY = "KlgH6AzYDeZeGwD288to79I3vTHT8wp7"
ALGORITHM = "HS256"


class CreateUser(BaseModel):
    username: str
    password: str


bcrypt_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

models.Base.metadata.create_all(bind=engine)

oauth2_bearer = OAuth2PasswordBearer(tokenUrl="token")


app = FastAPI()

# Configurer Jinja2
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def root(request: Request, conn=Depends(get_connection)):
    query = "SELECT * FROM olist_customers LIMIT 15;"
    rows = await conn.fetch(query)
    return templates.TemplateResponse("home/index.html", {"request": request, "rows": rows})


def get_password_hash(password):
    return bcrypt_context.hash(password)


def verify_password(plain_password, hashed_password):
    return bcrypt_context.verify(plain_password, hashed_password)


def authenticate_user(username: str, password: str, db):
    user = db.query(models.Users)\
        .filter(models.Users.username == username)\
        .first()

    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


def create_access_token(username: str, user_id: int,
                        expires_delta: Optional[timedelta] = None):

    encode = {"sub": username, "id": user_id}
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    encode.update({"exp": expire})
    return jwt.encode(encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(token: str = Depends(oauth2_bearer)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        user_id: int = payload.get("id")
        if username is None or user_id is None:
            raise get_user_exception()
        return {"username": username, "id": user_id}
    except JWTError:
        raise get_user_exception()


@app.get("/create/user", response_class=HTMLResponse)
async def create_user_form(request: Request):
    return templates.TemplateResponse("auth/register.html", {"request": request})


@app.post("/create/user", response_class=HTMLResponse)
async def create_new_user(request: Request, username: str = Form(...), password: str = Form(...), db=Depends(get_db)):
    create_user_model = models.Users()
    create_user_model.username = username
    hash_password = get_password_hash(password)

    create_user_model.hashed_password = hash_password
    create_user_model.is_active = True

    db.add(create_user_model)
    db.commit()

    return """
    <html>
        <body>
            <h1>User {username} created successfully!</h1>
            <a href="/">Go back to home</a>
        </body>
    </html>
    """.format(username=username)


@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(),
                                 db=Depends(get_db)):
    user = authenticate_user(form_data.username, form_data.password, db)
    if not user:
        raise token_exception()
    token_expires = timedelta(minutes=20)
    token = create_access_token(user.username,
                                user.id,
                                expires_delta=token_expires)
    return {"token": token}


# Exceptions
def get_user_exception():
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    return credentials_exception


def token_exception():
    token_exception_response = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect username or password",
        headers={"WWW-Authenticate": "Bearer"},
    )
    return token_exception_response


@app.get("/login", response_class=HTMLResponse)
async def login_form(request: Request):
    return templates.TemplateResponse("auth/login.html", {"request": request})


@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...), db=Depends(get_db), response: Response = None):
    user = authenticate_user(username, password, db)
    if not user:
        return templates.TemplateResponse("auth/login.html", {"request": request, "error": "Incorrect username or password"})

    # The user will remain logged in for 7 days
    token_expires = timedelta(days=7)
    token = create_access_token(
        user.username, user.id, expires_delta=token_expires)

    response = RedirectResponse(url="/", status_code=303)
    response.set_cookie(key="access_token", value=token,
                        httponly=True, max_age=token_expires.total_seconds())
    return response
