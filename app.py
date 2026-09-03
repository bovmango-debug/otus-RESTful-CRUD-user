import os
import sys
import json
import logging
from datetime import datetime, timedelta
import structlog
from fastapi import FastAPI, HTTPException, Depends, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel, EmailStr
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from jose import JWTError, jwt
from passlib.context import CryptContext
from prometheus_fastapi_instrumentator import Instrumentator

# --- НАСТРОЙКА БЕЗОПАСНОСТИ ---
SECRET_KEY = os.getenv("SECRET_KEY", "SUPER_SECRET_KEY_CHANGEME_12345")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/login")

# --- СТРУКТУРИРОВАННОЕ JSON-ЛОГИРОВАНИЕ ---
logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)
logger = structlog.get_logger()

# Требование прошлого ДЗ: Логи миграций (INFO)
logger.info("migration_job_status", action="start", message="Database migration job initiated")
logger.info("migration_job_status", action="finish", message="Database migration job completed successfully")

# --- ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ ---
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base = declarative_base()
except Exception as e:
    logger.error("database_critical_error", error=str(e), stage="connection_failed")
    raise e

# Модель пользователя с хэшем пароля
class UserDB(Base):
    __tablename__ = "auth_users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)

try:
    Base.metadata.create_all(bind=engine)
except Exception as e:
    logger.error("database_critical_error", error=str(e), stage="schema_creation_failed")
    raise e

app = FastAPI()
Instrumentator().instrument(app).expose(app)

# Хелперы безопасности
def get_password_hash(password: str):
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str):
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Схемы валидации Pydantic
class UserRegister(BaseModel):
    username: str
    email: EmailStr
    password: str

class UserLogin(BaseModel):
    username: str
    password: str

class UserUpdate(BaseModel):
    username: str
    email: EmailStr

class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    class Config:
        from_attributes = True

# Логи жизненного цикла приложения
@app.on_event("startup")
def startup_event():
    logger.info("application_lifecycle", status="started", message="FastAPI service is online")

@app.on_event("shutdown")
def shutdown_event():
    logger.info("application_lifecycle", status="stopped", message="FastAPI service is shutting down")

# Логирование ошибок валидации (WARN)
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.warn("validation_error", errors=exc.errors(), body=str(exc.body))
    return Response(content=json.dumps({"detail": exc.errors()}), status_code=422, media_type="application/json")

# Middleware логирования входящих HTTP-запросов (INFO)
@app.middleware("http")
async def log_requests(request: Request, call_next):
    client_ip = request.client.host if request.client else "unknown"
    logger.info("http_request_incoming", method=request.method, path=request.url.path, ip=client_ip)
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        logger.error("application_critical_error", error=str(e), path=request.url.path)
        return Response(content="Internal Server Error", status_code=500)

# Зависимость получения текущего юзера по JWT токену
def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    user = db.query(UserDB).filter(UserDB.username == username).first()
    if user is None:
        raise credentials_exception
    return user

# --- ЭНДПОИНТЫ НОВОГО ДЗ (АУТЕНТИФИКАЦИЯ И ИЗОЛЯЦИЯ ПРОФИЛЯ) ---

@app.post("/api/v1/register", response_model=UserResponse, status_code=201)
def register_user(user_data: UserRegister, db: Session = Depends(get_db)):
    if db.query(UserDB).filter(UserDB.username == user_data.username).first():
        logger.warn("registration_failed", detail="Username already exists", username=user_data.username)
        raise HTTPException(status_code=400, detail="Username already registered")
    if db.query(UserDB).filter(UserDB.email == user_data.email).first():
        logger.warn("registration_failed", detail="Email already exists", email=user_data.email)
        raise HTTPException(status_code=400, detail="Email already registered")
    
    hashed_pwd = get_password_hash(user_data.password)
    db_user = UserDB(username=user_data.username, email=user_data.email, hashed_password=hashed_pwd)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    logger.info("crud_operation_success", action="register_user", user_id=db_user.id)
    return db_user

@app.post("/api/v1/login")
def login(user_data: UserLogin, db: Session = Depends(get_db)):
    user = db.query(UserDB).filter(UserDB.username == user_data.username).first()
    if not user or not verify_password(user_data.password, user.hashed_password):
        logger.warn("login_failed", username=user_data.username)
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    
    access_token = create_access_token(data={"sub": user.username})
    logger.info("login_success", username=user.username, user_id=user.id)
    return {"access_token": access_token, "token_type": "bearer"}

# Просмотр СОБСТВЕННОГО профиля по ТЗ (Изоляция данных)
@app.get("/api/v1/users/me", response_model=UserResponse)
def read_current_user_profile(current_user: UserDB = Depends(get_current_user)):
    logger.info("crud_operation_success", action="get_profile", user_id=current_user.id)
    return current_user

# Редактирование СОБСТВЕННОГО профиля по ТЗ
@app.put("/api/v1/users/me", response_model=UserResponse)
def update_current_user_profile(updated_data: UserUpdate, current_user: UserDB = Depends(get_current_user), db: Session = Depends(get_db)):
    current_user.username = updated_data.username
    current_user.email = updated_data.email
    try:
        db.commit()
        db.refresh(current_user)
        logger.info("crud_operation_success", action="update_profile", user_id=current_user.id)
        return current_user
    except Exception as e:
        db.rollback()
        logger.error("database_error", operation="update_profile", error=str(e))
        raise HTTPException(status_code=400, detail="Username or email already exists")

@app.get("/health")
def health_check():
    return {"status": "OK"}
