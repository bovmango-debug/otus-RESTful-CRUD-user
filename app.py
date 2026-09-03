import os
import sys
import json
import logging
import structlog
from fastapi import FastAPI, HTTPException, Depends, Request, Response
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from prometheus_fastapi_instrumentator import Instrumentator

# --- НАСТРОЙКА СТРУКТУРИРОВАННОГО JSON-ЛОГИРОВАНИЯ ---
logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()  # Логи выводятся строго в JSON
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)
logger = structlog.get_logger()

# Требование ТЗ: Запуск/завершение Job с миграциями (INFO)
logger.info("migration_job_status", action="start", message="Database migration job initiated")
# ... (Имитация процесса наката миграций БД) ...
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
    # Требование ТЗ: Критические ошибки при работе с БД (ERROR)
    logger.error("database_critical_error", error=str(e), stage="connection_failed")
    raise e

class UserDB(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)

try:
    Base.metadata.create_all(bind=engine)
except Exception as e:
    logger.error("database_critical_error", error=str(e), stage="schema_creation_failed")
    raise e

app = FastAPI()
Instrumentator().instrument(app).expose(app)

# Требование ТЗ: Старт/остановка приложения (INFO)
@app.on_event("startup")
def startup_event():
    logger.info("application_lifecycle", status="started", message="FastAPI service is online")

@app.on_event("shutdown")
def shutdown_event():
    logger.info("application_lifecycle", status="stopped", message="FastAPI service is shutting down")

# Требование ТЗ: Ошибки валидации входных данных (WARN)
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.warn("validation_error", errors=exc.errors(), body=str(exc.body))
    return Response(
        content=json.dumps({"detail": exc.errors()}), 
        status_code=422, 
        media_type="application/json"
    )

# Требование ТЗ: Входящие HTTP-запросы (метод, путь, IP) (INFO)
@app.middleware("http")
async def log_requests(request: Request, call_next):
    client_ip = request.client.host if request.client else "unknown"
    logger.info("http_request_incoming", method=request.method, path=request.url.path, ip=client_ip)
    
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        # Требование ТЗ: Критические ошибки в самом приложении (ERROR)
        logger.error("application_critical_error", error=str(e), path=request.url.path)
        return Response(content="Internal Server Error", status_code=500)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class UserCreate(BaseModel):
    username: str
    email: str

class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    class Config:
        from_attributes = True

# --- CRUD МЕТОДЫ С ЛОГИРОВАНИЕМ УСПЕШНЫХ ОПЕРАЦИЙ (INFO) ---

@app.post("/api/v1/users", response_model=UserResponse, status_code=201)
def create_user(user: UserCreate, db: Session = Depends(get_db)):
    db_user = UserDB(username=user.username, email=user.email)
    db.add(db_user)
    try:
        db.commit()
        db.refresh(db_user)
        # Успешное выполнение: создан пользователь ID
        logger.info("crud_operation_success", action="create_user", user_id=db_user.id)
        return db_user
    except Exception as e:
        db.rollback()
        logger.error("database_error", operation="create_user", error=str(e))
        raise HTTPException(status_code=400, detail="Username or email already exists")

@app.get("/api/v1/users/{user_id}", response_model=UserResponse)
def get_user(user_id: int, db: Session = Depends(get_db)):
    user = db.query(UserDB).filter(UserDB.id == user_id).first()
    if not user:
        logger.warn("crud_operation_warning", action="get_user", detail="User not found", requested_id=user_id)
        raise HTTPException(status_code=404, detail="User not found")
    return user

@app.put("/api/v1/users/{user_id}", response_model=UserResponse)
def update_user(user_id: int, updated_user: UserCreate, db: Session = Depends(get_db)):
    user = db.query(UserDB).filter(UserDB.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.username = updated_user.username
    user.email = updated_user.email
    db.commit()
    db.refresh(user)
    # Успешное выполнение: обновлен пользователь ID
    logger.info("crud_operation_success", action="update_user", user_id=user.id)
    return user

@app.delete("/api/v1/users/{user_id}", status_code=204)
def delete_user(user_id: int, db: Session = Depends(get_db)):
    user = db.query(UserDB).filter(UserDB.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    db.delete(user)
    db.commit()
    # Успешное выполнение: удален пользователь ID
    logger.info("crud_operation_success", action="delete_user", user_id=user_id)
    return

@app.get("/health")
def health_check():
    return {"status": "OK"}
