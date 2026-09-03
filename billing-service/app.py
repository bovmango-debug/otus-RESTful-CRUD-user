import os
import sys
import json
import logging
import asyncio
from pydantic import BaseModel, EmailStr
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker, Session
import structlog
import aio_pika

logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)
structlog.configure(
    processors=[structlog.stdlib.add_log_level, structlog.processors.TimeStamper(fmt="iso"), structlog.processors.JSONRenderer()],
    logger_factory=structlog.stdlib.LoggerFactory(),
)
logger = structlog.get_logger()

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "billing_db") # Своя БД!
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class UserDB(Base):
    __tablename__ = "billing_users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, nullable=False)
    email = Column(String, unique=True, nullable=False)

class AccountDB(Base):
    __tablename__ = "billing_accounts"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, unique=True, nullable=False)
    balance = Column(Float, default=0.0)

Base.metadata.create_all(bind=engine)
app = FastAPI()

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

class UserCreate(BaseModel):
    username: str
    email: EmailStr

class BalanceOperation(BaseModel):
    amount: float

async def publish_event(routing_key: str, data: dict):
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        async with connection:
            channel = await connection.channel()
            await channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps(data).encode()), routing_key=routing_key,
            )
            logger.info("event_published", queue=routing_key, data=data)
    except Exception as e:
        logger.error("publish_failed", error=str(e))

@app.post("/api/v1/users", status_code=201)
async def create_user(user: UserCreate, db: Session = Depends(get_db)):
    if db.query(UserDB).filter(UserDB.username == user.username).first():
        raise HTTPException(status_code=400, detail="Username already exists")
    db_user = UserDB(username=user.username, email=user.email)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    
    # Сразу создаем аккаунт внутри этого же сервиса
    db.add(AccountDB(user_id=db_user.id, balance=0.0))
    db.commit()
    return {"id": db_user.id, "username": db_user.username, "email": db_user.email}

@app.post("/api/v1/billing/{user_id}/deposit")
def deposit_money(user_id: int, op: BalanceOperation, db: Session = Depends(get_db)):
    account = db.query(AccountDB).filter(AccountDB.user_id == user_id).first()
    if not account: raise HTTPException(status_code=404, detail="Account not found")
    account.balance += op.amount
    db.commit()
    return {"status": "success", "balance": account.balance}

@app.get("/api/v1/billing/{user_id}/balance")
def get_balance(user_id: int, db: Session = Depends(get_db)):
    account = db.query(AccountDB).filter(AccountDB.user_id == user_id).first()
    if not account: raise HTTPException(status_code=404, detail="Account not found")
    return {"user_id": user_id, "balance": account.balance}

@app.get("/health")
def health_check(): return {"status": "OK"}

# Фоновый воркер: обрабатывает заказы и проверяет баланс асинхронно
async def process_orders():
    await asyncio.sleep(5)
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    queue = await channel.declare_queue("OrderCreated")
    
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                data = json.loads(message.body.decode())
                db = SessionLocal()
                account = db.query(AccountDB).filter(AccountDB.user_id == data["user_id"]).first()
                
                if account and account.balance >= data["price"]:
                    account.balance -= data["price"]
                    db.commit()
                    # Деньги списаны -> Публикуем УСПЕХ
                    await publish_event("OrderPaid", {"order_id": data["order_id"], "user_id": data["user_id"], "email": data["email"], "status": "SUCCESS"})
                else:
                    # Денег не хватило -> Публикуем ПРОВАЛ
                    await publish_event("OrderPaymentFailed", {"order_id": data["order_id"], "user_id": data["user_id"], "email": data["email"], "status": "FAILED"})
                db.close()

@app.on_event("startup")
def startup_event():
    asyncio.create_task(process_orders())
