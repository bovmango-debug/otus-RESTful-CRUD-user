import os
import sys
import json
import logging
import asyncio
from pydantic import BaseModel, EmailStr
from fastapi import FastAPI, HTTPException, Depends, status
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker, Session
import structlog
import aio_pika

# --- НАСТРОЙКА ЛОГИРОВАНИЯ ---
logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)
structlog.configure(
    processors=[structlog.stdlib.add_log_level, structlog.processors.TimeStamper(fmt="iso"), structlog.processors.JSONRenderer()],
    logger_factory=structlog.stdlib.LoggerFactory(),
)
logger = structlog.get_logger()

# --- ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ---
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- ТАБЛИЦЫ ДЛЯ ВСЕХ ТРЕХ СЕРВИСОВ ---

# 1. Сервис Пользователей и Заказов
class UserDB(Base):
    __tablename__ = "stream_users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, nullable=False)
    email = Column(String, unique=True, nullable=False)

class OrderDB(Base):
    __tablename__ = "stream_orders"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    status = Column(String, default="PENDING")  # PENDING, PAID, CANCELLED

# 2. Сервис Биллинга
class AccountDB(Base):
    __tablename__ = "stream_accounts"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, unique=True, nullable=False)
    balance = Column(Float, default=0.0)

# 3. Сервис Нотификаций
class NotificationDB(Base):
    __tablename__ = "stream_notifications"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False)
    email = Column(String, nullable=False)
    message = Column(String, nullable=False)

Base.metadata.create_all(bind=engine)

app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Схемы валидации Pydantic
class UserCreate(BaseModel):
    username: str
    email: EmailStr

class BalanceOperation(BaseModel):
    amount: float

class OrderCreate(BaseModel):
    user_id: int
    price: float

# Вспомогательная функция для быстрой отправки событий в RabbitMQ
async def publish_event(routing_key: str, data: dict):
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        async with connection:
            channel = await connection.channel()
            await channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps(data).encode()),
                routing_key=routing_key,
            )
            logger.info("rabbitmq_event_published", queue=routing_key, data=data)
    except Exception as e:
        logger.error("rabbitmq_publish_failed", error=str(e))

# --- API ЭНДПОИНТЫ СЕРВИСОВ ---

@app.post("/api/v1/users", status_code=201)
async def create_user(user: UserCreate, db: Session = Depends(get_db)):
    if db.query(UserDB).filter(UserDB.username == user.username).first():
        raise HTTPException(status_code=400, detail="Username already exists")
    db_user = UserDB(username=user.username, email=user.email)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    
    # Event Collaboration: Публикуем событие создания пользователя для Биллинга
    await publish_event("UserCreated", {"user_id": db_user.id})
    return db_user

# Биллинг: Положить деньги
@app.post("/api/v1/billing/{user_id}/deposit")
def deposit_money(user_id: int, op: BalanceOperation, db: Session = Depends(get_db)):
    account = db.query(AccountDB).filter(AccountDB.user_id == user_id).first()
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    account.balance += op.amount
    db.commit()
    return {"status": "success", "balance": account.balance}

# Биллинг: Посмотреть баланс
@app.get("/api/v1/billing/{user_id}/balance")
def get_balance(user_id: int, db: Session = Depends(get_db)):
    account = db.query(AccountDB).filter(AccountDB.user_id == user_id).first()
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    return {"user_id": user_id, "balance": account.balance}

# Заказы: Создать заказ (1 этап Event Collaboration)
@app.post("/api/v1/orders", status_code=201)
async def create_order(order: OrderCreate, db: Session = Depends(get_db)):
    user = db.query(UserDB).filter(UserDB.id == order.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
        
    db_order = OrderDB(user_id=order.user_id, price=order.price, status="PENDING")
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    
    # Кидаем событие в брокер, запускающее цепочку "Оплата -> Письмо"
    await publish_event("OrderCreated", {"order_id": db_order.id, "user_id": order.user_id, "price": order.price, "email": user.email})
    return db_order

# Нотификации: Получить список "отправленных" писем пользователя
@app.get("/api/v1/notifications/{user_id}")
def get_notifications(user_id: int, db: Session = Depends(get_db)):
    notifications = db.query(NotificationDB).filter(NotificationDB.user_id == user_id).all()
    return [{"email": n.email, "message": n.message} for n in notifications]

@app.get("/health")
def health_check():
    return {"status": "OK"}

# --- АСИНХРОННЫЕ СЛУШАТЕЛИ RABBITMQ (ОБРАБОТКА ПОТОКА СОБЫТИЙ) ---

async def rabbitmq_consumer():
    await asyncio.sleep(5)  # Даем время RabbitMQ на старт
    while True:
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            channel = await connection.channel()
            
            # Декларируем очереди
            queue_user = await channel.declare_queue("UserCreated")
            queue_order = await channel.declare_queue("OrderCreated")
            
            logger.info("rabbitmq_consumer_started", status="listening")
            
            # Сценарий 1: Создание аккаунта в Биллинге при создании юзера
            async with queue_user.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        data = json.loads(message.body.decode())
                        db = SessionLocal()
                        if not db.query(AccountDB).filter(AccountDB.user_id == data["user_id"]).first():
                            db.add(AccountDB(user_id=data["user_id"], balance=0.0))
                            db.commit()
                            logger.info("billing_account_created", user_id=data["user_id"])
                        db.close()
            
            # Сценарий 2: Обработка заказа (Списание денег + Нотификация)
            async with queue_order.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        data = json.loads(message.body.decode())
                        db = SessionLocal()
                        
                        account = db.query(AccountDB).filter(AccountDB.user_id == data["user_id"]).first()
                        order = db.query(OrderDB).filter(OrderDB.id == data["order_id"]).first()
                        
                        if account and order:
                            if account.balance >= data["price"]:
                                # Снимаем деньги (Письмо счастья)
                                account.balance -= data["price"]
                                order.status = "PAID"
                                msg_text = f"Заказ {order.id} успешно оформлен! Списано {data['price']} руб."
                            else:
                                # Денег мало (Письмо горя)
                                order.status = "CANCELLED"
                                msg_text = f"Ошибка оформления заказа {order.id}. Недостаточно средств!"
                            
                            # Сервис нотификаций сохраняет письмо в БД
                            db.add(NotificationDB(user_id=data["user_id"], email=data["email"], message=msg_text))
                            db.commit()
                            logger.info("order_processed_async", order_id=order.id, status=order.status)
                        db.close()
                        
        except Exception as e:
            logger.error("rabbitmq_consumer_error", error=str(e))
            await asyncio.sleep(5)

@app.on_event("startup")
def startup_event():
    asyncio.create_task(rabbitmq_consumer())
