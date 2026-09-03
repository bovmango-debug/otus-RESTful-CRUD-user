import os
import sys
import json
import logging
import asyncio
from pydantic import BaseModel
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker, Session
import structlog
import aio_pika

logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)
structlog.configure(processors=[structlog.stdlib.add_log_level, structlog.processors.TimeStamper(fmt="iso"), structlog.processors.JSONRenderer()])
logger = structlog.get_logger()

DB_NAME = os.getenv("DB_NAME", "orders_db") # Своя БД!
DATABASE_URL = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'postgres')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class OrderDB(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    status = Column(String, default="PENDING")

Base.metadata.create_all(bind=engine)
app = FastAPI()

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

class OrderCreate(BaseModel):
    user_id: int
    price: float
    email: str # Передаем email с фронта/постмана для упрощения нотификации

@app.post("/api/v1/orders", status_code=201)
async def create_order(order: OrderCreate, db: Session = Depends(get_db)):
    db_order = OrderDB(user_id=order.user_id, price=order.price, status="PENDING")
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    
    # Отправляем событие на оплату в Биллинг
    connection = await aio_pika.connect_robust(os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"))
    async with connection:
        channel = await connection.channel()
        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps({"order_id": db_order.id, "user_id": order.user_id, "price": order.price, "email": order.email}).encode()),
            routing_key="OrderCreated"
        )
    return db_order

# Слушаем результаты оплаты, чтобы поменять статус заказа
async def listen_payment_results():
    await asyncio.sleep(5)
    connection = await aio_pika.connect_robust(os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"))
    channel = await connection.channel()
    
    # Слушаем и успешные, и провальные оплаты
    q_success = await channel.declare_queue("OrderPaid")
    q_failed = await channel.declare_queue("OrderPaymentFailed")
    
    async def update_status(message, new_status):
        async with message.process():
            data = json.loads(message.body.decode())
            db = SessionLocal()
            order = db.query(OrderDB).filter(OrderDB.id == data["order_id"]).first()
            if order:
                order.status = new_status
                db.commit()
            db.close()

    async def watch_queue(queue, status_str):
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                await update_status(message, status_str)

    asyncio.create_task(watch_queue(q_success, "PAID"))
    asyncio.create_task(watch_queue(q_failed, "CANCELLED"))

@app.on_event("startup")
def startup_event():
    asyncio.create_task(listen_payment_results())
