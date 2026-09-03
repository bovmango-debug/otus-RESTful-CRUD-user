import os
import sys
import json
import logging
import asyncio
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker, Session
import structlog
import aio_pika

logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)
structlog.configure(processors=[structlog.stdlib.add_log_level, structlog.processors.TimeStamper(fmt="iso"), structlog.processors.JSONRenderer()])
logger = structlog.get_logger()

DB_NAME = os.getenv("DB_NAME", "notifications_db") # Своя БД!
DATABASE_URL = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'postgres')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class NotificationDB(Base):
    __tablename__ = "notifications"
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, nullable=False)
    message = Column(String, nullable=False)

Base.metadata.create_all(bind=engine)
app = FastAPI()

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

@app.get("/api/v1/notifications/{email}")
def get_notifications(email: str, db: Session = Depends(get_db)):
    notifications = db.query(NotificationDB).filter(NotificationDB.email == email).all()
    return [{"email": n.email, "message": n.message} for n in notifications]

async def listen_notifications():
    await asyncio.sleep(5)
    connection = await aio_pika.connect_robust(os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"))
    channel = await connection.channel()
    
    q_success = await channel.declare_queue("OrderPaid")
    q_failed = await channel.declare_queue("OrderPaymentFailed")

    async def save_notification(message, is_success):
        async with message.process():
            data = json.loads(message.body.decode())
            db = SessionLocal()
            msg_text = f"Письмо счастья: заказ {data['order_id']} оформлен!" if is_success else f"Письмо горя: не хватило денег на заказ {data['order_id']}!"
            db.add(NotificationDB(email=data["email"], message=msg_text))
            db.commit()
            db.close()

    async def watch_success():
        async with q_success.iterator() as queue_iter:
            async for message in queue_iter:
                await save_notification(message, is_success=True)

    async def watch_failed():
        async with q_failed.iterator() as queue_iter:
            async for message in queue_iter:
                await save_notification(message, is_success=False)

    asyncio.create_task(watch_success())
    asyncio.create_task(watch_failed())

@app.on_event("startup")
def startup_event():
    asyncio.create_task(listen_notifications())
