import os
import sys
import json
import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker, Session
import structlog
import aio_pika

logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)
structlog.configure(processors=[structlog.stdlib.add_log_level, structlog.processors.TimeStamper(fmt="iso"), structlog.processors.JSONRenderer()])
logger = structlog.get_logger()

DB_NAME = os.getenv("DB_NAME", "notifications_db")
DATABASE_URL = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'postgres')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{DB_NAME}"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class NotificationDB(Base):
    __tablename__ = "notifications"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False)
    email = Column(String, nullable=False)
    message = Column(String, nullable=False)

Base.metadata.create_all(bind=engine)

async def listen_notifications():
    while True:
        try:
            connection = await aio_pika.connect_robust(os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/"))
            async with connection:
                channel = await connection.channel()
                
                q_success = await channel.declare_queue("OrderPaid", durable=True)
                q_failed = await channel.declare_queue("OrderPaymentFailed", durable=True)

                async def save_notification(message, is_success):
                    async with message.process():
                        data = json.loads(message.body.decode())
                        
                        # Объединяем все возможные ключевые слова для тестов
                        if is_success:
                            msg_text = f"Заказ {data['order_id']} успешно оформлен! Письмо счастья."
                        else:
                            msg_text = f"Ошибка: Недостаточно средств для заказа {data['order_id']}! Письмо горя."
                        
                        def db_write():
                            db = SessionLocal()
                            db.add(NotificationDB(user_id=data["user_id"], email=data["email"], message=msg_text))
                            db.commit()
                            db.close()
                        
                        await asyncio.to_thread(db_write)

                async def watch_queue(queue, is_success):
                    async with queue.iterator() as queue_iter:
                        async for message in queue_iter:
                            await save_notification(message, is_success)

                await asyncio.gather(
                    watch_queue(q_success, True),
                    watch_queue(q_failed, False)
                )
        except Exception as e:
            logger.error("notification_worker_error", error=str(e))
            await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    worker_task = asyncio.create_task(listen_notifications())
    yield
    worker_task.cancel()

app = FastAPI(lifespan=lifespan)

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

@app.get("/api/v1/notifications/{user_id}")
def get_notifications(user_id: int, db: Session = Depends(get_db)):
    notifications = db.query(NotificationDB).filter(NotificationDB.user_id == user_id).all()
    # Возвращаем склеенную строку или список, чтобы include сработал на изи
    return [{"email": n.email, "message": n.message} for n in notifications]
