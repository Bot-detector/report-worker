import asyncio
import json
import logging
import time
import traceback
from asyncio import Event, Queue

from sqlalchemy import insert, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncResult, AsyncSession
from sqlalchemy.sql.expression import Insert, Select, Update

import _kafka
import core.logging  # for log formatting
from app.views.player import PlayerInDB
from app.views.report import ReportInQueue, StgReportCreate, convert_report_q_to_db
from core.config import settings
from database.database import get_session
from database.models.player import Player
from database.models.report import StgReport

logger = logging.getLogger(__name__)


async def select_player(session: AsyncSession, name: str) -> PlayerInDB:
    sql: Select = select(Player)
    sql = sql.where(Player.name == name)
    result: AsyncResult = await session.execute(sql)
    data = result.scalars().all()
    return PlayerInDB(**data[0]) if data else None


# TODO: pydantic data
async def insert_report(session: AsyncSession, data: StgReportCreate):
    sql: Insert = insert(StgReport)
    sql = sql.values(data.model_dump(mode="json"))
    sql = sql.prefix_with("IGNORE")
    # await session.execute(sql)
    return


# Define a function to process data from a queue
async def process_data(receive_queue: Queue, error_queue: Queue, shutdown_event: Event):
    # Initialize counter and start time
    counter = 0
    start_time = time.time()

    # Run indefinitely
    while not shutdown_event.is_set():
        start_time, counter = _kafka.log_speed(
            counter=counter,
            start_time=start_time,
            _queue=receive_queue,
            topic="report",
            interval=60,
        )
        # Check if both queues are empty
        if receive_queue.empty():
            await asyncio.sleep(1)
            continue

        # Get a message from the chosen queue
        message: dict = await receive_queue.get()
        parsed_msg = ReportInQueue(**message)
        # TEMP
        print(parsed_msg)

        try:
            # Acquire an asynchronous database session
            session: AsyncSession = await get_session()
            async with session.begin():
                reporter = await select_player(
                    session=session, name=parsed_msg.reporter
                )
                if reporter is None:
                    logger.error(f"reporter does not exist {parsed_msg.reporter}")
                    receive_queue.task_done()
                    continue

                reported = await select_player(
                    session=session, name=parsed_msg.reported
                )
                if reported is None:
                    logger.error(f"reported does not exist {parsed_msg.reported}")
                    receive_queue.task_done()
                    continue

                report = convert_report_q_to_db(
                    reported_id=reported.id,
                    reporting_id=reporter.id,
                    report_in_queue=parsed_msg,
                )

                await insert_report(session=session, data=report)
                await session.commit()

            # Mark the message as processed in the queue
            receive_queue.task_done()
        # Handle exceptions, log the error, and put the message in the error queue
        # Mark the message as processed in the queue and continue to the next iteration
        except OperationalError as e:
            await error_queue.put(message)
            logger.error({"error": e})
            logger.info(f"error_qsize={error_queue.qsize()}, {message=}")
            receive_queue.task_done()
            continue
        except Exception as e:
            await error_queue.put(message)
            logger.error({"error": e})
            logger.debug(f"Traceback: \n{traceback.format_exc()}")
            logger.info(f"error_qsize={error_queue.qsize()}, {message=}")
            receive_queue.task_done()
            continue
        counter += 1


async def main():
    TOPIC = "report"
    GROUP = "report-worker"
    shutdown_event = Event()

    # get kafka engine
    consumer = await _kafka.kafka_consumer(
        topic=TOPIC, group=GROUP, bootstrap_servers=[settings.KAFKA_HOST]
    )
    producer = await _kafka.kafka_producer(bootstrap_servers=[settings.KAFKA_HOST])

    receive_queue = Queue(maxsize=100)
    send_queue = Queue(maxsize=100)

    asyncio.create_task(
        _kafka.receive_messages(
            consumer=consumer,
            receive_queue=receive_queue,
            shutdown_event=shutdown_event,
            batch_size=200,
        )
    )
    asyncio.create_task(
        _kafka.send_messages(
            topic=TOPIC,
            producer=producer,
            send_queue=send_queue,
            shutdown_event=shutdown_event,
        )
    )
    asyncio.create_task(
        process_data(
            receive_queue=receive_queue,
            error_queue=send_queue,
            shutdown_event=shutdown_event,
        )
    )

    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
