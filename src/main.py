import asyncio
import logging
import time
import traceback
from asyncio import Event, Queue

import _kafka
from app.views.player import PlayerCreate, PlayerInDB
from app.views.report import ReportInQueue, StgReportCreate, convert_report_q_to_db
from core.config import settings
from database.database import get_session, model_to_dict
from database.models.player import Player
from database.models.report import StgReport
from sqlalchemy import insert, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncResult, AsyncSession
from sqlalchemy.sql.expression import Insert, Select

logger = logging.getLogger(__name__)


async def select_player(session: AsyncSession, name: str) -> PlayerInDB:
    sql: Select = select(Player)
    sql = sql.where(Player.name == name)
    result: AsyncResult = await session.execute(sql)
    data = result.scalars().all()
    return PlayerInDB(**model_to_dict(data[0])) if data else None


async def insert_player(session: AsyncSession, player: PlayerCreate) -> PlayerInDB:
    player.name = player.name.lower().replace("_", " ").replace("-", " ").strip()
    sql: Insert = insert(Player)
    sql = sql.values(player.model_dump())
    await session.execute(sql)
    return await select_player(session=session, name=player.name)


async def get_or_create_player(session: AsyncSession, player_name: str) -> PlayerInDB:
    player = await select_player(session=session, name=player_name)
    # create reporter
    if player is None:
        player = await insert_player(
            session=session, player=PlayerCreate(name=player_name)
        )
    return player


# TODO: pydantic data
async def insert_report(session: AsyncSession, data: list[StgReportCreate]):
    logger.info(f"inserting: {len(data)}")
    sql: Insert = insert(StgReport)
    sql = sql.values([d.model_dump(mode="json") for d in data])
    sql = sql.prefix_with("IGNORE")
    await session.execute(sql)
    return


async def insert_batch(
    batch: list[StgReportCreate], error_queue: Queue, batch_last: float
):
    try:
        session: AsyncSession = await get_session()
        async with session.begin():
            if len(batch) > 1000 or time.time() - batch_last > 30:
                await insert_report(session=session, data=batch)
                await session.commit()
                batch = []
                batch_last = time.time()
        # Handle exceptions, log the error, and put the message in the error queue
    # Mark the message as processed in the queue and continue to the next iteration
    except OperationalError as e:
        for msg in batch:
            await error_queue.put(msg)
        logger.error({"error": e})
        logger.info(f"error_qsize={error_queue.qsize()}")
    except Exception as e:
        for msg in batch:
            await error_queue.put(msg)
        logger.error({"error": e})
        logger.debug(f"Traceback: \n{traceback.format_exc()}")
        logger.info(f"error_qsize={error_queue.qsize()}")
    return batch, batch_last


# Define a function to process data from a queue
async def process_data(receive_queue: Queue, error_queue: Queue, shutdown_event: Event):
    # Initialize counter and start time
    counter = 0
    start_time = time.time()
    batch = list()
    batch_last = time.time()

    # Run indefinitely
    while not shutdown_event.is_set():
        start_time, counter = _kafka.log_speed(
            counter=counter,
            start_time=start_time,
            _queue=receive_queue,
            topic="report-insert",
            interval=60,
        )
        if batch:
            batch, batch_last = await insert_batch(
                batch=batch,
                error_queue=error_queue,
                batch_last=batch_last,
            )
        # Check if both queues are empty
        if receive_queue.empty():
            await asyncio.sleep(1)
            continue

        # Get a message from the chosen queue
        message: dict = await receive_queue.get()
        parsed_msg = ReportInQueue(**message)
        try:
            # Acquire an asynchronous database session
            session: AsyncSession = await get_session()
            async with session.begin():
                reporter = await get_or_create_player(
                    session=session, player_name=parsed_msg.reporter
                )

                reported = await get_or_create_player(
                    session=session, player_name=parsed_msg.reported
                )

                # double check reporter & reported
                if reporter is None:
                    logger.error(f"reporter does not exist: '{parsed_msg.reporter}'")
                    receive_queue.task_done()
                    continue

                if reported is None:
                    logger.error(f"reported does not exist: '{parsed_msg.reported}'")
                    receive_queue.task_done()
                    continue

                report = convert_report_q_to_db(
                    reported_id=reported.id,
                    reporting_id=reporter.id,
                    report_in_queue=parsed_msg,
                )
                await session.commit()
                batch.append(report)

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

    engine = _kafka.AioKafkaEngine(
        receive_queue=receive_queue,
        send_queue=send_queue,
        producer=producer,
        consumer=consumer,
    )
    await engine.start(
        consumer_batch_size=200,
        producer_shutdown_event=shutdown_event,
        consumer_shutdown_event=shutdown_event,
        producer_topic=TOPIC,
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
