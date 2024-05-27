import asyncio
import logging
import time
import traceback
from asyncio import Queue

from _kafka import consumer, producer
from app.controllers.player import PlayerController
from app.controllers.report import ReportController
from app.views.report import ReportInQueue, StgReportCreate, convert_report_q_to_db
from database.database import get_session
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def check_duplicate_report(report_queue: Queue, valid_report_queue: Queue):
    while True:
        # Check if both queues are empty
        if report_queue.empty():
            await asyncio.sleep(1)
            continue

        # read message from queue
        msg: StgReportCreate = await report_queue.get()
        report_queue.task_done()

        try:
            # Acquire an asynchronous database session
            session: AsyncSession = await get_session()
            async with session.begin():
                report_controller = ReportController(session=session)
                report = await report_controller.get(
                    reported_id=msg.reportedID,
                    reporting_id=msg.reportingID,
                    region_id=msg.region_id,
                )

            # skip duplicate reports
            if report:
                continue

            await valid_report_queue.put(msg)
        except OperationalError as e:
            await report_queue.put(msg)
            logger.error({"error": e})
            await asyncio.sleep(5)
            continue
        except Exception as e:
            await report_queue.put(msg)
            logger.error({"error": e})
            logger.debug(f"Traceback: \n{traceback.format_exc()}")
            await asyncio.sleep(5)
            continue


async def queue_to_batch(queue: Queue, max_len: int = None) -> list:
    output = []
    max_len = max_len if max_len else queue.qsize()
    for _ in range(max_len):
        msg = await queue.get()
        queue.task_done()
        output.append(msg)
    return output


async def insert_batch(valid_report_queue: Queue):
    INSERT_INTERVAL_SEC = 20
    last_time = time.time()
    batch = []
    while True:
        if valid_report_queue.empty():
            await asyncio.sleep(1)
            continue

        if time.time() - last_time < INSERT_INTERVAL_SEC or len(batch) > 10_000:
            await asyncio.sleep(1)
            continue

        try:
            # Acquire an asynchronous database session
            session: AsyncSession = await get_session()
            async with session.begin():
                report_controller = ReportController(session=session)

                batch = await queue_to_batch(queue=valid_report_queue)
                logger.debug(f"batch inserting: {len(batch)}")
                await report_controller.insert(reports=batch)
                last_time = time.time()

        except OperationalError as e:
            await asyncio.gather(*[valid_report_queue.put(msg) for msg in batch])
            logger.error({"error": e})
            await asyncio.sleep(5)

        except Exception as e:
            await asyncio.gather(*[valid_report_queue.put(msg) for msg in batch])
            logger.error({"error": e})
            logger.debug(f"Traceback: \n{traceback.format_exc()}")
            await asyncio.sleep(5)


async def process_data(report_queue: Queue):
    receive_queue = consumer.get_queue()
    error_queue = producer.get_queue()
    while True:
        # Check if both queues are empty
        if receive_queue.empty():
            await asyncio.sleep(1)
            continue

        raw_msg: dict = await receive_queue.get()
        receive_queue.task_done()
        msg = ReportInQueue(**raw_msg)

        try:
            # Acquire an asynchronous database session
            session: AsyncSession = await get_session()
            async with session.begin():
                player_controller = PlayerController(session=session)
                reporter = await player_controller.get_or_insert(
                    player_name=msg.reporter
                )
                reported = await player_controller.get_or_insert(
                    player_name=msg.reported
                )

            # double check reporter & reported
            if reporter is None:
                logger.error(f"reporter does not exist: '{msg.reporter}'")
                continue

            if reported is None:
                logger.error(f"reported does not exist: '{msg.reported}'")
                continue

            report = convert_report_q_to_db(
                reported_id=reported.id,
                reporting_id=reporter.id,
                report_in_queue=msg,
            )
            await report_queue.put(report)
        except OperationalError as e:
            await error_queue.put(raw_msg)
            logger.error({"error": e})
            logger.info(f"error_qsize={error_queue.qsize()}, {raw_msg=}")
            await asyncio.sleep(5)
            continue
        except Exception as e:
            await error_queue.put(raw_msg)
            logger.error({"error": e})
            logger.debug(f"Traceback: \n{traceback.format_exc()}")
            logger.info(f"error_qsize={error_queue.qsize()}, {raw_msg=}")
            await asyncio.sleep(5)
            continue


async def main():
    report_queue = Queue(maxsize=500)
    valid_report_queue = Queue()
    await producer.start_engine(topic="report")
    await consumer.start_engine(topics=["report"])

    for _ in range(5):
        asyncio.create_task(process_data(report_queue=report_queue))
        asyncio.create_task(
            check_duplicate_report(
                report_queue=report_queue, valid_report_queue=valid_report_queue
            )
        )
    asyncio.create_task(insert_batch(valid_report_queue=valid_report_queue))

    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
