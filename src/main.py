import asyncio
import logging
import time
import traceback
from asyncio import Queue
from datetime import datetime

from _cache import SimpleALRUCache
from _kafka import consumer, producer
from app.controllers.player import PlayerController
from app.controllers.report import ReportController
from app.views.report import (
    ReportInQV1,
    ReportInQV2,
    StgReportCreate,
    convert_report_q_to_db,
    convert_stg_to_kafka_report,
)
from database.database import get_session
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class PlayerDoesNotExist(Exception):
    ...


class ReporterDoesNotExist(PlayerDoesNotExist):
    ...


class ReportedDoesNotExist(PlayerDoesNotExist):
    ...


async def create_batch(batch_size: int, batch_queue: Queue, report_queue: Queue):
    batch = []
    _time = time.time()
    MAX_INTERVAL = 60
    while True:
        if report_queue.empty():
            await asyncio.sleep(1)
            continue

        report = await report_queue.get()
        report_queue.task_done()

        batch.append(report)

        delta = _time - time.time()
        if len(batch) == batch_size or delta > MAX_INTERVAL:
            await batch_queue.put(batch)
            batch = []
            _time = time.time()


async def insert_batch(batch_queue: Queue, error_queue: Queue):
    while True:
        if batch_queue.empty():
            await asyncio.sleep(1)
            continue
        batch = await batch_queue.get()
        batch_queue.task_done()
        try:
            # Acquire an asynchronous database session
            session: AsyncSession = await get_session()
            async with session.begin():
                report_controller = ReportController(session=session)
                logger.debug(f"batch inserting: {len(batch)}")
                # stage report
                await report_controller.insert(reports=batch)
                # normalized report
                await report_controller.insert_report(reports=batch)
                await session.commit()
                logger.debug("inserted")
        except OperationalError as e:
            logger.error({"error": e})
            await asyncio.gather(
                *[
                    error_queue.put(convert_stg_to_kafka_report(m).model_dump())
                    for m in batch
                ]
            )
            await asyncio.sleep(5)
        except Exception as e:
            logger.error({"error": e})
            logger.debug(f"Traceback: \n{traceback.format_exc()}")
            await asyncio.gather(
                *[
                    error_queue.put(convert_stg_to_kafka_report(m).model_dump())
                    for m in batch
                ]
            )
            await asyncio.sleep(5)


async def process_msg_v1(
    msg: ReportInQV1, player_controller: PlayerController
) -> StgReportCreate:
    # Acquire an asynchronous database session
    session: AsyncSession = await get_session()
    async with session.begin():
        await player_controller.update_session(session=session)
        reporter = await player_controller.get_or_insert(player_name=msg.reporter)
        reported = await player_controller.get_or_insert(player_name=msg.reported)

    # double check reporter & reported
    if reporter is None:
        logger.error(f"reporter does not exist: '{msg.reporter}'")
        raise ReporterDoesNotExist()

    if reported is None:
        logger.error(f"reported does not exist: '{msg.reported}'")
        raise ReportedDoesNotExist
    report = convert_report_q_to_db(
        reported_id=reported.id,
        reporting_id=reporter.id,
        report_in_queue=msg,
    )
    return report


async def process_msg_v2(msg: ReportInQV2) -> StgReportCreate:
    if msg.ts > 1735736400:
        logger.warning(f"{msg.ts=} > 2025-01-01, {msg=}")
        return None

    gmt = time.gmtime(msg.ts)
    human_time = time.strftime("%Y-%m-%d %H:%M:%S", gmt)
    human_time = datetime.fromtimestamp(msg.ts)
    report = StgReportCreate(
        reportedID=msg.reported_id,
        reportingID=msg.reporter_id,
        timestamp=human_time,
        region_id=msg.region_id,
        x_coord=msg.x_coord,
        y_coord=msg.y_coord,
        z_coord=msg.z_coord,
        manual_detect=bool(msg.manual_detect),
        on_members_world=msg.on_members_world,
        on_pvp_world=bool(msg.on_pvp_world),
        world_number=msg.world_number,
        equip_head_id=msg.equipment.equip_head_id,
        equip_amulet_id=msg.equipment.equip_amulet_id,
        equip_torso_id=msg.equipment.equip_torso_id,
        equip_legs_id=msg.equipment.equip_legs_id,
        equip_boots_id=msg.equipment.equip_boots_id,
        equip_cape_id=msg.equipment.equip_cape_id,
        equip_hands_id=msg.equipment.equip_hands_id,
        equip_weapon_id=msg.equipment.equip_weapon_id,
        equip_shield_id=msg.equipment.equip_shield_id,
        equip_ge_value=msg.equip_ge_value,
    )
    return report


async def process_data(report_queue: Queue, player_cache: SimpleALRUCache):
    """
    Convert kafka messages to Reports, put Reports into the report_Queue
    """
    receive_queue = consumer.get_queue()
    error_queue = producer.get_queue()

    player_controller = PlayerController(cache=player_cache)

    while True:
        # Check if both queues are empty
        if receive_queue.empty():
            await asyncio.sleep(1)
            continue

        raw_msg: dict = await receive_queue.get()
        receive_queue.task_done()

        msg_metadata: dict = raw_msg.get("metadata")
        msg_version = msg_metadata.get("version") if msg_metadata else None

        try:
            if msg_version in [None, "v1.0.0"]:
                msg = ReportInQV1(**raw_msg)
                report = await process_msg_v1(
                    msg=msg, player_controller=player_controller
                )
            elif msg_version in ["v2.0.0"]:
                msg = ReportInQV2(**raw_msg)
                report = await process_msg_v2(msg=msg)
        except (ReporterDoesNotExist, ReportedDoesNotExist):
            continue
        # pydantic error
        except ValidationError as e:
            logger.error({"error": e})
        # database error
        except OperationalError as e:
            await error_queue.put(raw_msg)
            logger.error({"error": e})
            await asyncio.sleep(5)
        if report is None:
            continue
        await report_queue.put(report)


async def main():
    report_queue = Queue(maxsize=1_000)
    batch_queue = Queue(maxsize=10)
    BATCH_SIZE = 1_000

    await producer.start_engine(topic="report")
    await consumer.start_engine(topics=["report"])

    player_cache = SimpleALRUCache()

    for _ in range(5):
        asyncio.create_task(
            process_data(
                report_queue=report_queue,
                player_cache=player_cache,
            )
        )
    asyncio.create_task(
        create_batch(
            batch_size=BATCH_SIZE,
            batch_queue=batch_queue,
            report_queue=report_queue,
        )
    )
    asyncio.create_task(
        insert_batch(
            batch_queue=batch_queue,
            error_queue=producer.send_queue,
        )
    )

    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
