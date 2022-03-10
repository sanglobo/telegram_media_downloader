"""Downloads media from telegram."""
import asyncio
import logging
import os
import re
from datetime import datetime as dt
from typing import List, Optional, Tuple, Union
from dotenv import load_dotenv
import pyrogram
import yaml
import zc.lockfile
from pyrogram import errors
from pyrogram.types import Audio, Document, Photo, Video, VideoNote, Voice
from rich.logging import RichHandler

from utils.file_management import get_next_name, manage_duplicate_file
from utils.log import LogFilter
from utils.meta import print_meta
from utils.updates import check_for_updates

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[
        RichHandler(
            markup=True,
            rich_tracebacks=True
        )
    ]
)
logging.getLogger("pyrogram.session.session").addFilter(LogFilter())
logging.getLogger("pyrogram.client").addFilter(LogFilter())
logger = logging.getLogger("media_downloader")

WORKING_DIRECTORY = os.getcwd()
DOWNLOAD_DIR = WORKING_DIRECTORY


def update_config(config: dict):
    """
    Update existing configuration file.

    Parameters
    ----------
    config: dict
        Configuration to be written into config file.
    """
    logger.info("Updating config file")
    with open("config.yaml", "w") as yaml_file:
        yaml.dump(config, yaml_file, default_flow_style=False)
    logger.info("Updated last read message_id to config file")


def add_failed_id(config: dict, chat_id: int, message_id: int):
    """
    Add failed message_id to config file.
    config: dict
        Configuration to be written into config file.
    chat_id: int
        Chat id of the failed message.
    message_id: int
        Message id of the failed message.
    """
    if chat_id is not None and message_id is not None:
        logger.info(f"Adding failed message_id {message_id} to config file")
        ids_to_retry: dict[str, list[int]] = config["ids_to_retry"]
        ids_to_retry[str(chat_id)].append(message_id)
        config["ids_to_retry"] = ids_to_retry
    update_config(config=config)


def _can_download(
        _type: str, file_formats: dict, file_format: Optional[str]
) -> bool:
    """
    Check if the given file format can be downloaded.

    Parameters
    ----------
    _type: str
        Type of media object.
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types
    file_format: str
        Format of the current file to be downloaded.

    Returns
    -------
    bool
        True if the file format can be downloaded else False.
    """
    if _type in ["audio", "document", "video"]:
        allowed_formats: list = file_formats[_type]
        if file_format not in allowed_formats and allowed_formats[0] != "all":
            return False
    return True


def _is_exist(file_path: str) -> bool:
    """
    Check if a file exists and it is not a directory.

    Parameters
    ----------
    file_path: str
        Absolute path of the file to be checked.

    Returns
    -------
    bool
        True if the file exists else False.
    """
    return not os.path.isdir(file_path) and os.path.exists(file_path)


async def _get_media_meta(
        media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice],
        _type: str, chatId: str,
) -> Tuple[str, Optional[str]]:
    """Extract file name and file id from media object.

    Parameters
    ----------
    media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice]
        Media object to be extracted.
    _type: str
        Type of media object.

    Returns
    -------
    Tuple[str, Optional[str]]
        file_name, file_format
    """
    if _type in ["audio", "document", "video"]:
        # pylint: disable = C0301
        file_format: Optional[str] = media_obj.mime_type.split("/")[-1]  # type: ignore
    else:
        file_format = None

    if _type in ["voice", "video_note"]:
        # pylint: disable = C0209
        file_format = media_obj.mime_type.split("/")[-1]  # type: ignore
        file_name: str = os.path.join(
            DOWNLOAD_DIR,
            chatId,
            _type,
            "{}_{}.{}".format(
                _type,
                dt.utcfromtimestamp(media_obj.date).isoformat(),  # type: ignore
                file_format,
            ),
        )
    else:
        file_name = os.path.join(
            DOWNLOAD_DIR, chatId, _type, getattr(media_obj, "file_name", None) or ""
        )
    return file_name, file_format


async def download_media(
        client: pyrogram.client.Client,
        message: pyrogram.types.Message,
        media_types: List[str],
        file_formats: dict,
        config: dict,
        chatname: str,
):
    """
    Download media from Telegram.

    Each of the files to download are retried 3 times with a
    delay of 5 seconds each.

    Parameters
    ----------
    client: pyrogram.client.Client
        Client to interact with Telegram APIs.
    message: pyrogram.types.Message
        Message object retrieved from telegram.
    media_types: list
        List of strings of media types to be downloaded.
        Ex : `["audio", "photo"]`
        Supported formats:
            * audio
            * document
            * photo
            * video
            * voice
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types.
    config: dict
        Dictionary containing the config file.
    Returns
    -------
    int
        Current message id.
    """
    for retry in range(3):
        try:
            if message.media is None:
                return message.message_id
            for _type in media_types:
                _media = getattr(message, _type, None)
                if _media is None:
                    continue
                file_name, file_format = await _get_media_meta(_media, _type, str(chatname))
                if _can_download(_type, file_formats, file_format):
                    if _is_exist(file_name):
                        file_name = get_next_name(file_name)
                        download_path = await client.download_media(
                            message, file_name=file_name
                        )
                        # pylint: disable = C0301
                        download_path = manage_duplicate_file(download_path)  # type: ignore
                    else:
                        download_path = await client.download_media(
                            message, file_name=file_name
                        )
                    if download_path:
                        logger.info("Media downloaded - %s", download_path)
            break
        except errors.exceptions.bad_request_400.BadRequest:
            logger.warning(
                "Message[%d]: file reference expired, fetching...",
                message.message_id,
            )
            message = await client.get_messages(  # type: ignore
                chat_id=message.chat.id,  # type: ignore
                message_ids=message.message_id,
            )
            if retry == 2:
                # pylint: disable = C0301
                logger.error(
                    "Message[%d]: file reference expired for 3 retries, download skipped.",
                    message.message_id,
                )
                add_failed_id(config=config, message_id=message.message_id, chat_id=message.chat.id)
        except TypeError:
            # pylint: disable = C0301
            logger.warning(
                "Timeout Error occurred when downloading Message[%d], retrying after 5 seconds",
                message.message_id,
            )
            await asyncio.sleep(5)
            if retry == 2:
                logger.error(
                    "Message[%d]: Timing out after 3 reties, download skipped.",
                    message.message_id,
                )
                add_failed_id(config=config, message_id=message.message_id, chat_id=message.chat.id)
        except Exception as e:
            # pylint: disable = C0301
            logger.error(
                "Message[%d]: could not be downloaded due to following exception:\n[%s].",
                message.message_id,
                e,
                exc_info=True,
            )
            add_failed_id(config=config, message_id=message.message_id, chat_id=message.chat.id)
            break
    return message.message_id


async def process_messages(
        client: pyrogram.client.Client,
        messages: List[pyrogram.types.Message],
        media_types: List[str],
        config: dict,
        file_formats: dict,
        chatname: str,
) -> int:
    """
    Download media from Telegram.

    Parameters
    ----------
    client: pyrogram.client.Client
        Client to interact with Telegram APIs.
    messages: list
        List of telegram messages.
    media_types: list
        List of strings of media types to be downloaded.
        Ex : `["audio", "photo"]`
        Supported formats:
            * audio
            * document
            * photo
            * video
            * voice
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types.
    config: dict
        Dictionary containing the configuration.
    Returns
    -------
    int
        Max value of list of message ids.
    """
    message_ids = await asyncio.gather(
        *[
            download_media(client, message, media_types, file_formats, config=config, chatname=chatname)
            for message in messages
        ]
    )

    last_message_id = max(message_ids)
    return last_message_id


async def begin_import(config: dict, pagination_limit: int) -> dict:
    """
    Create pyrogram client and initiate download.

    The pyrogram client is created using the ``api_id``, ``api_hash``
    from the config and iter through message offset on the
    ``last_message_id`` and the requested file_formats.

    Parameters
    ----------
    config: dict
        Dict containing the config to create pyrogram client.
    pagination_limit: int
        Number of message to download asynchronously as a batch.

    Returns
    -------
    dict
        Updated configuration to be written into config file.
    """
    client = pyrogram.Client(
        "media_downloader",
        api_id=os.environ.get('API_ID', config["api_id"]),
        api_hash=os.environ.get('API_HASH', config["api_hash"]),
        phone_number=os.environ.get('PHONE_NUMBER', config["phone_number"]),
    )
    await client.start()
    chat_id = config["chat_id"]

    lastMessageIds: dict = config["last_read_message_id"]
    # force to dict
    lastMessageIds = dict(lastMessageIds)

    dialogs = client.iter_dialogs(
        limit=pagination_limit
    )
    async for dialog in dialogs:
        # get chat name for folder
        chatname = dialog.chat.id
        if chat_id is not None and chat_id != "" and dialog.chat.type == "private":
            continue
        if dialog.chat.username is not None:
            chatname = dialog.chat.username
        elif dialog.chat.title is not None:
            chatname = re.sub(r'[^A-Za-z0-9 ]+', '', dialog.chat.title)
        if chat_id is not None and chat_id != "" and dialog.chat.id != chat_id:
            continue
        chatname = str(chatname).replace(" ", "_")


        offset_id: int = 0
        if str(dialog.chat.id) in lastMessageIds:
            offset_id = lastMessageIds[str(dialog.chat.id)]
        messages_iter = client.iter_history(
            chat_id=dialog.chat.id,
            offset_id=offset_id,
            reverse=True,
        )
        pagination_count: int = 0
        messages_list: list = []
        async for message in messages_iter:
            if message is None or message.message_id is None:
                continue
            if pagination_count != pagination_limit:
                pagination_count += 1
                messages_list.append(message)
            else:
                last_read_message_id = await process_messages(
                    client=client,
                    messages=messages_list,
                    media_types=config["media_types"],
                    file_formats=config["file_formats"],
                    config=config,
                    chatname=chatname,
                )
                pagination_count = 0
                messages_list = [message]
                lastMessageIds.update({str(dialog.chat.id): last_read_message_id})
                config["last_read_message_id"] = lastMessageIds
                update_config(config)
        if messages_list:
            last_read_message_id = await process_messages(
                client=client,
                messages=messages_list,
                media_types=config["media_types"],
                file_formats=config["file_formats"],
                config=config,
                chatname=chatname,
            )
            lastMessageIds.update({str(dialog.chat.id): last_read_message_id})

    await client.stop()
    config["last_read_message_id"] = lastMessageIds
    return config


def main():
    # TODO: verify config file and directory access
    zc.lockfile.LockFile('lock')
    load_dotenv()
    """Main function of the downloader."""
    with open(os.path.join(WORKING_DIRECTORY, "config.yaml")) as f:
        config = yaml.safe_load(f)
        global DOWNLOAD_DIR
        if os.environ.get('DOWNLOAD_DIRECTORY', config["download_directory"]) != "":
            DOWNLOAD_DIR = os.path.normpath(os.environ.get('DOWNLOAD_DIRECTORY', config["download_directory"]))

        updated_config = asyncio.run(
            begin_import(config, pagination_limit=100), debug=True
        )
    '''
    if FAILED_IDS:
        logger.info(
            "Downloading of %d files failed. "
            "Failed message ids are added to config file.\n"
            "Functionality to re-download failed downloads will be added "
            "in the next version of `Telegram-media-downloader`",
            len(set(FAILED_IDS)),
        )
    '''
    update_config(updated_config)
    #check_for_updates()


if __name__ == "__main__":
    print_meta(logger)
    main()
