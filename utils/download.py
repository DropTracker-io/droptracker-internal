import aiofiles
import os
import re
from db import Player, models
import uuid

import aiohttp
import asyncio
import inspect

async def download_image(sub_type: str,
                         player: Player,
                         player_wom_id: int,
                         file_data,
                         processed_data):
        # Validate player has a wom_id
        #print(f"API image download in process..... type: {sub_type} player: {player.player_name} player_wom_id: {player_wom_id} file_data: {file_data} processed_data: {processed_data}")
        if not player_wom_id:
            print(f"(download_image) Error: Player missing or has no wom_id: {player_wom_id}")
            return None
            
        base_dir = "/store/droptracker/disc/static/assets/img/user-upload/"
    
        base_url = "https://www.droptracker.io/img/user-upload/"

        # Normalize submission type aliases
        type_map = {
            "npc": "drop",
            "other": "drop",
            "personal_best": "pb",
            "kill_time": "pb",
            "npc_kill": "pb",
            "combat_achievement": "ca",
            "collection_log": "clog",
        }
        canonical_type = type_map.get(sub_type, sub_type)

        def sanitize_filename(filename):
            """Sanitize filename to remove/replace problematic characters"""
            filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
            filename = re.sub(r'\s+', '_', filename)
            return filename.strip('. ')

        # Determine subfolder (path component) based on canonical type
        if canonical_type == "clog":
            raw_component = processed_data.get('source', 'unknown')
        elif canonical_type == "pb":
            raw_component = processed_data.get('boss_name', processed_data.get('npc_name', 'unknown'))
        elif canonical_type == "ca":
            raw_component = processed_data.get('task_tier', processed_data.get('tier', 'unknown'))
        else:
            raw_component = processed_data.get('source', processed_data.get('npc_name', 'unknown'))

        path_component = sanitize_filename(str(raw_component)) if raw_component else ""
        #print(f"Path component: {path_component}")
        # Build directory and URL paths consistently
        directory_path = os.path.join(base_dir, str(player_wom_id), canonical_type)
        if path_component:
            directory_path = os.path.join(directory_path, path_component)
        #print(f"Directory path: {directory_path}")
        url_path = f"{player_wom_id}/{canonical_type}/"
        if path_component:
            url_path = f"{url_path}{path_component}/"

        def generate_unique_filename(directory, base_name_with_ext):
            """Generate unique filename, handling files that already have extensions"""
            # Split the filename and extension
            if '.' in base_name_with_ext:
                base_name, ext = base_name_with_ext.rsplit('.', 1)
            else:
                base_name, ext = base_name_with_ext, 'jpg'
            
            counter = 1
            unique_file_name = f"{base_name}.{ext}"
            while os.path.exists(os.path.join(directory, unique_file_name)):
                unique_file_name = f"{base_name}_{counter}.{ext}"
                counter += 1
            return unique_file_name

        try:
            # Determine target file extension from upload metadata
            ext = 'jpg'
            try:
                upload_filename = getattr(file_data, 'filename', None)
                if upload_filename and '.' in upload_filename:
                    cand = upload_filename.rsplit('.', 1)[1].lower()
                    if cand in {'jpg', 'jpeg', 'png', 'gif', 'webp'}:
                        ext = 'jpg' if cand == 'jpeg' else cand
                else:
                    content_type = getattr(file_data, 'content_type', None) or getattr(file_data, 'mimetype', None)
                    if content_type:
                        if 'jpeg' in content_type:
                            ext = 'jpg'
                        elif 'png' in content_type:
                            ext = 'png'
                        elif 'gif' in content_type:
                            ext = 'gif'
                        elif 'webp' in content_type:
                            ext = 'webp'
            except Exception:
                pass
            # Generate human-friendly filename based on submission type
            if canonical_type == "drop":
                source_name = sanitize_filename(str(processed_data.get("source", processed_data.get("npc_name", "unknown"))))
                item_name = sanitize_filename(str(processed_data.get("item", "unknown")))
                base_name = f"{source_name}_{item_name}" if item_name and source_name else item_name or "image"
                filename = generate_unique_filename(directory_path, f"{base_name}.{ext}")
            elif canonical_type == "pb":
                boss_name = sanitize_filename(str(processed_data.get("boss_name", processed_data.get("npc_name", "unknown"))))
                team_size = sanitize_filename(str(processed_data.get("team_size", "solo")))
                time_value = sanitize_filename(str(processed_data.get("time", "unknown")))
                base_name = f"{boss_name}_{team_size}_{time_value}"
                filename = generate_unique_filename(directory_path, f"{base_name}.{ext}")
            elif canonical_type == "clog":
                item_name = sanitize_filename(str(processed_data.get("item", "unknown")))
                base_name = item_name or "image"
                filename = generate_unique_filename(directory_path, f"{base_name}.{ext}")
            elif canonical_type == "ca":
                task_name = sanitize_filename(str(processed_data.get("task_name", processed_data.get("task", "unknown"))))
                task_tier = sanitize_filename(str(processed_data.get("task_tier", processed_data.get("tier", "unknown"))))
                base_name = f"{task_name}_{task_tier}"
                filename = generate_unique_filename(directory_path, f"{base_name}.{ext}")
            else:
                # Treat unknown types like drops if we have data; else fallback to uuid
                source_name = sanitize_filename(str(processed_data.get("source", processed_data.get("npc_name", ""))))
                item_name = sanitize_filename(str(processed_data.get("item", "")))
                base_name = f"{source_name}_{item_name}".strip("_") or f"submission_{uuid.uuid4()}"
                filename = generate_unique_filename(directory_path, f"{base_name}.{ext}")

            os.makedirs(directory_path, exist_ok=True)
            filepath = os.path.join(directory_path, filename)
            #print(f"Filepath: {filepath}")
            # Save the file robustly, supporting multiple upload backends
            # Always try to rewind any underlying stream first
            try:
                if hasattr(file_data, 'seek'):
                    if inspect.iscoroutinefunction(getattr(file_data, 'seek', None)):
                        await file_data.seek(0)
                    else:
                        try:
                            file_data.seek(0)
                        except Exception:
                            pass
                if hasattr(file_data, 'stream') and hasattr(file_data.stream, 'seek'):
                    try:
                        file_data.stream.seek(0)
                    except Exception:
                        pass
                if hasattr(file_data, 'file') and hasattr(file_data.file, 'seek'):
                    try:
                        file_data.file.seek(0)
                    except Exception:
                        pass
            except Exception as rewind_err:
                print(f"Warning: could not rewind upload stream: {rewind_err}")

            saved = False

            # Case 1: Objects exposing a .save API (e.g., Flask/Quart FileStorage)
            if hasattr(file_data, 'save'):
                save_func = getattr(file_data, 'save')
                try:
                    if inspect.iscoroutinefunction(save_func):
                        await save_func(filepath)
                    else:
                        # Run blocking save in thread to avoid blocking event loop
                        await asyncio.to_thread(save_func, filepath)
                    saved = True
                except Exception as save_err:
                    #print(f"Primary save path failed, will try fallback reader: {type(save_err).__name__}: {save_err}")
                    pass

            # Case 2: Starlette/FastAPI UploadFile-like interfaces (.read/.file)
            if not saved:
                try:
                    if hasattr(file_data, 'read') and inspect.iscoroutinefunction(getattr(file_data, 'read', None)):
                        async with aiofiles.open(filepath, 'wb') as f:
                            while True:
                                chunk = await file_data.read(1024 * 1024)
                                if not chunk:
                                    break
                                await f.write(chunk)
                        saved = True
                    elif hasattr(file_data, 'file'):
                        # Synchronously copy underlying SpooledTemporaryFile to disk off-thread
                        underlying = file_data.file
                        def _copy_sync():
                            import shutil
                            try:
                                underlying.seek(0)
                            except Exception:
                                pass
                            with open(filepath, 'wb') as out_f:
                                shutil.copyfileobj(underlying, out_f)
                        await asyncio.to_thread(_copy_sync)
                        saved = True
                except Exception as fallback_err:
                    print(f"Fallback save path failed: {type(fallback_err).__name__}: {fallback_err}")

            if not saved:
                raise RuntimeError("Unsupported upload file object; could not save image")

            # Verify file size and retry with streaming if zero bytes
            try:
                size_bytes = os.path.getsize(filepath)
                #print(f"Saved file to {filepath} ({size_bytes} bytes)")
                pass
            except Exception:
                size_bytes = -1
            if size_bytes == 0:
                print("Warning: saved image is 0 bytes, attempting stream-based rewrite")
                try:
                    # Rewind where possible
                    try:
                        if hasattr(file_data, 'seek'):
                            file_data.seek(0)
                        if hasattr(file_data, 'file') and hasattr(file_data.file, 'seek'):
                            file_data.file.seek(0)
                    except Exception:
                        pass
                    if hasattr(file_data, 'read') and inspect.iscoroutinefunction(getattr(file_data, 'read', None)):
                        async with aiofiles.open(filepath, 'wb') as f:
                            while True:
                                chunk = await file_data.read(1024 * 1024)
                                if not chunk:
                                    break
                                await f.write(chunk)
                    elif hasattr(file_data, 'file'):
                        def _copy_sync_retry():
                            import shutil
                            try:
                                file_data.file.seek(0)
                            except Exception:
                                pass
                            with open(filepath, 'wb') as out_f:
                                shutil.copyfileobj(file_data.file, out_f)
                        await asyncio.to_thread(_copy_sync_retry)
                    #print(f"Rewrote file to {filepath} ({os.path.getsize(filepath)} bytes)")
                    pass
                except Exception as retry_err:
                    print(f"Retry save failed: {type(retry_err).__name__}: {retry_err}")

            # Add the external URL to the processed data (mirrors filesystem)
            processed_data["image_path"] = f"{base_url}{url_path}{filename}"
            
            #print(f"Saved image to {filepath}")
            #print(f"External URL set to: {processed_data['image_path']}")
            return filepath
        except Exception as e:
            print(f"Error saving image: {e}")
            return None

async def download_player_image(submission_type: str, 
                                file_name: str,
                                player: Player,
                                attachment_url: str,
                                file_extension: str,
                                entry_id: int,  # Generic ID for any submission type
                                entry_name: str,  # Generic name for the entry
                                npc_name: str = ""):
    """
        Images should be stored in:
        /store/droptracker/disc/static/assets/img/user-upload/{player.wom_id}/{submission_type}/{npc_name (optional)}/{entry_name}_{entry_id}.{file_extension}
        This is served externally at:
        https://www.droptracker.io/img/user-upload/{player.wom_id}/{submission_type}/{npc_name (optional)}/{entry_name}_{entry_id}.{file_extension}
    """
    # Validate player has a wom_id
    if not player or not player.wom_id:
        print(f"Error: Player missing or has no wom_id: {player}")
        return None, None
    
    # Base internal directory path for storage
    base_dir = "/store/droptracker/disc/static/assets/img/user-upload/"
    
    # Base external URL for serving images
    base_url = "https://www.droptracker.io/img/user-upload/"

    # Normalize type and sanitize names
    type_map = {
        "npc": "drop",
        "other": "drop",
        "personal_best": "pb",
        "kill_time": "pb",
        "npc_kill": "pb",
        "combat_achievement": "ca",
        "collection_log": "clog",
    }
    canonical_type = type_map.get(submission_type, submission_type)

    def sanitize_filename(filename: str) -> str:
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = re.sub(r'\s+', '_', filename)
        return filename.strip('. ')

    # Build directory structure with optional subfolder
    directory_path = os.path.join(base_dir, str(player.wom_id), canonical_type)
    subfolder = sanitize_filename(str(npc_name)) if npc_name else ""
    if subfolder:
        directory_path = os.path.join(directory_path, subfolder)

    url_path = f"{player.wom_id}/{canonical_type}/"
    if subfolder:
        url_path = f"{url_path}{subfolder}/"

    # Ensure the directory structure exists
    os.makedirs(directory_path, exist_ok=True)

    # Generate unique filename for the download
    def generate_unique_filename(directory, file_name, ext):
        base_name = sanitize_filename(str(file_name)) if file_name else "image"
        counter = 1
        unique_file_name = f"{base_name}.{ext}"
        while os.path.exists(os.path.join(directory, unique_file_name)):
            unique_file_name = f"{base_name}_{counter}.{ext}"
            counter += 1
        return unique_file_name

    # Generate the full filename with entry_name and entry_id
    complete_file_name = f"{sanitize_filename(str(entry_name))}_{entry_id}"
    unique_file_name = generate_unique_filename(directory_path, complete_file_name, file_extension or "jpg")
    download_path = os.path.join(directory_path, unique_file_name)

    # Download the file asynchronously
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(attachment_url) as response:
                if response.status == 200:
                    async with aiofiles.open(download_path, 'wb') as f:
                        while True:
                            chunk = await response.content.read(1024)
                            if not chunk:
                                break
                            await f.write(chunk)
                    # Construct the external URL
                    external_url = f"{base_url}{url_path}{unique_file_name}"
                    print(f"Successfully downloaded image to {download_path}")
                    return download_path, external_url
                else:
                    print(f"HTTP Error {response.status} downloading image from {attachment_url}")
                    return None, None
    except aiohttp.ClientError as e:
        print(f"Network error downloading image from {attachment_url}: {type(e).__name__}: {e}")
        return None, None
    except OSError as e:
        print(f"File system error saving image to {download_path}: {type(e).__name__}: {e}")
        return None, None
    except Exception as e:
        print(f"Unexpected error downloading image from {attachment_url}: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None, None