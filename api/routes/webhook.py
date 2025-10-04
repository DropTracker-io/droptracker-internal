import asyncio
from datetime import datetime, timedelta

from quart import Blueprint, jsonify, request
from quart_rate_limiter import rate_limit

from api.core import logger, get_db_session, metrics
from data import submissions
from db import Player
from utils.download import download_image


webhook_bp = Blueprint("webhook", __name__)


@webhook_bp.post("/submit")
@rate_limit(limit=10, period=timedelta(seconds=1))
async def submit_data():
    return await webhook_data()


@webhook_bp.post("/webhook")
@rate_limit(limit=100, period=timedelta(seconds=1))
async def webhook_data():
    success = False
    request_type = "webhook"
    submission_type = None
    db_session = None

    try:
        content_type = request.headers.get('Content-Type', '')
        if 'multipart/form-data' in content_type:
            try:
                form = await request.form
                payload_json = form.get('payload_json')
                if not payload_json:
                    return jsonify({"error": "No payload_json found in form data"}), 400

                import json
                webhook_payload = json.loads(payload_json)
                if webhook_payload is None:
                    return jsonify({"error": "Invalid JSON in payload_json"}), 400

                files = await request.files
                image_file = files.get('file') if files else None

                processed_items = await process_webhook_data(webhook_payload)
                logger.log_sync("info", f"Processed webhook data: {processed_items}")
                if not processed_items:
                    return jsonify({"error": "Could not process webhook data"}), 400

                submission_type = processed_items[0].get("type")
                processed_items[0]["downloaded"] = False

                db_session = get_db_session()
                response = None
                try:
                    downloaded = False
                    file_path = None
                    for processed_data in processed_items:
                        submission_type = processed_data.get("type")
                        processed_data["downloaded"] = False

                        if image_file:
                            processed_data["has_image"] = True
                            image_session = get_db_session()
                            player_name = processed_data.get('player', processed_data.get('player_name', None))
                            try:
                                player = image_session.query(Player).filter(Player.player_name == player_name).first()
                                player_wom_id = player.wom_id if player else None
                                if player:
                                    file_path = await download_image(sub_type=processed_data.get('type', 'unknown'), player=player, player_wom_id=player_wom_id, file_data=image_file, processed_data=processed_data)
                                    if file_path:
                                        if processed_data.get("image_path"):
                                            file_path = processed_data["image_path"]
                                        processed_data["image_url"] = file_path
                                        processed_data["downloaded"] = True
                                        downloaded = True
                            finally:
                                image_session.close()
                        else:
                            if file_path:
                                if processed_data.get("image_path"):
                                    processed_data["image_url"] = processed_data["image_path"]
                                else:
                                    processed_data["image_url"] = file_path
                        processed_data['used_api'] = True
                        match (submission_type):
                            case "drop" | "other"| "npc":
                                submission_type = "drop"
                                response = await submissions.drop_processor(processed_data, external_session=db_session)
                                logger.log_sync("info", f"drop_processor response: {response}")
                            case "collection_log":
                                submission_type = "collection_log"
                                await submissions.clog_processor(processed_data, external_session=db_session)
                                logger.log_sync("info", f"clog_processor response: {response}")
                            case "personal_best" | "kill_time" | "npc_kill":
                                submission_type = "personal_best"
                                await submissions.pb_processor(processed_data, external_session=db_session)
                                logger.log_sync("info", f"pb_processor response: {response}")
                            case "combat_achievement":
                                submission_type = "combat_achievement"
                                await submissions.ca_processor(processed_data, external_session=db_session)
                                logger.log_sync("info", f"ca_processor response: {response}")
                            case "experience_update" | "experience_milestone" | "level_up":
                                continue
                            case "quest_completion":
                                continue
                            case "pet":
                                await submissions.pet_processor(processed_data, external_session=db_session)
                                logger.log_sync("info", f"pet_processor response: {response}")
                                continue
                            case "adventure_log":
                                await submissions.adventure_log_processor(processed_data, external_session=db_session)
                                logger.log_sync("info", f"adventure_log_processor response: {response}")
                                continue
                            case _:
                                continue

                    db_session.commit()
                    success = True

                except Exception as processor_error:
                    logger.log_sync("error", f"Processor error: {processor_error}")
                    if db_session:
                        db_session.rollback()
                    return jsonify({"error": f"Error processing data: {str(processor_error)}"}), 200
                if response:
                    return jsonify({"message": response.message, "notice": response.notice}), 200
                else:
                    return jsonify({"message": "Webhook data processed successfully"}), 200

            except Exception as e:
                logger.log_sync("error", f"Error processing multipart request: {e}")
                return jsonify({"error": f"Error processing request: {str(e)}"}), 400
        else:
            try:
                data = await request.get_json()
                return jsonify({"message": "JSON data processed"}), 200
            except Exception as e:
                logger.log_sync("error", f"Error processing JSON request: {e}")
                return jsonify({"error": f"Error processing request: {str(e)}"}), 400
    except Exception as e:
        logger.log_sync("error", f"Webhook Exception: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if db_session:
            try:
                db_session.close()
            except Exception:
                pass
        if submission_type:
            metrics.record_request(submission_type, success, app="new_api")
        else:
            metrics.record_request(request_type, success, app="new_api")


async def process_webhook_data(webhook_data):
    try:
        embeds = webhook_data.get("embeds", [])
        if not embeds:
            print("No embeds found in webhook data")
            return None
        processed_items = []
        for embed in embeds:
            processed_data = {
                field["name"]: field["value"] for field in embed.get("fields", [])
            }
            processed_data["timestamp"] = datetime.now().isoformat()
            processed_items.append(processed_data)
        return processed_items
    except Exception as e:
        print(f"Error processing webhook data: {e}")
        return None


