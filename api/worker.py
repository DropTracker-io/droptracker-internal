import asyncio
# from events.generators.BingoBoardGen import generate_bingo_board
# from events.helpers.images import get_bingo_task_tile_image
# from events.models import EventModel, EventNotification
#from utils.semantic_check import get_npc_id, get_item_id
from utils.wiseoldman import check_user_by_username
import asyncio
from datetime import datetime, timedelta
import json
import interactions
import os
from dotenv import load_dotenv
from tabulate import tabulate
from colorama import Fore, Style, init
import enum
from quart import Quart, request, jsonify, render_template, Blueprint
from quart_cors import cors, route_cors
# from events import models
# from events.models.tasks.TaskFactory import TaskFactory
# from events.models.tasks.BaseTask import BaseTask
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from contextlib import contextmanager
from db import models, Drop, Group, GroupConfiguration, GroupPersonalBestMessage, ItemList, Player, Session, NpcList
from utils import wiseoldman

# Create the blueprint
worker_bp = Blueprint('worker', __name__)

# Load environment variables
load_dotenv()

@contextmanager
def get_db_session():
    """Context manager for database sessions"""
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()

@worker_bp.route("/taskgen")
async def index():
    return await render_template("taskgen.html")

@worker_bp.route("/api/npcs", methods=["GET"])
async def get_npcs():
    try:
        with get_db_session() as session:
            # Use a subquery to get distinct names first
            subquery = session.query(NpcList.npc_id, NpcList.npc_name)\
                .distinct(NpcList.npc_name)\
                .subquery()
            
            npcs = session.query(subquery).all()
            return jsonify([{"id": npc.npc_id, "name": npc.npc_name} for npc in npcs])
    except SQLAlchemyError as e:
        print(f"Database error in get_npcs: {e}")
        return jsonify({"error": "Database error occurred"}), 500
    except Exception as e:
        print(f"Unexpected error in get_npcs: {e}")
        return jsonify({"error": str(e)}), 500

@worker_bp.route("/api/items", methods=["GET"])
async def get_items():
    try:
        with get_db_session() as session:
            # Use a subquery to get distinct names first
            subquery = session.query(ItemList.item_id, ItemList.item_name)\
                .distinct(ItemList.item_name)\
                .filter(ItemList.noted == 0)\
                .subquery()
            
            items = session.query(subquery).all()
            return jsonify([{"id": item.item_id, "name": item.item_name} for item in items])
    except SQLAlchemyError as e:
        print(f"Database error in get_items: {e}")
        return jsonify({"error": "Database error occurred"}), 500
    except Exception as e:
        print(f"Unexpected error in get_items: {e}")
        return jsonify({"error": str(e)}), 500
    
@worker_bp.route('/latest_welcome', methods=['GET'])
async def get_latest_welcome_message():
    with get_db_session() as session:
        try:
            welcome_message: GroupConfiguration = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == 2,
                                                                                GroupConfiguration.config_key == "welcome_message").first()
            if welcome_message is not None:
                if welcome_message.long_value is not None:
                    return welcome_message.long_value
                else:
                    return "Welcome to the DropTracker", 200
            else:
                return "Welcome to the DropTracker", 200
        except Exception as e:
            return jsonify({"error": "Error getting latest welcome message: " + str(e)}), 500
    return "No welcome message found", 200
        
        
@worker_bp.route('/latest_news', methods=['GET'])
async def get_latest_news():
    with get_db_session() as session:
        try:
            news_message: GroupConfiguration = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == 2,
                                                                              GroupConfiguration.config_key == "latest_news").first()
            if news_message is not None:
                if news_message.long_value is not None:
                    return str(news_message.long_value)
                else:
                    return "No news message found", 200
            else:
                return "No news message found", 200
        except Exception as e:
            return jsonify({"error": "Error getting latest news message: " + str(e)}), 500
        

@worker_bp.route("/api/item_by_search", methods=["GET"])
async def get_item_by_search():
    try:
        search = request.args.get("search")
        with get_db_session() as session:
            # Use a subquery to get distinct names first, then filter
            subquery = session.query(ItemList.item_id, ItemList.item_name)\
                .distinct(ItemList.item_name)\
                .filter(ItemList.noted == 0)\
                .subquery()
            
            items = session.query(subquery)\
                .filter(subquery.c.item_name.ilike(f"%{search}%"))\
                .all()
            
            return jsonify([{"id": item.item_id, "name": item.item_name} for item in items])
    except SQLAlchemyError as e:
        print(f"Database error in get_item_by_search: {e}")
        return jsonify({"error": "Database error occurred"}), 500
    except Exception as e:
        print(f"Unexpected error in get_item_by_search: {e}")
        return jsonify({"error": str(e)}), 500

@worker_bp.route("/api/npc_by_search", methods=["GET"])
async def get_npc_by_search():
    try:
        search = request.args.get("search")
        with get_db_session() as session:
            # Use a subquery to get distinct names first, then filter
            subquery = session.query(NpcList.npc_id, NpcList.npc_name)\
                .distinct(NpcList.npc_name)\
                .subquery()
            
            npcs = session.query(subquery)\
                .filter(subquery.c.npc_name.ilike(f"%{search}%"))\
                .all()
            
            return jsonify([{"id": npc.npc_id, "name": npc.npc_name} for npc in npcs])
    except SQLAlchemyError as e:
        print(f"Database error in get_npc_by_search: {e}")
        return jsonify({"error": "Database error occurred"}), 500
    except Exception as e:
        print(f"Unexpected error in get_npc_by_search: {e}")
        return jsonify({"error": str(e)}), 500



### Event routes -- Disabled on dev instances
# @worker_bp.route("/api/tasks", methods=["POST"])
# async def create_task():
#     try:
#         data = await request.get_json()
        
#         # Validate required fields
#         required_fields = ["name", "points", "taskType"]
#         for field in required_fields:
#             if field not in data:
#                 return jsonify({"error": f"Missing required field: {field}"}), 400
        
#         with get_db_session() as session:
#             # Create task based on type
#             task = None
#             if data["taskType"] == "ITEM_COLLECTION":
#                 task_config = data.get("task_config", {})
#                 required_items = task_config.get('required_items', None)
#                 if task_config.get('requires', None) != "set" and task_config.get('requires', None) != "points":
#                     if "required_items" not in data:
#                         return jsonify({"error": "Missing required_items for ITEM_COLLECTION task"}), 400
#                 else:
#                     if task_config.get('items', None) is not None:
#                         ## This is a point-based task, with each item stored in the items list, containing {"item_name": points_earned}
#                         required_items = task_config.get('items', None)
#                         required_points = task_config.get('pts_required', None)
#                         if not required_items or not required_points:
#                             return jsonify({"error": "Missing required items or points for POINTS-BASED ITEM_COLLECTION task"}), 400
#                     elif task_config.get('sets', None) is None:
#                         ## if the sets are empty and we had no 'items' in the task_config, then the task is invalid
#                         return jsonify({"error": "Missing required sets for SET-BASED ITEM_COLLECTION task"}), 400
#                 task = TaskFactory.create_base_item_collection_task(
#                     name=data["name"],
#                     points=data["points"],
#                     required_items=required_items,
#                     description=data.get("description"),
#                     difficulty=data.get("difficulty"),
#                     task_config=task_config
#                 )
#             elif data["taskType"] == "KC_TARGET":
#                 if "task_config" not in data or "source_npcs" not in data["task_config"] or "target_kc" not in data["task_config"]:
#                     return jsonify({"error": "Missing required task_config for KC_TARGET task"}), 400
#                 if not isinstance(data["task_config"]["source_npcs"], list) or len(data["task_config"]["source_npcs"]) == 0:
#                     return jsonify({"error": "source_npcs must be a non-empty list for KC_TARGET task"}), 400
#                 task = TaskFactory.create_base_kc_target_task(
#                     name=data["name"],
#                     points=data["points"],
#                     source_npcs=data["task_config"]["source_npcs"],
#                     target_kc=data["task_config"]["target_kc"],
#                     description=data.get("description"),
#                     difficulty=data.get("difficulty")
#                 )
#             elif data["taskType"] == "XP_TARGET":
#                 if "task_config" not in data or "skill_name" not in data["task_config"] or "target_xp" not in data["task_config"]:
#                     return jsonify({"error": "Missing required task_config for XP_TARGET task"}), 400
#                 task = TaskFactory.create_base_xp_target_task(
#                     name=data["name"],
#                     points=data["points"],
#                     skill_name=data["task_config"]["skill_name"],
#                     target_xp=data["task_config"]["target_xp"],
#                     description=data.get("description"),
#                     difficulty=data.get("difficulty")
#                 )
#             elif data["taskType"] == "EHP_TARGET":
#                 if "task_config" not in data or "target_ehp" not in data["task_config"]:
#                     return jsonify({"error": "Missing required task_config for EHP_TARGET task"}), 400
#                 task = TaskFactory.create_base_ehp_target_task(
#                     name=data["name"],
#                     points=data["points"],
#                     target_ehp=data["task_config"]["target_ehp"],
#                     description=data.get("description"),
#                     difficulty=data.get("difficulty")
#                 )
#             elif data["taskType"] == "EHB_TARGET":
#                 if "task_config" not in data or "target_ehb" not in data["task_config"]:
#                     return jsonify({"error": "Missing required task_config for EHB_TARGET task"}), 400
#                 task = TaskFactory.create_base_ehb_target_task(
#                     name=data["name"],
#                     points=data["points"],
#                     target_ehb=data["task_config"]["target_ehb"],
#                     description=data.get("description"),
#                     difficulty=data.get("difficulty")
#                 )
#             elif data["taskType"] == "LOOT_VALUE":
#                 if "task_config" not in data or "target_value" not in data["task_config"]:
#                     return jsonify({"error": "Missing required task_config for LOOT_VALUE task"}), 400
#                 source_npcs = data["task_config"].get("source_npcs")
#                 if source_npcs is not None and (not isinstance(source_npcs, list) or len(source_npcs) == 0):
#                     return jsonify({"error": "source_npcs must be a non-empty list for LOOT_VALUE task"}), 400
#                 task = TaskFactory.create_base_loot_value_task(
#                     name=data["name"],
#                     points=data["points"],
#                     target_value=data["task_config"]["target_value"],
#                     source_npcs=source_npcs,
#                     description=data.get("description"),
#                     difficulty=data.get("difficulty")
#                 )
#             elif data["taskType"] == "CUSTOM":
#                 if "task_config" not in data:
#                     return jsonify({"error": "Missing required task_config for CUSTOM task"}), 400
#                 task = TaskFactory.create_base_custom_task(
#                     name=data["name"],
#                     points=data["points"],
#                     task_config=data["task_config"],
#                     description=data.get("description"),
#                     difficulty=data.get("difficulty")
#                 )
#             else:
#                 return jsonify({"error": f"Invalid task type: {data['taskType']}"}), 400
            
#             # Save task to database
#             session.add(task)
#             session.commit()
            
#             return jsonify({"message": "Task created successfully", "task_id": task.id}), 201
            
#     except SQLAlchemyError as e:
#         print(f"Database error in create_task: {e}")
#         return jsonify({"error": "Database error occurred"}), 500
#     except Exception as e:
#         print(f"Unexpected error in create_task: {e}")
#         return jsonify({"error": str(e)}), 500
    
# @worker_bp.route("/api/tasks/import", methods=["GET", "POST"])
# async def import_tasks():
#     if request.method == "POST":
#         data = await request.get_json()
#         print(data)
#         return jsonify({"message": "Tasks imported successfully"}), 200
#     return await render_template("task_import.html")

# @worker_bp.route("/api/tasks", methods=["GET"])
# async def get_tasks():
#     try:
#         with get_db_session() as session:
#             raw_tasks = session.query(BaseTask).all()
#             tasks = []
#             for task in raw_tasks:
#                 tasks.append({"id": task.id, "name": task.name, "points": task.points, 
#                             "task_type": task.task_type.name, "description": task.description, 
#                             "difficulty": task.difficulty, "config": task.task_config})
#             return jsonify(tasks)
#     except SQLAlchemyError as e:
#         print(f"Database error in get_tasks: {e}")
#         return jsonify({"error": "Database error occurred"}), 500
#     except Exception as e:
#         print(f"Unexpected error in get_tasks: {e}")
#         return jsonify({"error": str(e)}), 500

# @worker_bp.route("/api/tasks/<int:task_id>", methods=["GET"])
# async def get_task(task_id):
#     try:
#         with get_db_session() as session:
#             task = session.query(BaseTask).filter(BaseTask.id == task_id).first()
#             if not task:
#                 return jsonify({"error": "Task not found"}), 404
#             return jsonify({"id": task.id, "name": task.name, "points": task.points, 
#                             "task_type": task.task_type.name, "description": task.description, 
#                             "difficulty": task.difficulty, "config": task.task_config})
#     except SQLAlchemyError as e:
#         print(f"Database error in get_task: {e}")
#         return jsonify({"error": "Database error occurred"}), 500
#     except Exception as e:
#         print(f"Unexpected error in get_task: {e}")
#         return jsonify({"error": str(e)}), 500
    

# @worker_bp.route("/get_board", methods=["GET"])
# @route_cors(allow_origin="https://www.droptracker.io")
# async def get_board_from_event():
#     event_id = request.args.get("event_id")
#     team_id = request.args.get("team_id")
#     board_url = generate_bingo_board(event_id, team_id)
#     return jsonify({"board_url": board_url})

# @worker_bp.route("/api/notifications", methods=["GET"])
# async def get_notifications():
#     with get_db_session() as session:
#         notifications = session.query(EventNotification).all()
#         return jsonify([{"id": notification.id, "message": notification.message, "status": notification.status} for notification in notifications])
    
# @worker_bp.route("/events/send_participant_invite", methods=["POST", "GET"])
# async def send_participant_invite():
#     event_id = request.args.get("event_id")
#     with get_db_session() as session:
#         event = session.query(EventModel).filter(EventModel.id == event_id).first()
#         group_id = event.group_id
#         notification = EventNotification(
#             event_id=event_id,
#             notification_type="player_invite_message",
#             group_id=group_id,
#             message=f"Admin sent player invite button to join {event.id}",
#             status="pending"
#         )
#         session.add(notification)
#         session.commit()
#     return jsonify({"message": "Invite sent successfully"}), 200




# @worker_bp.route("/api/task_tile", methods=["GET"])
# @route_cors(allow_origin="https://www.droptracker.io")
# async def get_task_tile():
#     try:
#         print("Starting get_task_tile request")
#         task_id = request.args.get("task_id")
#         print(f"Received task_id: {task_id}")
        
#         with get_db_session() as session:
#             print("Querying database for task")
#             task = session.query(BaseTask).filter(BaseTask.id == task_id).first()
#             if not task:
#                 print(f"Task not found for id: {task_id}")
#                 return jsonify({"error": "Task not found"}), 404
                
#             print(f"Found task: id={task.id}, type={task.task_type}, name={task.name}")
#             print(f"Task config: {task.task_config}")
            
#             print("Calling get_bingo_task_tile_image")
#             tile_url = await get_bingo_task_tile_image(task)
#             print(f"Generated tile URL: {tile_url}")
            
#             return jsonify({"tile_url": tile_url})
#     except SQLAlchemyError as e:
#         print(f"Database error in get_task_tile: {e}")
#         return jsonify({"error": "Database error occurred"}), 500
#     except Exception as e:
#         print(f"Unexpected error in get_task_tile: {e}")
#         print(f"Error type: {type(e)}")
#         import traceback
#         print(f"Traceback: {traceback.format_exc()}")
#         return jsonify({"error": str(e)}), 500


# Export the blueprint
def create_blueprint():
    return worker_bp
