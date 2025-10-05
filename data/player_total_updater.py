### This separate process is used to run player update cycles in the background, 
# as opposed to holding up the main process's ability to respond to requests, etc.

import asyncio
from datetime import datetime, timedelta
import time
import signal
import sys
import aiohttp
import quart
from quart import Quart, request
import os
from dotenv import load_dotenv
import logging
from monitor.sdnotifier import SystemdWatchdog

from sqlalchemy import func
from db.models import Group, LBUpdate, session, Player, User, Drop, Session
from services import redis_updates
# from db.update_player_total import update_player_in_redis
#from db.update_player_total import update_player_in_redis
from lootboard.generator import generate_server_board
from utils.github import GithubPagesUpdater

from utils.redis import redis_client

from db.app_logger import AppLogger

app_logger = AppLogger()

# Dictionary to track recently updated players: {player_id: timestamp}
recently_updated = {}
# Cooldown period in seconds (60 minutes)
UPDATE_COOLDOWN = 3600

# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Create the Quart application
app = Quart(__name__)

# Global variables for systemd watchdog
watchdog = None
shutdown_event = asyncio.Event()

# Health check function for systemd watchdog
async def health_check():
    """Lightweight health check for the player update service"""
    try:
        # Basic health check - service is running if we get here
        # Don't do blocking operations in health check to avoid watchdog timeouts
        return True
    except Exception as e:
        print(f"Health check failed: {e}")
        return False

# Signal handlers for graceful shutdown
def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)

def delete_player_keys(player_id, batch_size=100):
    """
    Delete player keys in batches to avoid blocking.
    """
    pattern = f"player:{player_id}:*"
    keys = []
    for key in redis_client.client.scan_iter(pattern, count=batch_size):
        keys.append(key)
        if len(keys) >= batch_size:
            redis_client.client.delete(*keys)
            keys = []
    if keys:
        redis_client.client.delete(*keys)   

async def send_watchdog_heartbeat():
    """Send a manual watchdog heartbeat"""
    global watchdog
    if watchdog and watchdog.notifier:
        try:
            watchdog.notifier.notify("WATCHDOG=1")
            print("Sent manual watchdog heartbeat")
        except Exception as e:
            print(f"Failed to send watchdog heartbeat: {e}")

# Define routes
@app.route('/')
async def index():
    return "Player Update Service is running!"

@app.route('/health')
async def health_check_route():
    return {"status": "healthy"}

async def update_players():
    """Enhanced update_players with watchdog notifications"""
    global watchdog
    
    while not shutdown_event.is_set():
        print("Player update loop beginning...")
        cycle_start_time = time.time()
        
        try:
            local_session = Session()
            
            # Send watchdog heartbeat at start of cycle
            await send_watchdog_heartbeat()
            
            players_to_update = local_session.query(Player).filter(
                Player.date_updated < datetime.now() - timedelta(days=14)
            ).all()
            
            print(f"Found {len(players_to_update)} players to update, limiting to 2 per iteration...")
            players_to_update = players_to_update[:2]
            
            if not players_to_update:
                print("No players to update")
                local_session.close()
                await asyncio.sleep(30)
                continue
            
            for i, player in enumerate(players_to_update):
                try:
                    player_start_time = time.time()
                    print(f"Updating player {player.player_id} ({i+1}/{len(players_to_update)})")
                    
                    # Send watchdog heartbeat before starting player update
                    await send_watchdog_heartbeat()
                    
                    # Run the player update in a thread to avoid blocking
                    def update_player_sync():
                        return redis_updates.force_update_player(
                            player_id=player.player_id, 
                            session_to_use=local_session
                        )
                    
                    # Execute the blocking operation in a thread
                    loop = asyncio.get_event_loop()
                    update_result = await loop.run_in_executor(None, update_player_sync)
                    
                    player_elapsed = time.time() - player_start_time
                    print(f"Updated player {player.player_id} in {player_elapsed:.2f}s - Result: {update_result}")
                    
                    # Send another watchdog heartbeat after player update
                    await send_watchdog_heartbeat()
                    
                    # Small delay between players to allow other operations
                    if i < len(players_to_update) - 1:
                        await asyncio.sleep(1)
                        
                except Exception as e:
                    print(f"Error updating player {player.player_id}: {e}")
                    app_logger.log(
                        log_type="error", 
                        data=f"Error updating player {player.player_id}: {e}", 
                        app_name="player_updates", 
                        description="update_players"
                    )
                    continue
                    
        except Exception as e:
            print(f"Error updating players: {e}")
            if 'local_session' in locals():
                local_session.rollback()
            app_logger.log(
                log_type="error", 
                data=f"Error updating players: {e}", 
                app_name="player_updates", 
                description="update_players"
            )
        finally:
            if 'local_session' in locals():
                local_session.close()
        
        cycle_elapsed = time.time() - cycle_start_time
        print(f"Player update loop finished in {cycle_elapsed:.2f}s")
        
        # Send final watchdog heartbeat for this cycle
        await send_watchdog_heartbeat()
        
        # Wait with periodic heartbeats during sleep
        await sleep_with_watchdog_heartbeats(30)

async def sleep_with_watchdog_heartbeats(sleep_duration: int):
    """
    Sleep for the specified duration while sending periodic watchdog heartbeats
    and checking for shutdown signals.
    """
    heartbeat_interval = 10  # Send heartbeat every 10 seconds during sleep
    elapsed = 0
    
    while elapsed < sleep_duration and not shutdown_event.is_set():
        sleep_time = min(heartbeat_interval, sleep_duration - elapsed)
        await asyncio.sleep(sleep_time)
        elapsed += sleep_time
        
        # Send heartbeat if we're still waiting
        if elapsed < sleep_duration and not shutdown_event.is_set():
            await send_watchdog_heartbeat()

@app.route('/update', methods=['POST'])
async def update():
    data = await request.get_json()
    player_id = data.get('player_id')
    force_update = True
    print(f"Received update request for player {player_id}. Force update: {force_update}")
    
    # Send watchdog heartbeat at start of manual update
    await send_watchdog_heartbeat()
    
    # Check if player was recently updated
    current_time = time.time()
    if player_id in recently_updated:
        last_update = recently_updated[player_id]
        time_since_update = current_time - last_update
        with Session() as session:
            player = session.query(Player).filter(Player.player_id == player_id).first()
            if player:
                player.date_updated = datetime.now()
                session.commit()
        
        if time_since_update < UPDATE_COOLDOWN:
            minutes_ago = int(time_since_update / 60)
            return {"status": "skipped", "reason": f"Updated {minutes_ago} minutes ago"}
    
    with Session() as session:
        try:
            print("Attempting to get player...")
            player = session.query(Player).filter(Player.player_id == player_id).first()
            if player:
                print("Player found, attempting to update using optimized method...")
                
                # Send heartbeat before starting update
                await send_watchdog_heartbeat()
                
                # Run the update in a thread to avoid blocking
                def update_player_sync():
                    return redis_updates.force_update_player(player.player_id, session)
                
                loop = asyncio.get_event_loop()
                updated = await loop.run_in_executor(None, update_player_sync)
                
                # Send heartbeat after update
                await send_watchdog_heartbeat()
                
                print("Returned:", updated)
                if updated and updated == True:
                    # Record the update time
                    recently_updated[player_id] = current_time
                    player.date_updated = datetime.now()
                    session.commit()
                    print("Updated player properly.")
                    return {"status": "updated"}
                else:
                    print("Didn't update player properly.")
                    return {"status": "failed"}
            else:
                print("Player not found.")
                return {"status": "player not found"}
        except Exception as e:
            print(f"Error in manual update: {e}")
            session.rollback()
            return {"status": "failed", "error": str(e)}

async def github_update_loop():
    """Enhanced github_update_loop with watchdog notifications"""
    if os.getenv("STATUS") == "dev" or os.getenv("STATE") == "dev":
        print("Skipping GitHub update loop on dev instance")
        ## Do not perform github updates on dev instances
        return
    last_update = redis_client.client.get("github_update_last_timestamp")
    if last_update:
        if last_update > datetime.now() - timedelta(minutes=30):
            ## Ensure we are only updating once per 30 minutes, at minimum
            return
    redis_client.client.set("github_update_last_timestamp", datetime.now().isoformat())
    updater = GithubPagesUpdater()
    app_logger.log(log_type="access", data=f"Started GitHub update loop", app_name="player_updates", description="github_update_loop")
    
    while not shutdown_event.is_set():
        try:
            # Send watchdog heartbeat before GitHub update
            await send_watchdog_heartbeat()
            
            # Pass watchdog instance to prevent timeout during webhook checking
            await updater.update_github_pages(watchdog)
            
            # Send watchdog heartbeat after GitHub update
            await send_watchdog_heartbeat()
            
        except Exception as e:
            print(f"Error in GitHub update loop: {e}")
        
        # Sleep with periodic heartbeats (1 hour = 3600 seconds)
        await sleep_with_watchdog_heartbeats(3600)

# Background task for player updates
@app.before_serving
async def setup_background_tasks():
    app.update_task = asyncio.create_task(update_players())
    app.github_task = asyncio.create_task(github_update_loop())
    app_logger.log(log_type="access", data=f"Started background tasks", app_name="player_updates", description="setup_background_tasks")

async def get_all_groups(session_to_use = None):
    if session_to_use is not None:
        session = session_to_use
    groups = session.query(Group).all()
    return groups

@app.after_serving
async def cleanup_background_tasks():
    """Enhanced cleanup with proper task cancellation"""
    print("Shutting down background tasks...")
    
    # Signal shutdown to all loops
    shutdown_event.set()
    
    # Cancel tasks
    if hasattr(app, 'update_task'):
        app.update_task.cancel()
        try:
            await app.update_task
        except asyncio.CancelledError:
            pass
    
    if hasattr(app, 'github_task'):
        app.github_task.cancel()
        try:
            await app.github_task
        except asyncio.CancelledError:
            pass
    
    app_logger.log(log_type="access", data=f"Background tasks were cancelled", app_name="player_updates", description="cleanup_background_tasks")

async def main():
    """Main function with systemd watchdog integration"""
    global watchdog
    
    # Setup signal handlers
    setup_signal_handlers()
    
    # Initialize systemd watchdog
    watchdog = SystemdWatchdog()
    watchdog.set_health_check(health_check)
    
    # Get port from environment variable or use default
    port = int(os.getenv("PLAYER_UPDATE_PORT", 21475))
    
    try:
        async with watchdog:
            # Notify systemd that we're ready
            await watchdog.notify_ready()
            print("Systemd watchdog initialized and ready notification sent")
            app_logger.log(log_type="access", data=f"Starting Player Update Service on port {port}", app_name="player_updates", description="main")
            
            # Start the Quart app
            app_task = asyncio.create_task(app.run_task(host='0.0.0.0', port=port))
            
            # Wait for either app to complete or shutdown signal
            done, pending = await asyncio.wait(
                [app_task, asyncio.create_task(shutdown_event.wait())],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # If shutdown was requested, cancel the app task
            if shutdown_event.is_set():
                print("Shutdown requested, stopping Player Update Service...")
                if not app_task.done():
                    app_task.cancel()
                    try:
                        await app_task
                    except asyncio.CancelledError:
                        pass
            
            print("Player Update Service shutting down gracefully...")
            
    except KeyboardInterrupt:
        print("Received keyboard interrupt")
    except Exception as e:
        print(f"Fatal error in main: {e}")
        app_logger.log(log_type="error", data=f"Fatal error in main: {e}", app_name="player_updates", description="main")
        raise
    finally:
        app_logger.log(log_type="access", data=f"Player Update Service cleanup completed", app_name="player_updates", description="main")
        print("Player Update Service cleanup completed")

if __name__ == '__main__':
    asyncio.run(main())

