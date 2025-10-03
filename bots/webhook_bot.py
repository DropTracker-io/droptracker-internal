import interactions
import os
import json
import asyncio
import signal
import sys
from datetime import datetime
from dotenv import load_dotenv
from interactions.api.events import MemberUpdate, MessageCreate, MessageReactionAdd, Startup
from interactions import Embed, Intents, Message, ChannelType, OptionType, listen, slash_command, Permissions, slash_option
from interactions.models import Member
from db.models import Group, ItemList, PersonalBestEntry, PlayerPet, Session, Player, User, UserConfiguration
from data.submissions import adventure_log_processor, clog_processor, ca_processor, pb_processor, drop_processor, pet_processor
from api.services.metrics import MetricsTracker
from services.points import award_points_to_player
from utils.format import convert_to_ms, get_true_boss_name
from services.ticket_system import Tickets
from sqlalchemy.exc import OperationalError, DisconnectionError
from dotenv import load_dotenv
load_dotenv()

# Provide a no-op watchdog in dev to avoid systemd usage on Windows
class _DummyWatchdog:
    def set_health_check(self, fn):
        return None
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        return None
    async def notify_ready(self):
        return None

if os.getenv("STATUS") == "dev":
    SystemdWatchdog = _DummyWatchdog  # type: ignore
else:
    from monitor.sdnotifier import SystemdWatchdog
import time

target_guilds = os.getenv("TARGET_GUILDS").split(",")

if os.getenv("STATUS") == "dev":
    bot_token = os.getenv("DEV_WEBHOOK_TOKEN")
else:
    bot_token = os.getenv("WEBHOOK_TOKEN")

bot = interactions.Client(token=os.getenv("WEBHOOK_TOKEN"), intents=Intents.ALL)
metrics = MetricsTracker()
watchdog = None
shutdown_event = asyncio.Event()

# Health check function for systemd watchdog
async def health_check():
    """Comprehensive health check for the webhook bot"""
    try:
        # Check if bot is ready and connected
        if not bot.is_ready:
            return False
        
        # Check if metrics tracker is running
        if metrics is None:
            return False
        
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

@listen(MemberUpdate)
async def on_member_update(event: MemberUpdate):
    if os.getenv("PROCESS_NITRO_BOOSTS") == "true":
        try:
            local_session = Session()
            role_id = os.getenv("PRIMARY_GUILD_NITRO_ROLE_ID")
            if event.guild_id == os.getenv("DISCORD_GUILD_ID"):
                previously_boosting = False
                if event.before.roles != event.after.roles:
                    for role in event.before.roles:
                        if role.id == role_id:
                            previously_boosting = True
                    for role in event.after.roles:
                        if role.id == role_id:
                            if not previously_boosting:
                                ## This event contains the player's boost role update -- we need to apply points here
                                await award_nitro_boost(event.before.user.id)
        except Exception as e:
            print(f"Error processing member update: {e}")
        finally:
            local_session.close()

async def award_nitro_boost(user_id: int, session_to_use = None):
    if not session_to_use:
        local_session = Session()
    else:
        local_session = session_to_use
    user = local_session.query(User).filter(User.discord_id == user_id).first()
    user_players = local_session.query(Player).filter(Player.user_id == user.user_id).all()
    print(f"Awarding nitro boost to {user_players[0].player_name}...")
    award_points_to_player(player_id=user_players[0].player_id, amount=250, source='Nitro Boost Upgrade',expires_in_days=60,session=local_session)
    if not session_to_use:
        local_session.close()


# Add retry decorator for database operations
def retry_on_database_error(max_retries=3, delay=1):
    """Decorator to retry database operations on connection failures"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except (OperationalError, DisconnectionError) as e:
                    last_exception = e
                    if "server has gone away" in str(e).lower() or "connection reset" in str(e).lower():
                        print(f"Database connection lost on attempt {attempt + 1}, retrying in {delay}s...")
                        if attempt < max_retries - 1:  # Don't sleep on the last attempt
                            await asyncio.sleep(delay)
                        continue
                    else:
                        raise  # Re-raise if it's not a connection issue
                except Exception as e:
                    # For non-database errors, don't retry
                    raise
            
            # If we get here, all retries failed
            print(f"All {max_retries} database retry attempts failed")
            raise last_exception
        return wrapper
    return decorator



@retry_on_database_error(max_retries=3, delay=1)
async def process_submission_with_session(submission_type, embed_data):
    """Process a submission with a fresh database session"""
    session = Session()
    try:
        success = False
        if submission_type == "collection_log":
            result = await clog_processor(embed_data, external_session=session)
            success = True
        elif submission_type == "combat_achievement":
            result = await ca_processor(embed_data, external_session=session)
            success = True
        elif submission_type == "personal_best":
            result = await pb_processor(embed_data, external_session=session)
            success = True
        elif submission_type == "drop":
            result = await drop_processor(embed_data, external_session=session)
            success = True
        elif submission_type == "pet":
            result = await pet_processor(embed_data, external_session=session)
            success = True
        elif submission_type == "adventure_log":
            result = await adventure_log_processor(embed_data, external_session=session)
            success = True
        else:
            result = None
        
        # Commit the session if everything succeeded
        session.commit()
        try:
            metrics.record_request(submission_type, success, app="webhook_bot")
            #print(f"Recorded request: {submission_type} {success}")
        except Exception:
            #print(f"Error recording request: {submission_type} {success}")
            pass
        return result
        
    except Exception as e:
        # Rollback on any error
        session.rollback()
        #print(f"Error processing {submission_type}: {e}")
        try:
            metrics.record_request(submission_type, False, app="webhook_bot")
        except Exception:
            pass
        raise
    finally:
        # Always close the session
        session.close()

@interactions.listen(MessageCreate)
async def on_message_create(event: MessageCreate):
    def embed_to_dict(embed: Embed):
        if embed.fields:
            return {f.name: f.value for f in embed.fields}
        return {}
    
    bot: interactions.Client = event.bot
    if bot.is_closed:
        await bot.astart(token=os.getenv("WEBHOOK_TOKEN"))
    await bot.wait_until_ready()
    
    if isinstance(event, Message):
        message = event
    else:
        message = event.message
        
    if message.author.system:  # or message.author.bot:
        return
    if message.author.id == bot.user.id:
        return
    if message.channel.type == ChannelType.DM or message.channel.type == ChannelType.GROUP_DM:
        return
    channel_id = message.channel.id
                    
    if str(message.guild.id) in target_guilds or message.guild.id == os.getenv("DISCORD_GUILD_ID"):
        for embed in message.embeds:
            embed_data = embed_to_dict(embed)
            if message.attachments:
                for attachment in message.attachments:
                    if attachment.url:
                        embed_data['attachment_url'] = attachment.url
                        embed_data['attachment_type'] = attachment.content_type
                        
            field_names = [field.name for field in embed.fields]
            if embed_data:
                field_values = [field.value.lower().strip() for field in embed.fields]
                if "source_type" in field_names and "loot chest" in field_values:
                    ## Skip pvp
                    continue
                    
                embed_data['used_api'] = False
                
                try:
                    if "collection_log" in field_values:
                        await process_submission_with_session("collection_log", embed_data)
                        continue
                    elif "combat_achievement" in field_values:
                        await process_submission_with_session("combat_achievement", embed_data)
                        continue
                    elif "npc_kill" in field_values or "kill_time" in field_values:
                        await process_submission_with_session("personal_best", embed_data)
                        continue
                    elif embed.title and "received some drops" in embed.title or "drop" in field_values:
                        await process_submission_with_session("drop", embed_data)
                        continue
                    elif "experience_update" in field_values or "experience_milestone" in field_values or "level_up" in field_values:
                        # await experience_processor(embed_data)
                        continue
                    elif "quest_completion" in field_values:
                        # await quest_processor(embed_data)
                        continue
                    elif "pet" in field_values and "pet_name" in field_names:
                        await process_submission_with_session("pet", embed_data)
                        continue
                    elif "adventure_log" in field_values:
                        await process_submission_with_session("adventure_log", embed_data)
                        continue
                        
                except Exception as e:
                    print(f"Failed to process submission after retries: {e}")
                    # Continue processing other embeds even if one fails
    else:
        print(f"Message is not in the target guilds: {message.guild.id}")

        
@interactions.listen(Startup)
async def on_startup(event: Startup):
    
    # Load extensions first (they don't require database)
    try:
        bot.load_extension("services.ticket_system")
    except Exception as e:
        print(f"Error loading extensions: {e}")
    # Then handle database operations with proper session management
    player_count = 0
    local_session = Session()
    try:
        
        player_count = local_session.query(Player.player_id).count()
        await bot.change_presence(status=interactions.Status.ONLINE,
                            activity=interactions.Activity(name=f" ~{player_count} players", type=interactions.ActivityType.WATCHING))
    except (OperationalError, DisconnectionError) as e:
        await bot.change_presence(status=interactions.Status.ONLINE,
                            activity=interactions.Activity(name="DropTracker Bot", type=interactions.ActivityType.WATCHING))
    except Exception as e:
        print(f"Unexpected error during startup: {e}")
        await bot.change_presence(status=interactions.Status.ONLINE,
                            activity=interactions.Activity(name="DropTracker Bot", type=interactions.ActivityType.WATCHING))
    finally:
        local_session.close()
    
    

@interactions.listen(MessageReactionAdd)
async def on_message_reaction_add(event: MessageReactionAdd):
    if os.getenv("SHOULD_PROCESS_REACTIONS") == "false":
        return
    if event.message.id == os.getenv("DISCORD_MESSAGE_REACTION_ROLE_MESSAGE_ID"):
        if event.emoji.id == os.getenv("DISCORD_MESSAGE_REACTION_ROLE_EMOJI_ID"):
            emoji_user = event.author
            dt_guild = bot.get_guild(os.getenv("DISCORD_GUILD_ID"))
            member = dt_guild.get_member(member_id=emoji_user.id)
            if member:
                await member.add_role(role=os.getenv("DISCORD_MESSAGE_REACTION_ROLE_ROLE_ID"))
            return

async def main():
    """Main function with systemd watchdog integration"""
    global watchdog
    
    # Setup signal handlers
    setup_signal_handlers()
    
    # Initialize systemd watchdog
    watchdog = SystemdWatchdog()
    watchdog.set_health_check(health_check)
    
    try:
        async with watchdog:
            # Notify systemd that we're ready
            await watchdog.notify_ready()
            print("Systemd watchdog initialized and ready notification sent")
            
            # Start the bot
            bot_task = asyncio.create_task(bot.astart(token=os.getenv("WEBHOOK_TOKEN")))
            
            # Wait for either bot to complete or shutdown signal
            done, pending = await asyncio.wait(
                [bot_task, asyncio.create_task(shutdown_event.wait())],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # If shutdown was requested, cancel the bot task
            if shutdown_event.is_set():
                print("Shutdown requested, stopping bot...")
                if not bot_task.done():
                    bot_task.cancel()
                    try:
                        await bot_task
                    except asyncio.CancelledError:
                        pass
                
                # Bot will be closed automatically when the process exits
            
            print("Webhook bot shutting down gracefully...")
            
    except KeyboardInterrupt:
        print("Received keyboard interrupt")
    except Exception as e:
        print(f"Fatal error in main: {e}")
        raise
    finally:
        print("Webhook bot cleanup completed")

if __name__ == "__main__":
    asyncio.run(main())