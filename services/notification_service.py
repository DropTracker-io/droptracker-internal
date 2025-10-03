import asyncio
import json
import os
from datetime import datetime, timedelta
import interactions
from sqlalchemy import text
from db.models import ItemList, NotificationQueue, NpcList, PersonalBestEntry, User, UserConfiguration, get_current_partition, session, Player, Group, GroupConfiguration
from db.ops import DatabaseOperations, associate_player_ids, get_formatted_name
from db.xf.upgrades import check_active_upgrade
from utils.redis import redis_client
from utils.embeds import update_boss_pb_embed
from utils.messages import confirm_new_npc, confirm_new_item, name_change_message, new_player_message
from utils.format import format_number, replace_placeholders, convert_from_ms
from utils.download import download_player_image
from db.app_logger import AppLogger
from utils import osrs_api
from services.xf_services import get_user_id, create_alert
from utils.wiseoldman import fetch_group_members
from services.redis_updates import get_player_list_loot_sum, loot_tracker, get_player_current_month_total

app_logger = AppLogger()
global_footer = os.getenv('DISCORD_MESSAGE_FOOTER')
db = DatabaseOperations()


sent_drops = {}
sent_pbs = {}
sent_cas = {}
sent_clogs = {}

class NotificationService:
    def __init__(self, bot: interactions.Client, db_ops: DatabaseOperations):
        self.bot = bot
        self.db_ops = db_ops
        self.notified_users = []
        self.running = False
        self._processing_lock = asyncio.Lock()
    
    @interactions.Task.create(interactions.IntervalTrigger(seconds=5))
    async def start(self):
        """Start the notification service"""
        if self.running:
            return
            
        self.running = True
        asyncio.create_task(self.process_notifications_loop())
    
    async def stop(self):
        """Stop the notification service"""
        self.running = False
    
    
    async def process_notifications_loop(self):
        """Main loop to process notifications"""
        cleanup_counter = 0
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while self.running:
            try:
                async with self._processing_lock:
                    await self.process_pending_notifications()
                    
                    # Reset error counter on successful processing
                    consecutive_errors = 0
                    
                    # Clean up tracking dictionaries and stuck notifications every 100 iterations
                    cleanup_counter += 1
                    if cleanup_counter >= 100:
                        await self.cleanup_tracking_dicts()
                        await self.cleanup_stuck_notifications()
                        cleanup_counter = 0
                        
            except Exception as e:
                consecutive_errors += 1
                app_logger.log(log_type="error", data=f"Error processing notifications (attempt {consecutive_errors}): {e}", app_name="notification_service", description="process_notifications_loop")
                
                # If we have too many consecutive errors, increase sleep time
                if consecutive_errors >= max_consecutive_errors:
                    app_logger.log(log_type="warning", data=f"Too many consecutive errors ({consecutive_errors}), increasing sleep time", app_name="notification_service", description="process_notifications_loop")
                    await asyncio.sleep(30)  # Longer sleep on repeated errors
                    consecutive_errors = 0  # Reset after longer sleep
            finally:
                # Normal sleep time
                await asyncio.sleep(5)
    
    async def process_pending_notifications(self):
        """Process pending notifications with improved locking strategy"""
        try:
            # First, get a small batch of pending notifications without locking
            notifications = session.query(NotificationQueue).filter(
                NotificationQueue.status == 'pending'
            ).order_by(NotificationQueue.created_at.asc()).limit(5).all()
            
            og_length = len(notifications)
            if og_length == 0:
                #print("No pending notifications found, sleeping for 5 seconds")
                await asyncio.sleep(5)
                return
            for notification in notifications:
                try:
                    # Use a more targeted locking approach - lock only the specific row
                    locked_notification = session.query(NotificationQueue).filter(
                        NotificationQueue.id == notification.id,
                        NotificationQueue.status == 'pending'
                    ).with_for_update(skip_locked=True).first()
                    
                    # Skip if already locked by another process or no longer pending
                    if not locked_notification:
                        continue
                        
                    # Mark as processing immediately after acquiring lock
                    locked_notification.status = 'processing'
                    session.commit()
                    
                    # Process the notification
                    await self.process_notification(locked_notification)
                    
                except Exception as e:
                    # Ensure we have a valid notification object for error handling
                    if 'locked_notification' in locals() and locked_notification:
                        locked_notification.status = 'failed'
                        locked_notification.error_message = str(e)
                        session.commit()
                    app_logger.log(log_type="error", data=f"Error processing notification {notification.id}: {e}", app_name="notification_service", description="process_pending_notifications")
            
            
        except Exception as e:
            app_logger.log(log_type="error", data=f"Error in process_pending_notifications: {e}", app_name="notification_service", description="process_pending_notifications")

    async def process_notification(self, notification: NotificationQueue):
        """Process a single notification based on its type"""
        try:
            app_logger.log(log_type="info", data=f"Processing notification {notification.id} of type '{notification.notification_type}'", app_name="notification_service", description="process_notification")
            
            data = json.loads(notification.data)
            notification_type = notification.notification_type
            
            # Check for duplicates before processing
            if not await self._is_not_sent(notification, data):
                app_logger.log(log_type="info", data=f"Notification {notification.id} was already sent, skipping", app_name="notification_service", description="process_notification")
                return
            
            # Process the notification based on its type
            if notification_type == 'drop':
                await self.send_drop_notification(notification, data)
            elif notification_type == 'pb':
                await self.send_pb_notification(notification, data)
            elif notification_type == 'ca':
                await self.send_ca_notification(notification, data)
            elif notification_type == 'clog':
                await self.send_clog_notification(notification, data)
            elif notification_type == 'pet':
                await self.send_pet_notification(notification, data)
            elif notification_type == 'new_npc':
                await self.send_new_npc_notification(notification, data)
            elif notification_type == 'new_item':
                await self.send_new_item_notification(notification, data)
            elif notification_type == 'name_change':
                await self.send_name_change_notification(notification, data)
            elif notification_type == 'new_player':
                await self.send_new_player_notification(notification, data)
            elif notification_type == 'user_upgrade':
                await self.send_user_upgrade_notification(notification, data)
            elif notification_type == 'group_upgrade':
                await self.send_group_upgrade_notification(notification, data)
            elif notification_type == 'update_log':
                await self.send_update_log_data(notification, data)
            elif notification_type == 'points_earned':
                await self.send_points_notification(notification, data)
            else:
                notification.status = 'failed'
                notification.error_message = f"Unknown notification type: {notification_type}"
                print(f"Notification type not found: '{notification_type}'")
                session.commit()
        except Exception as e:
            app_logger.log(log_type="error", data=f"Error processing notification {notification.id}: {e}", app_name="notification_service", description="process_notification")
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise

    
    
    async def send_update_log_data(self, notification: NotificationQueue, data: dict):
        """Send a update log data notification to Discord"""
        notification.status = 'processing'
        session.commit()
        try:
            channel_id = 1210765287591256084
            channel = await self.bot.fetch_channel(channel_id=channel_id)
            if channel:
                updates = data.get('updates')
                updates = ["- " + update + "\n" for update in updates]
                text = f"### A new update log has been published:\n\n"
                text += f"".join(updates)
                await channel.send(text)
                notification.processed_at = datetime.now()
                session.commit()
            else:
                notification.status = 'failed'
                notification.error_message = f"Channel not found"
                session.commit()
                return
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            app_logger.log(log_type="error", data=f"Error sending update log data notification: {e}", app_name="notification_service", description="send_update_log_data")
            raise
    
    async def send_points_notification(self, notification: NotificationQueue, data: dict):
        """Send a points earned notification to Discord"""
        notification.status = 'processing'
        session.commit()
        try:
            scope_earned = data.get('scope_earned')
            if not scope_earned:
                return
            match scope_earned:
                case 'player':
                    user_id = data.get('user_id')
                    user = session.query(User).filter(User.user_id == user_id).first()
                case 'group':
                    group_id = data.get('group_id')
                    group = session.query(Group).filter(Group.group_id == group_id).first()
            if not user and not group:
                return
            if user:
                user_name = user.username
            else:
                user_name = group.group_name
            amount_earned = data.get('amount_earned')
            source = data.get('source')
            new_total = data.get('current_total')
            comment = data.get('comment')
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            app_logger.log(log_type="error", data=f"Error sending points earned notification: {e}", app_name="notification_service", description="send_points_notification")
            raise

    async def send_group_upgrade_notification(self, notification: NotificationQueue, data: dict):
        """Send a group upgrade notification to Discord"""
        notification.status = 'processing'
        session.commit()
        try:
            group_id = data.get('group_id')
            group = session.query(Group).filter(Group.group_id == group_id).first()
            user_id = data.get('dt_id')
            user = session.query(User).filter(User.user_id == user_id).first()
            status = data.get('status') 
            if user.players:
                players = [player for player in user.players]
                player_name = players[0].player_name
            else:
                player_name = None
            global_embed = None
            group_embed = None
            global_channel = None
            channel = None
            if group and user:
                bot: interactions.Client = self.bot
                guild_id = group.guild_id
                guild = await bot.fetch_guild(guild_id)
                if guild:
                    channel = guild.public_updates_channel
                global_channel = await bot.fetch_channel(1373331322709479485)
                if channel:
                    match status:
                        case 'added':
                            group_embed = interactions.Embed(
                                title=f"<:supporter:1263827303712948304> Your group has been upgraded!",
                                description=f"<@{user.discord_id}> has upgraded {group.group_name} to unlock premium features, such as customizable embeds!",
                                color="#00f0f0"
                            )
                            group_embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            group_embed.add_field(
                                name="Thank you for your support!",
                                value="Developing and maintaining a project like this takes lots of time and effort. We're extremely grateful for your continued support!"
                            )
                            group_embed.set_footer(global_footer)
                            global_embed = interactions.Embed(
                                title=f"<:supporter:1263827303712948304> `{user.username}` just upgraded {group.group_name}!",
                                description=f"{player_name if player_name else f'<@{user.discord_id}>'} just used their [account upgrade benefits](https://www.droptracker.io/account/upgrades) to unlock premium features for [{group.group_name}](https://www.droptracker.io/groups/{group.group_name}.{group.group_id}/view)",
                                color="#00f0f0"
                            )
                            global_embed.add_field(
                                name="Thank you for your support!",
                                value="Contributions like this keep us motivated to continue maintaining the project."
                            )
                            global_embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            global_embed.set_footer(global_footer)
                            global_guild = await bot.fetch_guild(1172737525069135962)
                            guild_member = await global_guild.fetch_member(user.discord_id)
                            if guild_member:
                                premium_role = global_guild.get_role(role_id=1210765189625151592)
                                await guild_member.add_role(role=premium_role)
                        case 'expired':
                            group_embed = interactions.Embed(
                                title=f"<:supporter:1263827303712948304> Your group has been downgraded!",
                                description=f"Your group upgrade has now expired.",
                                color="#f00000"
                            )
                            group_embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            group_embed.add_field(
                                name="Thank you for your support!",
                                value="Developing and maintaining a project like this takes lots of time and effort. We're extremely grateful for any support you provided."
                            )
                            group_embed.set_footer(global_footer)
                            global_guild = await bot.fetch_guild(1172737525069135962)
                            guild_member = await global_guild.fetch_member(user.discord_id)
                            if guild_member:
                                premium_role = global_guild.get_role(role_id=1210765189625151592)
                                if premium_role in guild_member.roles:
                                    await guild_member.remove_role(role=premium_role)
                    if channel and group_embed:
                        try:
                            await channel.send(embed=group_embed)
                            notification.status = 'sent'
                            notification.processed_at = datetime.now()
                        except Exception as e:
                            if group.configurations:
                                for config in group.configurations:
                                    if config.config_key == 'authed_users':
                                        authed_users = config.config_value
                                        authed_users = authed_users.replace('[','').replace(']','').replace('"','').replace(' ', '').split(',')
                                        for user_id in authed_users:
                                            user_id = int(user_id)
                                            try:
                                                authed_user = await bot.fetch_user(user_id)
                                                if authed_user:
                                                    if status == 'expired':
                                                        group_embed.add_field(
                                                        name=f"Original Supporter:",
                                                        value=f"<@{user.discord_id}>",
                                                        inline=False
                                                    )
                                                    if user_id in self.notified_users:
                                                        ## Don't notify the same user twice in quick succession.
                                                        return
                                                    self.notified_users.append(user_id)
                                                    await authed_user.send(embed=group_embed)
                                                    await asyncio.sleep(0.2)
                                            except Exception as e:
                                                app_logger.log(log_type="error", data=f"Error sending group embed to authed user {user_id}: {e}", app_name="notification_service", description="send_group_upgrade_notification")
                            app_logger.log(log_type="error", data=f"Error sending group embed: {e}", app_name="notification_service", description="send_group_upgrade_notification")
                    else:
                        app_logger.log(log_type="error", data=f"Channel or group embed not found", app_name="notification_service", description="send_group_upgrade_notification")
                    if global_channel and global_embed:
                        try:
                            await global_channel.send(embed=global_embed)
                            notification.status = 'sent'
                            notification.processed_at = datetime.now()
                            session.commit()
                        except Exception as e:
                            app_logger.log(log_type="error", data=f"Error sending global embed: {e}", app_name="notification_service", description="send_group_upgrade_notification")
                    else:
                        app_logger.log(log_type="error", data=f"Global channel or global embed not found", app_name="notification_service", description="send_group_upgrade_notification")
                    
                    session.commit()
                else:
                    notification.status = 'failed'
                    notification.error_message = f"Channel not found"
                    session.commit()
            else:
                notification.status = 'failed'
                notification.error_message = f"Group not found"
                session.commit()
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise

    async def send_user_upgrade_notification(self, notification: NotificationQueue, data: dict):
        """Send a user upgrade notification to Discord"""
        notification.status = 'processing'
        session.commit()
        try:
            user_id = data.get('dt_id')
            status = data.get('status')
            db_user = session.query(User).filter(User.user_id == user_id).first()
            if user_id in self.notified_users:
                ## Don't notify the same user twice in quick succession.
                return
            self.notified_users.append(user_id)
            if db_user:
                bot: interactions.Client = self.bot
                user = await bot.fetch_user(db_user.discord_id)
                if user:
                    match status:
                        case 'added':
                            embed = interactions.Embed(
                                title="<a:droptracker:1346787143778963497> Thank you for your support!",
                                description=f"Your account upgrade has been successfully processed.",
                                color="#00f0f0"
                            )
                            embed.add_field(
                                name="What's next?",
                                value="You can now [select a group](https://www.droptracker.io/account/premium)" + 
                                " to use your premium features on.\n\n" + 
                                "If you have any questions, [feel free to reach out in our Discord](https://www.droptracker.io/discord)"
                            )
                            embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            embed.set_footer(global_footer)
                            await user.send(embed=embed)
                            notification.status = 'sent'
                            notification.processed_at = datetime.now()
                            session.commit()
                            return
                        case 'expired':
                            embed = interactions.Embed(
                                title="We're sorry to see you go!",
                                description=f"Your account upgrade has expired.\n" +
                                "Please consider [re-upgrading your account](https://www.droptracker.io/account/upgrades) to continue supporting the project," + 
                                " and to retain access to your group's premium features.",
                                color="#f00000"
                            )
                            embed.set_thumbnail("https://www.droptracker.io/img/droptracker-small.gif")
                            
                            embed.set_footer(global_footer)
                            await user.send(embed=embed)
                            notification.status = 'sent'
                            notification.processed_at = datetime.now()
                            session.commit()
                            return
            else:
                notification.status = 'failed'
                notification.error_message = f"User not found"
                session.commit()
                return
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            app_logger.log(log_type="error", data=f"Error sending user upgrade notification: {e}", app_name="notification_service", description="send_user_upgrade_notification")
            raise
                
    async def send_drop_notification(self, notification: NotificationQueue, data: dict):
        """Send a drop notification to Discord"""
        from db.models import NotifiedSubmission
        try:
            group_id = notification.group_id
            player_id = notification.player_id
            #print(f"Got raw drop notification data: {data}")
            drop_id = data.get('drop_id')


            
            # Get channel ID for this group
            channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_loot'
            ).first()

            existing_notification = session.query(NotifiedSubmission).filter(
                NotifiedSubmission.player_id == player_id,
                NotifiedSubmission.group_id == group_id,
                NotifiedSubmission.drop_id == drop_id
            ).first()
            if existing_notification:
                print(f"Drop was already notified... Skipping")
                return
            
            if not channel_id_config:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                session.commit()
                return
            
            channel_id = channel_id_config.config_value
            if channel_id != "":
                channel = await self.bot.fetch_channel(channel_id=channel_id)
            else:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                session.commit()
                return
            
            # Get player name
            player_name = data.get('player_name')
            item_name = data.get('item_name')
            kill_count = data.get('kill_count', None)
            item_id = session.query(ItemList).filter(ItemList.item_name == item_name).first()
            if item_id:
                item_id = item_id.item_id
            else:
                item_id = 1
            npc_name = data.get('npc_name', None)
            if npc_name:
                npc_id = session.query(NpcList).filter(NpcList.npc_name == npc_name).first()
            else:
                npc_id = 0
            if npc_id:
                npc_id = npc_id.npc_id
            else:
                npc_id = 1
            value = data.get('value')
            quantity = data.get('quantity')
            total_value = data.get('total_value')
            image_url = data.get('image_url', None)
            if image_url is None or image_url == "":
                try:
                    drop = session.query(Drop).filter(Drop.drop_id == data.get('drop_id')).first()
                    if drop:
                        image_url = drop.image_url
                except Exception as e:
                    image_url = None
            #print(f"Debug - image_url: {image_url}, type: {type(image_url)}")
            if not image_url or "droptracker.io" not in image_url:
                image_url = ""
            
            # Get embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('drop', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('drop', 1)
            #print(f"Debug - embed_template: {embed_template}")
            
            if not embed_template:
                notification.status = 'failed'
                notification.error_message = f"No embed template for group {group_id}"
                session.commit()
                return
            
            # Download image if available
            attachment = None
            if image_url:
                try:
                    # Convert external URL to local path matching the actual storage structure
                    local_url = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                    if os.path.exists(local_url):
                        attachment = interactions.File(local_url)
                    else:
                        print(f"Debug - Image file not found at: {local_url}")
                        attachment = None
                except Exception as e:
                    print(f"Debug - Couldn't get attachment from path: {e}")
                    attachment = None
                    pass
            
            # Replace placeholders in embed
            player = None
            if not player_id:
                player = session.query(Player).filter(Player.player_name == player_name).first()
                if player:
                    player_id = player.player_id
            
            partition = get_current_partition()
            # Use monthly total computed by redis_updates player cache
            month_total_int = self._get_player_month_total(player_id, partition)
            player_month_total = format_number(month_total_int)
            players_in_group = session.query(Player.player_id).join(Player.groups).filter(Group.group_id == group_id).all()
            group_month_total = format_number(get_player_list_loot_sum([player.player_id for player in players_in_group]))
            # Use centralized rank helper for accuracy
            global_rank_data = loot_tracker.get_player_rank(player_id, None, partition)
            group_rank_data = loot_tracker.get_player_rank(player_id, group_id, partition)
            #print(f"Got group rank data: {group_rank_data}")
            print(f"Got global rank data: {global_rank_data}")
            if group_rank_data:
                group_rank, user_count = group_rank_data
            else:
                group_rank, user_count = None, redis_client.client.zcard(f"leaderboard:{partition}:group:{group_id}")
            if global_rank_data:
                global_rank, total_global_players = global_rank_data
            else:
                global_rank, total_global_players = None, redis_client.client.zcard(f"leaderboard:{partition}")
            # get all group ranks
            ## TODO -- remove this if not in dev instance!!
            #all_groups = session.query(Group.group_id).filter(Group.group_id != 2).all()
            all_groups = session.query(Group.group_id).all()
            total_groups = len(all_groups) - 1
            group_totals = []
            for group in all_groups:
                group_total = redis_client.zsum(f"leaderboard:{partition}:group:{group.group_id}")
                group_totals.append({'id': group.group_id,
                                   'total': group_total})
            sorted_groups = sorted(group_totals, key=lambda x: x['total'], reverse=True)
            group_to_group_rank = str(next((i for i, g in enumerate(sorted_groups) if g['id'] == group_id), 0) + 1)
            formatted_name = get_formatted_name(player_name, group_id, session)
            # Build rank strings safely
            if global_rank is not None and total_global_players is not None:
                global_rank_str = "`" + str(global_rank) + "`" + "/" + "`" + str(total_global_players) + "`"
            else:
                global_rank_str = "`?`"
            if group_rank is not None and user_count is not None:
                group_rank_str = "`" + str(group_rank) + "`" + "/" + "`" + str(user_count) + "`"
            else:
                group_rank_str = "`?`"
            values = {
                "{item_name}": item_name,
                "{month_name}": datetime.now().strftime("%B"),
                "{player_total_month}": "`" + player_month_total + "`",
                "{global_rank}": global_rank_str,
                "{group_rank}": group_rank_str,
                "{group_total}": "`" + str(group_total) + "`",
                "{user_count}": "`" + str(user_count) + "`",
                "{group_total_month}": "`" + group_month_total + "`",
                "{group_to_group_rank}": "`" + str(group_to_group_rank) + "`" + "/" + "`" + str(total_groups) + "`",
                "{item_id}": str(item_id),
                "{npc_id}": str(npc_id),
                "{npc_name}": npc_name,
                "{kill_count}": str(kill_count),
                "{item_value}": "`" + format_number(total_value) + "`",
                "{quantity}": "`" + str(quantity) + "`",
                "{total_value}": "`" + str(total_value) + "`",
                "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{player_name}.{player_id}/view)",
                "{image_url}": image_url or ""
            }
            #print("Sending to replace_placeholders")
            
            embed = replace_placeholders(embed_template, values)
            if group_id == 2:
                embed = await self.remove_group_field(embed)
            if kill_count is None or int(kill_count) < 1:
                embed = await self.remove_kc_field(embed)
            image_url = data.get('image_url', None)
            if image_url and "cdn.discordapp.com" in image_url:
                try:
                    drop = session.query(Drop).filter(Drop.drop_id == data.get('drop_id')).first()
                    if drop:
                        image_url = drop.image_url
                except Exception as e:
                    image_url = None
            if image_url:
                try:
                    local_url = image_url.replace("https://www.droptracker.io/", "/store/droptracker/disc/static/assets/")
                    attachment = interactions.File(local_url)
                except Exception as e:
                    print(f"Debug - Couldn't get attachment from path: {e}")
                    attachment = None
                    pass
            #print("Got the embed...")
            # Send message
            if attachment:
                message = await channel.send(f"{formatted_name} received a drop:", embed=embed, files=attachment)
            else:
                message = await channel.send(f"{formatted_name} received a drop:", embed=embed)
            
            # Mark as sent
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            
            # Create NotifiedSubmission entry
            from db.models import Drop
            drop = session.query(Drop).filter(Drop.drop_id == data.get('drop_id')).first()
            
            if drop and message:
                notified_sub = NotifiedSubmission(
                    channel_id=str(message.channel.id),
                    player_id=player_id,
                    message_id=str(message.id),
                    group_id=group_id,
                    status="sent",
                    drop=drop
                )
                session.add(notified_sub)
            
            session.commit()
            player_notif_data = data.copy()
            player_notif_data['item_name'] = item_name
            #await self.send_player_notification(player_id, player_name, player_notif_data, 'drop', data.get('drop_id'))
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
    
    async def send_new_npc_notification(self, notification: NotificationQueue, data: dict):
        """Send notification about new NPC"""
        try:
            npc_name = data.get('npc_name')
            player_name = data.get('player_name')
            item_name = data.get('item_name')
            value = data.get('value')
            
            await confirm_new_npc(self.bot, npc_name, player_name, item_name, value)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
    
    async def send_new_item_notification(self, notification: NotificationQueue, data: dict):
        """Send notification about new item"""
        try:
            item_name = data.get('item_name')
            player_name = data.get('player_name')
            item_id = data.get('item_id')
            npc_name = data.get('npc_name')
            value = data.get('value')
            
            await confirm_new_item(self.bot, item_name, player_name, item_id, npc_name, value)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
    
    async def send_name_change_notification(self, notification: NotificationQueue, data: dict):
        """Send notification about player name change"""
        try:
            player_name = data.get('player_name')
            player_id = data.get('player_id')
            old_name = data.get('old_name')
            
            await name_change_message(self.bot, player_name, player_id, old_name)
            
            # Also send DM to user if they have Discord ID
            player = session.query(Player).filter(Player.player_id == player_id).first()
            if player and player.user:
                user_discord_id = player.user.discord_id
                if user_discord_id:
                    try:
                        user = await self.bot.fetch_user(user_id=user_discord_id)
                        if user:
                            embed = interactions.Embed(
                                title=f"Name change detected:",
                                description=f"Your account, {old_name}, has changed names to {player_name}.",
                                color="#00f0f0"
                            )
                            embed.add_field(
                                name=f"Is this a mistake?",
                                value=f"Reach out in [our discord](https://www.droptracker.io/discord)"
                            )
                            embed.set_footer(global_footer)
                            await user.send(f"Hey, <@{user.discord_id}>", embed=embed)
                    except Exception as e:
                        app_logger.log(log_type="error", data=f"Couldn't DM user about name change: {e}", app_name="notification_service", description="send_name_change_notification")
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
    
    async def send_new_player_notification(self, notification: NotificationQueue, data: dict):
        """Send notification about new player"""
        try:
            player_name = data.get('player_name')
            
            await new_player_message(self.bot, player_name)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
    
    async def send_pb_notification(self, notification: NotificationQueue, data: dict):
        """Send a personal best notification to Discord"""
        from db.models import NotifiedSubmission
        try:
            group_id = notification.group_id
            player_id = notification.player_id
            
            # Get channel ID for this group
            channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_pb'
            ).first()
            pb_id = data.get('pb_id', None)
            if pb_id:
                existing_notification = session.query(NotifiedSubmission).filter(
                    NotifiedSubmission.player_id == player_id,
                    NotifiedSubmission.group_id == group_id,
                    NotifiedSubmission.pb_id == pb_id
                ).first()
                if existing_notification:
                    print(f"PB was already notified... Skipping")
                    return
            
            
            if not channel_id_config:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                session.commit()
                return
            
            channel_id = channel_id_config.config_value
            if channel_id != "":
                channel = await self.bot.fetch_channel(channel_id=channel_id)
            else:
                channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_loot'
                ).first()
                if channel_id_config:
                    channel_id = channel_id_config.config_value
                    channel = await self.bot.fetch_channel(channel_id=channel_id)
                else:
                    notification.status = 'failed'
                    notification.error_message = f"No channel configured for group {group_id}"
                    session.commit()
                    return
            # hall_of_fame = self.bot.get_ext("services.hall_of_fame")
            # if hall_of_fame:
            #     try:
            #         await hall_of_fame.update_boss_component(group_id, npc_id)
            #     except Exception as e:
            #         print(f"Error updating boss component: {e}")
            #         pass
            # Get data
            player_name = data.get('player_name')
            boss_name = data.get('boss_name')
            time_ms = data.get('time_ms')
            old_time_ms = data.get('old_time_ms')
            kill_time_ms = data.get('kill_time_ms')
            image_url = data.get('image_url')
            team_size = data.get('team_size')
            npc_id = data.get('npc_id')
            # Format times
            time_formatted = convert_from_ms(time_ms)
            old_time_formatted = convert_from_ms(old_time_ms) if old_time_ms else None
            
            # Get embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('pb', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('pb', 1)
            if group_id == 2:
                embed_template = await self.remove_group_field(embed_template)
            
            #print(f"Debug - embed_template: {embed_template}")
            partition = get_current_partition()
            player_total_raw = redis_client.client.zscore(f"leaderboard:{partition}", player_id)
            group_wom_id = session.query(Group.wom_id).filter(Group.group_id == group_id).first()
            if group_wom_id:
                group_wom_id = group_wom_id[0]
            wom_member_list = []
            if group_wom_id:
                #print("Finding group members?")
                try:
                    wom_member_list = await fetch_group_members(wom_group_id=int(group_wom_id))
                except Exception as e:
                    #print("Couldn't get the member list", e)
                    return
            player_ids = await associate_player_ids(wom_member_list)
            
            group_ranks = session.query(PersonalBestEntry).filter(PersonalBestEntry.player_id.in_(player_ids), PersonalBestEntry.npc_id == int(npc_id),
                                                                        PersonalBestEntry.team_size == team_size).order_by(PersonalBestEntry.personal_best.asc()).all()
            all_ranks = session.query(PersonalBestEntry).filter(PersonalBestEntry.npc_id == int(npc_id),
                                                                    PersonalBestEntry.team_size == team_size).order_by(PersonalBestEntry.personal_best.asc()).all()
                #print("Group ranks:",group_ranks)
                #print("All ranks:",all_ranks)
            total_ranked_group = len(group_ranks)
            total_ranked_global = len(all_ranks)
            current_user_best_ms = time_ms
                ## player's rank in group
            group_placement = None
            global_placement = None
            #print("Assembling rankings....")
            ## For some reason, players occassionally don't appear in group rank listings...
            if str(player_id) not in [str(entry.player_id) for entry in group_ranks]:
                # Find where this time would be inserted in the sorted list
                group_placement = len(group_ranks) + 1  # Default to last place (worst time)
                for idx, entry in enumerate(group_ranks, start=1):
                    if current_user_best_ms <= entry.personal_best:
                        # Current user's time is faster or equal, so they rank at this position
                        group_placement = idx
                        break
            else:
                for idx, entry in enumerate(group_ranks, start=1): 
                    if entry.personal_best == current_user_best_ms:
                        group_placement = idx
                        break
            ## player's rank globally
            global_placement = len(all_ranks) + 1  # Default to last place (worst time)
            for idx, entry in enumerate(all_ranks, start=1):
                if current_user_best_ms <= entry.personal_best:
                    global_placement = idx
                    break
            if group_placement is None:
                group_placement = "`?`"
                # Replace placeholders
            formatted_name = get_formatted_name(player_name, group_id, session)
            
            replacements = {
                "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{player_name}.{player_id}/view)",
                "{global_rank}": str(global_placement),
                "{total_ranked_global}": str(total_ranked_global),
                "{group_rank}": str(group_placement),
                "{total_ranked_group}": str(total_ranked_group),
                "{npc_name}": boss_name,
                "{npc_id}": str(npc_id),
                "{team_size}": team_size,
                "{personal_best}": time_formatted,
            }
            
            embed = replace_placeholders(embed_template, replacements)
            
            # Send message
            if image_url:
                try:
                    local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                    if os.path.exists(local_path):
                        attachment = interactions.File(local_path)
                        message = await channel.send(f"{formatted_name} has achieved a new personal best:", embed=embed, files=attachment)
                    else:
                        #print(f"Debug - PB image file not found at: {local_path}")
                        message = await channel.send(f"{formatted_name} has achieved a new personal best:", embed=embed)
                except Exception as e:
                    #print(f"Debug - Error loading PB attachment: {e}")
                    message = await channel.send(f"{formatted_name} has achieved a new personal best:", embed=embed)
            else:
                message = await channel.send(f"{formatted_name} has achieved a new personal best:", embed=embed)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise

    async def send_pet_notification(self, notification: NotificationQueue, data: dict):
        """Send a pet notification to Discord"""
        from db.models import NotifiedSubmission
        group_id = notification.group_id
        player_id = notification.player_id
        print("Got pet data:", data)
        # notification_data = {
        #         'player_name': player_name,
        #         'player_id': player_id,
        #         'pet_name': pet_name,
        #         'source': source,
        #         'npc_name': npc_name,
        #         'killcount': killcount,
        #         'milestone': milestone,
        #         'duplicate': duplicate,
        #         'previously_owned': previously_owned,
        #         'game_message': game_message,
        #         'image_url': dl_path,
        #         'item_id': pet_item_id,
        #         'npc_id': npc_id,
        #         'is_new_pet': is_new_pet
        #     }
        pet_name = data.get('pet_name')
        source = data.get('source')
        npc_name = data.get('npc_name')
        killcount = data.get('killcount')
        milestone = data.get('milestone')
        duplicate = data.get('duplicate')
        previously_owned = data.get('previously_owned')
        game_message = data.get('game_message')
        image_url = data.get('image_url')
        item_id = data.get('item_id')
        npc_id = data.get('npc_id')
        is_new_pet = data.get('is_new_pet')
        group_id = data.get('group_id')
        player_name = data.get('player_name')
        update_active = check_active_upgrade(group_id)
        if update_active:
            embed_template = await self.db_ops.get_group_embed('pet', group_id)
        else:
            embed_template = await self.db_ops.get_group_embed('pet', 1)
        
        if not embed_template:
            notification.status = 'failed'
            notification.error_message = f"No embed template for group {group_id}"
            session.commit()
            return
        
        
        channel_id_config = session.query(GroupConfiguration).filter(
            GroupConfiguration.group_id == group_id,
            GroupConfiguration.config_key == 'channel_id_to_post_pets'
        ).first()
        
        
        if not channel_id_config:
            notification.status = 'failed'
            notification.error_message = f"No channel configured for group {group_id}"
            session.commit()
            return
        kc_received = milestone if milestone else killcount
        
        value_dict = {
            "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{player_name}.{player_id}/view)",
            "{pet_name}": pet_name,
            "{source}": source,
            "{npc_name}": npc_name,
            "{killcount}": kc_received, 
            "{milestone}": kc_received,
            "{duplicate}": duplicate,
            "{previously_owned}": previously_owned
        }
        try:
            channel = await self.bot.fetch_channel(channel_id=channel_id_config.config_value)
            formatted_name = get_formatted_name(player_name, group_id, session)
            if channel:
                embed = replace_placeholders(embed_template, value_dict)
                if group_id == 2:
                    embed = await self.remove_group_field(embed)
                
                if image_url:
                    try:
                        local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                        if os.path.exists(local_path):
                            attachment = interactions.File(local_path)
                            message = await channel.send(f"{formatted_name} has acquired a new pet!", embed=embed, files=attachment)
                    except Exception as e:
                        message = await channel.send(f"{formatted_name} has acquired a new pet!", embed=embed)
                else:
                    message = await channel.send(f"{formatted_name} has acquired a new pet!", embed=embed)
                
                notification.status = 'sent'
                notification.processed_at = datetime.now()
                session.commit()
                return
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = f"Failed to send pet notification: {e}"
            session.commit()
            return

    async def send_ca_notification(self, notification: NotificationQueue, data: dict):
        """Send a combat achievement notification to Discord"""
        from db.models import NotifiedSubmission
        try:
            group_id = notification.group_id
            player_id = notification.player_id
            #print("Got raw CA data:", data)
            
            # Get channel ID for this group
            channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_ca'
            ).first()
            
            if not channel_id_config:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                session.commit()
                return
            
            ca_id = data.get('ca_id', None)
            if ca_id:
                existing_notification = session.query(NotifiedSubmission).filter(
                    NotifiedSubmission.player_id == player_id,
                    NotifiedSubmission.group_id == group_id,
                    NotifiedSubmission.ca_id == ca_id
                ).first()
                if existing_notification:
                    print(f"CA was already notified... Skipping")
                    return
            
            channel_id = channel_id_config.config_value
            if channel_id != "":
                channel = await self.bot.fetch_channel(channel_id=channel_id)
            else:
                channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_loot'
                ).first()
                if channel_id_config:
                    channel_id = channel_id_config.config_value
                    channel = await self.bot.fetch_channel(channel_id=channel_id)
                else:
                    notification.status = 'failed'
                    notification.error_message = f"No channel configured for group {group_id}"
                    session.commit()
            # Get data
            player_name = data.get('player_name')
            task_name = data.get('task_name')
            task_tier = data.get('tier')
            image_url = data.get('image_url')
            points_awarded = data.get('points_awarded')
            points_total = data.get('points_total')
            
            # Map tier to color and name
            tier_colors = {
                "1": 0x00ff00,  # Easy - Green
                "2": 0x0000ff,  # Medium - Blue
                "3": 0xff0000,  # Hard - Red
                "4": 0xffff00,  # Elite - Yellow
                "5": 0xff00ff,  # Master - Purple
                "6": 0x00ffff   # Grandmaster - Cyan
            }
            
            tier_names = {
                "1": "Easy",
                "2": "Medium",
                "3": "Hard",
                "4": "Elite",
                "5": "Master",
                "6": "Grandmaster"
            }
            
            # Get embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('ca', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('ca', 1)
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
        
    async def send_ca_notification(self, notification: NotificationQueue, data: dict):
        """Send a combat achievement notification to Discord"""
        from db.models import NotifiedSubmission
        try:
            group_id = notification.group_id
            player_id = notification.player_id
            #print("Got raw CA data:", data)
            
            # Get channel ID for this group
            channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_ca'
            ).first()
            
            if not channel_id_config:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                session.commit()
                return
            
            ca_id = data.get('ca_id', None)
            if ca_id:
                existing_notification = session.query(NotifiedSubmission).filter(
                    NotifiedSubmission.player_id == player_id,
                    NotifiedSubmission.group_id == group_id,
                    NotifiedSubmission.ca_id == ca_id
                ).first()
                if existing_notification:
                    print(f"CA was already notified... Skipping")
                    return
            
            channel_id = channel_id_config.config_value
            if channel_id != "":
                channel = await self.bot.fetch_channel(channel_id=channel_id)
            else:
                channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_loot'
                ).first()
                if channel_id_config:
                    channel_id = channel_id_config.config_value
                    channel = await self.bot.fetch_channel(channel_id=channel_id)
                else:
                    notification.status = 'failed'
                    notification.error_message = f"No channel configured for group {group_id}"
                    session.commit()
            # Get data
            player_name = data.get('player_name')
            task_name = data.get('task_name')
            task_tier = data.get('tier')
            image_url = data.get('image_url')
            points_awarded = data.get('points_awarded')
            points_total = data.get('points_total')
            
            # Map tier to color and name
            tier_colors = {
                "1": 0x00ff00,  # Easy - Green
                "2": 0x0000ff,  # Medium - Blue
                "3": 0xff0000,  # Hard - Red
                "4": 0xffff00,  # Elite - Yellow
                "5": 0xff00ff,  # Master - Purple
                "6": 0x00ffff   # Grandmaster - Cyan
            }
            
            tier_names = {
                "1": "Easy",
                "2": "Medium",
                "3": "Hard",
                "4": "Elite",
                "5": "Master",
                "6": "Grandmaster"
            }
            
            # Get embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('ca', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('ca', 1)
            
            async with osrs_api.create_client() as client:
                actual_tier = await client.semantic.get_current_ca_tier(points_total)
            #actual_tier = await get_current_ca_tier(points_total)
            tier_order = ['Grandmaster', 'Master', 'Elite', 'Hard', 'Medium', 'Easy']
            if actual_tier is None:
                next_tier = "Easy"
            else:
                next_tier = tier_order[tier_order.index(actual_tier) - 1]
            async with osrs_api.create_client() as client:
                progress, next_tier_points = await client.semantic.get_ca_tier_progress(points_total)
            #progress, next_tier_points = await get_ca_tier_progress(points_total)
            formatted_task_name = task_name.replace(" ", "_").replace("?", "%3F")
            wiki_url = f"https://oldschool.runescape.wiki/w/{formatted_task_name}"
            formatted_task_name = f"[{task_name}]({wiki_url})"
            try:
                if not next_tier_points or next_tier_points == 0:
                    next_tier_points = 38
                points_left = int(next_tier_points) - int(points_total)
            except Exception as e:
                points_left = "Unknown"
            if embed_template:
                value_dict = {
                    "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{player_name}.{player_id}/view)",
                    "{task_name}": formatted_task_name,
                    "{current_tier}": actual_tier,
                    "{progress}": progress,
                    "{points_awarded}": points_awarded,
                    "{total_points}": points_total,
                    "{next_tier}": next_tier,
                    "{task_tier}": task_tier,
                    "{next_tier_points}": next_tier_points,
                    "{points_left}": points_left
                }
            
            embed = replace_placeholders(embed_template, value_dict)
            
            # Send message
            formatted_name = get_formatted_name(player_name, group_id, session)
            
            if image_url:
                try:
                    local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                    if os.path.exists(local_path):
                        attachment = interactions.File(local_path)
                        message = await channel.send(f"{formatted_name} has completed a combat achievement!", embed=embed, files=attachment)
                    else:
                        #print(f"Debug - CA image file not found at: {local_path}")
                        message = await channel.send(f"{formatted_name} has completed a combat achievement!", embed=embed)
                except Exception as e:
                    #print(f"Debug - Error loading CA attachment: {e}")
                    message = await channel.send(f"{formatted_name} has completed a combat achievement!", embed=embed)
            else:
                message = await channel.send(f"{formatted_name} has completed a combat achievement!", embed=embed)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
    
    async def send_clog_notification(self, notification: NotificationQueue, data: dict):
        """Send a collection log notification to Discord"""
        from db.models import NotifiedSubmission
        try:
            group_id = notification.group_id
            player_id = notification.player_id
            #print(f"Found a collection log notification to send in {group_id}")
            
            # Get channel ID for this group
            channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_clog'
            ).first()
            #print(f"Found a channel id config for {group_id}")
            if not channel_id_config or not channel_id_config.config_value:
                notification.status = 'failed'
                notification.error_message = f"No channel configured for group {group_id}"
                session.commit()
                return
            
            clog_id = data.get('clog_id', data.get('log_id', None))
            if clog_id:
        
                existing_notification = session.query(NotifiedSubmission).filter(
                    NotifiedSubmission.player_id == player_id,
                    NotifiedSubmission.group_id == group_id,
                    NotifiedSubmission.clog_id == clog_id
                ).first()
                if existing_notification:
                    print(f"Drop was already notified... Skipping")
                    return
            channel = None
            channel_id = channel_id_config.config_value
            if channel_id and channel_id != "" and len(str(channel_id)) > 10:
                channel = await self.bot.fetch_channel(channel_id=channel_id)
            else:
                channel_id_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'channel_id_to_post_loot'
                ).first()
                if channel_id_config:
                    channel_id = channel_id_config.config_value
                    channel = await self.bot.fetch_channel(channel_id=channel_id)
                else:
                    #print(f"Invalid channel id: {channel_id}")
                    notification.status = 'failed'
                    notification.error_message = f"Invalid channel id: {channel_id}"
                    session.commit()
                    return
            if not channel:
                #print(f"Channel not found for group {group_id} (id was passed as {channel_id})")
                notification.status = 'failed'
                notification.error_message = f"Channel not found for group {group_id}"
                session.commit()
                return
            # Get data
            player_name = data.get('player_name')
            item_name = data.get('item_name')
            collection_name = data.get('collection_name')
            image_url = data.get('image_url')
            item_id = data.get('item_id')
            kc = data.get('kc_received')
            npc_name = data.get('npc_name')
            partition = get_current_partition()
            month_total_int = self._get_player_month_total(player_id, partition)
            player_month_total = format_number(month_total_int)
            
            # Get embed template
            upgrade_active = check_active_upgrade(group_id)
            if upgrade_active:
                embed_template = await self.db_ops.get_group_embed('clog', group_id)
            else:
                embed_template = await self.db_ops.get_group_embed('clog', 1)
            
            if group_id == 2:
                embed_template = await self.remove_group_field(embed_template)

            user_count = format_number(redis_client.client.zcard(f"leaderboard:{partition}:group:{group_id}"))
            # Replace placeholders
            replacements = {
                "{player_name}": f"[{player_name}](https://www.droptracker.io/players/{player_name}.{player_id}/view)",
                "{player_loot_month}": player_month_total,
                "{kc_received}": kc,
                "{item_name}": item_name,
                "{collection_name}": collection_name,
                "{item_id}": item_id,
                "{npc_name}": npc_name,
                "{total_tracked}": user_count
            }
            
            embed = replace_placeholders(embed_template, replacements)
            
            # Send message
            formatted_name = get_formatted_name(player_name, group_id, session)
            
            if image_url:
                try:
                    local_path = image_url.replace("https://www.droptracker.io/img/", "/store/droptracker/disc/static/assets/img/")
                    if os.path.exists(local_path):
                        attachment = interactions.File(local_path)
                        message = await channel.send(f"{formatted_name} has added an item to their collection log:", embed=embed, files=attachment)
                    else:
                        print(f"Debug - Collection log image file not found at: {local_path}")
                        message = await channel.send(f"{formatted_name} has added an item to their collection log!", embed=embed)
                except Exception as e:
                    print(f"Debug - Error loading collection log attachment: {e}")
                    message = await channel.send(f"{formatted_name} has added an item to their collection log!", embed=embed)
            else:
                message = await channel.send(f"{formatted_name} has added an item to their collection log!", embed=embed)
            
            notification.status = 'sent'
            notification.processed_at = datetime.now()
            session.commit()
            
        except Exception as e:
            notification.status = 'failed'
            notification.error_message = str(e)
            session.commit()
            raise
    
    async def send_player_notification(self, player_id: int, player_name: str, data: dict, notif_type: str, notif_id: int):
        """Creates an Alert on the website for a player's submission"""
        try:
            xf_user_id = await get_user_id(player_id)
            if xf_user_id:
                link_text = f"View your Profile"
                link_url = f"https://www.droptracker.io/players/{player_name}.{player_id}/view"
                if notif_type == 'drop':
                    alert_text = f"Your {data.get('item_name')} drop has been processed & sent to Discord."
                    link_text = f"View Drop"
                    link_url = f"https://www.droptracker.io/drops/{notif_id}"
                elif notif_type == 'clog':
                    alert_text = f"Your new log slot ({data.get('item_name')}) has been processed & sent to Discord."
                elif notif_type == 'ca':
                    alert_text = f"Your {data.get('task_name')} combat achievement has been processed & sent to Discord."
                elif notif_type == 'pb':
                    alert_text = f"Your new personal best ({data.get('npc_name')}) has been processed & sent to Discord."
                else:
                    return

                await create_alert(xf_user_id, alert_text, link_text, link_url)
        except Exception as e:
            app_logger.log(log_type="error", data=f"Error sending player notification: {e}", app_name="notification_service", description="send_player_notification")
            raise

    async def remove_group_field(self, embed: interactions.Embed):
        """Removes the Group field from the embed"""
        if embed.fields:
            embed.fields = [field for field in embed.fields if "Group" not in field.name]
        return embed
    
    async def remove_kc_field(self, embed: interactions.Embed):
        """Removes the Kills field from the embed"""
        if embed.fields:
            embed.fields = [field for field in embed.fields if "Source:" not in field.name]
        return embed
    
    async def _is_not_sent(self, notification: NotificationQueue, data: dict):
        """Check if a notification has already been sent.
        Returns True if the notification should be sent, False if it should be skipped."""
        try:
            # Get the appropriate tracking dictionary based on notification type
            tracking_dict = None
            id_key = None
            
            if notification.notification_type == 'drop':
                tracking_dict = sent_drops
                id_key = 'drop_id'
            elif notification.notification_type == 'pb':
                tracking_dict = sent_pbs
                id_key = 'pb_id'
            elif notification.notification_type == 'ca':
                tracking_dict = sent_cas
                id_key = 'ca_id'
            elif notification.notification_type == 'clog':
                tracking_dict = sent_clogs
                id_key = 'clog_id'
            else:
                return True
                
            if not tracking_dict or not id_key:
                return True
                
            # Initialize group tracking if needed
            if notification.group_id not in tracking_dict:
                tracking_dict[notification.group_id] = []
                
            # Get the ID to check
            notification_id = data.get(id_key)
            if not notification_id:
                return True
                
            # Check if this ID has been sent for this group
            if notification_id in tracking_dict[notification.group_id]:
                app_logger.log(log_type="info", 
                             data=f"Notification {notification.id} was already sent for {id_key} {notification_id} in group {notification.group_id}", 
                             app_name="notification_service", 
                             description="_is_not_sent")
                return False  # Return False to prevent sending
            
            # Add to tracking and allow sending
            tracking_dict[notification.group_id].append(notification_id)
            return True
            
        except Exception as e:
            app_logger.log(log_type="error", 
                         data=f"Error checking if notification was sent: {e}", 
                         app_name="notification_service", 
                         description="_is_not_sent")
            return True  # On error, allow sending to be safe
            
    async def cleanup_tracking_dicts(self):
        """Clean up the tracking dictionaries to prevent unbounded growth"""
        try:
            # Keep only the last 1000 entries per group
            max_entries = 1000
            
            for group_id in list(sent_drops.keys()):
                sent_drops[group_id] = sent_drops[group_id][-max_entries:]
                
            for group_id in list(sent_pbs.keys()):
                sent_pbs[group_id] = sent_pbs[group_id][-max_entries:]
                
            for group_id in list(sent_cas.keys()):
                sent_cas[group_id] = sent_cas[group_id][-max_entries:]
                
            for group_id in list(sent_clogs.keys()):
                sent_clogs[group_id] = sent_clogs[group_id][-max_entries:]
                
        except Exception as e:
            app_logger.log(log_type="error", 
                         data=f"Error cleaning up tracking dictionaries: {e}", 
                         app_name="notification_service", 
                         description="cleanup_tracking_dicts")

    async def cleanup_stuck_notifications(self):
        """Reset notifications that have been stuck in 'processing' status for too long"""
        try:
            # Find notifications stuck in processing for more than 10 minutes
            stuck_time = datetime.now() - timedelta(minutes=10)
            stuck_notifications = session.query(NotificationQueue).filter(
                NotificationQueue.status == 'processing',
                NotificationQueue.processed_at.is_(None)
            ).all()
            
            if stuck_notifications:
                app_logger.log(log_type="warning", 
                             data=f"Found {len(stuck_notifications)} stuck notifications, resetting to pending", 
                             app_name="notification_service", 
                             description="cleanup_stuck_notifications")
                
                for notification in stuck_notifications:
                    notification.status = 'pending'
                    notification.error_message = 'Reset due to timeout'
                
                session.commit()
                
        except Exception as e:
            app_logger.log(log_type="error", 
                         data=f"Error cleaning up stuck notifications: {e}", 
                         app_name="notification_service", 
                         description="cleanup_stuck_notifications")

    def _get_player_month_total(self, player_id: int, partition: int = None) -> int:
        """Fetch the player's monthly total loot from Redis computed by redis_updates."""
        try:
            if partition is None:
                partition = get_current_partition()
            key = f"player:{player_id}:{partition}:total_loot"
            total_str = redis_client.get(key)
            if total_str is None:
                # Fallback to global leaderboard score if key missing
                score = redis_client.client.zscore(f"leaderboard:{partition}", player_id)
                return int(float(score)) if score is not None else 0
            return int(float(total_str))
        except Exception:
            return 0