import datetime
import json
import hashlib
import random
import time
from dataclasses import dataclass
from collections import deque
from typing import List, Dict, Optional
from interactions import BaseComponent, Extension, listen
import interactions
from interactions import ComponentContext, Extension, ActionRow, Button, ButtonStyle, FileComponent, PartialEmoji, Permissions, SlashContext, UnfurledMediaItem, listen, slash_command
from interactions.api.events import Startup, Component, ComponentCompletion, ComponentError, ModalCompletion, ModalError, MessageCreate
from interactions.models import ContainerComponent, ThumbnailComponent, SeparatorComponent, UserSelectMenu, SlidingWindowSystem, SectionComponent, SeparatorComponent, TextDisplayComponent, ThumbnailComponent, MediaGalleryComponent, MediaGalleryItem, OverwriteType
from db.models import GroupConfiguration, GroupPersonalBestMessage, Guild, get_current_partition, session, Group, NpcList, User, Player, user_group_association, PersonalBestEntry
from sqlalchemy import select, func, text
from sqlalchemy.orm import aliased
from db.ops import get_formatted_name
from utils.format import convert_from_ms, format_number, get_npc_image_url
import asyncio
from utils.redis import redis_client

class HallOfFame(Extension):
    def __init__(self, bot: interactions.Client):
        self.bot = bot
        # Async job queue and workers for controlled updates
        self._hof_queue: asyncio.Queue["HOFJob"] = asyncio.Queue(maxsize=1000)
        self._pending_jobs: set[str] = set()
        self._guild_locks: Dict[int, asyncio.Lock] = {}
        self._guild_limiters: Dict[int, "RateLimiter"] = {}
        self._global_limiter: RateLimiter = RateLimiter(max_calls=8, period_seconds=1.0)
        self._group_forbidden_until: Dict[int, float] = {}
        self._workers = [asyncio.create_task(self._worker(i)) for i in range(3)]
        print(f"[HALL OF FAME] Started {len(self._workers)} worker(s)")
        asyncio.create_task(self.update_hall_of_fame())
        # print("Hall of Fame service initialized.")
    

    def _is_in_development(self):
        cfg = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == 2,
                                                GroupConfiguration.config_key == "is_in_development").first()
        if cfg and cfg.config_value == "1":
            return True
        return False
    
    async def guild_has_bot(self, guild_id: int):
        try:
            guild = await self.bot.fetch_guild(guild_id)
            return guild.get_member(self.bot.user.id) is not None
        except Exception as e:
            print("Error checking if guild has bot:", e)
            return False
        return False
    

    async def update_hall_of_fame(self):
        while True:
            # print("Update hall of fame called")
            groups_configured = session.query(GroupConfiguration).filter(GroupConfiguration.config_key == "create_pb_embeds",
                                                                                 GroupConfiguration.config_value == "1").all()
            #group_ids = [group.group_id for group in groups_to_update]
            print(f"[HALL OF FAME] Group IDs to update: {[group.group_id for group in groups_configured]}")

            final_list = []
            group_obj_list = []
            for group_cfg in groups_configured:
                group_id = group_cfg.group_id
                group_obj = session.query(Group).filter(Group.group_id == group_id).first()
                guild_id = group_obj.guild_id
                guild = await self.bot.fetch_guild(guild_id)
                if not guild:
                    group_cfg.config_value = "0"
                    session.commit()
                    print(f"[HALL OF FAME] Group {group_id} not found, disabling PB embeds")
                    continue
                group_obj_list.append(group_obj)
                if await self.guild_has_bot(guild_id):
                    print(f"[HALL OF FAME] Group {group_id} has bot, enabling PB embeds")
                    final_list.append(group_cfg)
            for group in final_list:
                group_obj = next((obj for obj in group_obj_list if obj.group_id == group.group_id), None)
                if not group_obj:
                    continue
                print(f"[HALL OF FAME] Updating group {group_obj.group_id}")
                await self._update_group_hof(group_obj)
            await asyncio.sleep(360)

    async def _update_group_hof(self, group: Group):
        if self._is_in_development() and group.group_id != 2:
            return
        group_bosses = []
        required_bosses: GroupConfiguration = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == group.group_id, 
                                                                                GroupConfiguration.config_key == "personal_best_embed_boss_list").first()
        boss_list = required_bosses.config_value
        if boss_list == "" or len(str(boss_list)) < 10:
            boss_list = required_bosses.long_value
        if boss_list == "" or len(str(boss_list)) < 10:
            ## Neither field has entries, so we skip this group
            print(f"[HALL OF FAME] Group {group.group_id} has no bosses to update")
            return
        bosses_to_update = boss_list.replace("[", "").replace("]", "").split(",")
        bosses_to_update = [boss.strip() for boss in bosses_to_update]
        for boss in bosses_to_update:
            if boss not in group_bosses:
                group_bosses.append(boss)
        print(f"[HALL OF FAME]Group {group.group_id} bosses to update: {group_bosses}")
        for boss in group_bosses:
            boss = boss.replace('"', '')
            npc = session.query(NpcList).filter(NpcList.npc_name == boss).first()
            if npc:
                print(f"[HALL OF FAME] Enqueuing job for {boss} in group {group.group_id}")
                await self._enqueue_job(group_id=group.group_id, npc_id=npc.npc_id)
            else:
                print(f"[HALL OF FAME] NPC {boss} not found in group {group.group_id}")
                pass

    async def _should_send_hof(self, group_id: int, npc: NpcList):
        required_bosses: GroupConfiguration = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == group_id, 
                                                                                GroupConfiguration.config_key == "personal_best_embed_boss_list").first()
        if required_bosses and required_bosses.config_value:
            boss_list = required_bosses.config_value
            if boss_list == "" or len(str(boss_list)) < 10:
                boss_list = required_bosses.long_value
            if boss_list == "" or len(str(boss_list)) < 10:
                ## Neither field has entries, so we skip this group
                return False
            bosses_to_update = boss_list.replace("[", "").replace("]", "").split(",")
            bosses_to_update = [boss.strip() for boss in bosses_to_update]
            if npc.npc_name in bosses_to_update:
                return True
        return False
    
    async def _update_boss_component(self, group_id: int, npc: NpcList):
        if await self._should_send_hof(group_id, npc):
            await self._enqueue_job(group_id=group_id, npc_id=npc.npc_id)
        else:
            # # print(f"[HALL OF FAME]No need to update boss component for {npc.npc_name}")
            pass

    async def _send_boss_components(self, group_id: int, npc: NpcList, components: List[BaseComponent]):
        group = session.query(Group).filter(Group.group_id == group_id).first()
        if group:
            # print(f"[HALL OF FAME] Sending components for {npc.npc_name} in group {group_id}")
            channel_cfg = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == group_id, GroupConfiguration.config_key == "channel_id_to_send_pb_embeds").first()
            existing_message = session.query(GroupPersonalBestMessage).filter(GroupPersonalBestMessage.group_id == group_id,
                                                                              GroupPersonalBestMessage.boss_name == npc.npc_name).first()
            if existing_message:
                # print(f"[HALL OF FAME] Existing message found for {npc.npc_name} in group {group_id}: {existing_message.message_id}")
                message_id = existing_message.message_id
                channel_id = existing_message.channel_id
                if message_id and message_id != "":
                    # print(f"[HALL OF FAME] Message ID: {message_id}")
                    try:
                        channel = await self.bot.fetch_channel(int(channel_id))
                        if channel:
                            message = await channel.fetch_message(int(message_id))
                            # Rate-limited edit with retry
                            await self._rate_limited_send_or_edit("edit", group_id)
                            await message.edit(components=components)
                            existing_message.date_updated = datetime.datetime.now()
                            session.commit()
                            await asyncio.sleep(random.uniform(0.15, 0.35))
                            return True
                        else:
                            # print(f"[HALL OF FAME]Channel not found for {channel_id}")
                            pass
                    except Exception as e:
                        # Re-raise to allow caller to classify and handle (e.g., 403/429)
                        raise
            elif channel_cfg and channel_cfg.config_value:
                channel_id = channel_cfg.config_value
                if channel_id != "":
                    channel = await self.bot.fetch_channel(int(channel_id))
                    if channel:
                        await self._rate_limited_send_or_edit("send", group_id)
                        message = await channel.send(components=components)
                        # # print(f"[HALL OF FAME]Message sent to channel for {npc.npc_name}")
                        await asyncio.sleep(random.uniform(0.15, 0.35))
                        session.add(GroupPersonalBestMessage(group_id=group_id, message_id=message.id, channel_id=channel_id, boss_name=npc.npc_name))
                        session.commit()
                        return True
                    else:
                        # Channel missing is a hard failure; raise so caller can mark forbidden
                        raise RuntimeError(f"Channel not found for id {channel_id}")
            else:
                # print(f"[HALL OF FAME]Channel not configured for group {group_id}")
                return False
        else:
            # print(f"[HALL OF FAME]Group not found for {group_id}")
            return False

    async def _finalize_boss_components(self, npc: NpcList, group: Group):
        # Create components matching message_handler.py structure
        pb_components, summary_content = self._create_pb_components(group.group_id, npc)
        # print(f"[HALL OF FAME]PB components returned: {pb_components}")
        # print(f"[HALL OF FAME]PB component types: {[type(c) for c in pb_components]}")

        container = ContainerComponent(
            SeparatorComponent(divider=True),
            SectionComponent(
                components=[
                    TextDisplayComponent(
                        content=f"## 🏆 {self._get_linked_name(npc)} - Hall of Fame 🏆\n" + 
                        f"{summary_content}"
                    )
                ],
                accessory=ThumbnailComponent(
                    media=UnfurledMediaItem(
                        url=self._get_npc_img_url(npc)
                    )
                )
            ),
            SeparatorComponent(divider=True),
            *pb_components,
            SeparatorComponent(divider=True),
            TextDisplayComponent(
                content=f"-# Powered by the [DropTracker](https://www.droptracker.io) • [View all Personal Bests](https://www.droptracker.io/personal_bests)"
            ),
            SeparatorComponent(divider=True),
        )

        components = [container]

        return components

    def _create_base_boss_component(self, npc: NpcList):
        """
        Creates the base component layout for a boss message
        """
        components = [
            ContainerComponent(
                SeparatorComponent(divider=True),
                SectionComponent(
                    components=[
                        TextDisplayComponent(
                            content=f"### 🏆 {self._get_linked_name(npc)} - Hall of Fame 🏆\n" + 
                            f""
                        )
                    ],
                    accessory=ThumbnailComponent(
                        media=UnfurledMediaItem(
                            url=self._get_npc_img_url(npc)
                        )
                    )
                ),
                SeparatorComponent(divider=True),
            ),
        ]
        return components
    

    def _create_pb_components(self, group_id: int, npc: NpcList):
        """
        Create the personal best components for a given group and npc
        """
        pbs = self._get_pbs(group_id, npc.npc_name)
        components = []
        fastest_kill = None
        # print(f"[HALL OF FAME]Got PBs: {pbs}")
        fastest_kill_part = ""
        total_pbs = 0
        for team_size, entries in pbs.items():
            # print(f"[HALL OF FAME]Team size: {team_size}")
            for pb in entries:
                # print(f"[HALL OF FAME]PB: {pb}")
                total_pbs += 1
                if fastest_kill is None or pb.personal_best < fastest_kill[0]:
                    fastest_kill = [pb.personal_best, team_size, pb.player_id, None]
        # print(f"[HALL OF FAME]Fastest kill: {fastest_kill}")
        if total_pbs > 0:
            if fastest_kill:
                fastest_kill[3] = session.query(Player).filter(Player.player_id == fastest_kill[2]).first()
                fastest_kill_part = (f"-# • Fastest kill: `{convert_from_ms(fastest_kill[0])}` ({self._get_team_size_string(fastest_kill[1])})\n" +
                                     f"-# ↳ by {get_formatted_name(fastest_kill[3].player_name, group_id, session)}")
            else:
                fastest_kill = [0, 0, 0, "No data"]
        partition = get_current_partition()
        if group_id != 2:
            key = f"leaderboard:group:{group_id}:npc:{npc.npc_id}:{partition}"
            all_key = f"leaderboard:group:{group_id}:npc:{npc.npc_id}"
        else:
            key = f"leaderboard:npc:{npc.npc_id}:{partition}"
            all_key = f"leaderboard:npc:{npc.npc_id}"
        # print(f"[HALL OF FAME]Using key: {key}")
        most_loot_month = redis_client.client.zrevrange(key, 0, 4, withscores=True)
        most_loot_part = ""
        total_loot_part = ""
        if len(most_loot_month) > 1:
       
            month_looters = []
            for loot in most_loot_month:
                player = session.query(Player).filter(Player.player_id == loot[0]).first()
                month_looters.append([loot[0], 1, loot[1], player])
            most_loot = month_looters[0]
            # print(f"[HALL OF FAME]Most loot: {most_loot}")
            
            most_loot_alltime = redis_client.client.zrevrange(all_key, 0, 4, withscores=True)
            if len(most_loot_alltime) > 1:
                most_loot_alltime = most_loot_alltime[0]
                alltime_most_loot = [most_loot_alltime[0], 1, most_loot_alltime[1], None]
            else:
                alltime_most_loot = [0, 0, 0, "No data"]
            # print(f"[HALL OF FAME]All-time most loot: {alltime_most_loot}")
            alltime_most_loot[3] = session.query(Player).filter(Player.player_id == alltime_most_loot[0]).first()
            # print(f"[HALL OF FAME]All-time most loot player: {alltime_most_loot[3]}")
            total_loot = redis_client.zsum(all_key)
            most_loot_part = (f"\n-# • Most Loot: `{format_number(most_loot[2])}` gp (this month)\n" +
                f"-# ↳ by {get_formatted_name(most_loot[3].player_name, group_id, session)}")
            total_loot_part = f"-# • Total loot tracked: `{format_number(total_loot)}` gp\n"
            
        # Debug the content being created
        summary_content = (
            f"📊 **__Overview__**\n" +
            f"-# • Total PBs tracked: `{total_pbs}`\n" +
            f"{total_loot_part}" +
            f"{fastest_kill_part}" +
            f"{most_loot_part}"
        )
        # # print(f"[HALL OF FAME]Summary content: {summary_content}")
        
        # summary_component = TextDisplayComponent(content=summary_content)
        # # print(f"[HALL OF FAME]Summary component type: {type(summary_component)}")
        # components.append(summary_component)
        if len(most_loot_month) > 1:
            loot_str = ""
            for i in range(len(most_loot_month)):
                loot_str += f"-# {i + 1}. {get_formatted_name(month_looters[i][3].player_name, group_id, session)} - `{format_number(month_looters[i][2])}` gp\n"
            looters_content = (
                f"💰 **__Loot Leaderboard__**\n" +
                f"-# Top 5 players (this month):\n" +
                loot_str
            )
            looters_component = TextDisplayComponent(content=looters_content)
            components.append(looters_component)
            components.append(SeparatorComponent(divider=True))
        components.append(
            TextDisplayComponent(
                content=f":hourglass: **__Personal Best Leaderboards__**\n" 
        ))

        ## Sort the team sizes to place solo first, then 2, 3, 4, etc
        team_size_order = ["Solo", "1", "2", "3", "4", "5", "6+", "7", "8", "9", "10"]
        pbs = {k: v for k, v in sorted(pbs.items(), key=lambda item: team_size_order.index(str(item[0])) if str(item[0]) in team_size_order else len(team_size_order))}

        for team_size, entries in pbs.items():
            team_size_string = self._get_team_size_string(team_size)
            team_size_component = TextDisplayComponent(content=f"-# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n" + 
                                                       f"-# **{team_size_string}**")
            # print(f"[HALL OF FAME]Team size component type: {type(team_size_component)}")
            components.append(team_size_component)
            pb_text = ""
            for i, pb in enumerate(entries):
                if i >= 5:
                    break
                pb: PersonalBestEntry = pb
                pb_text += f"-# {i + 1} - `{convert_from_ms(pb.personal_best)}` - {get_formatted_name(pb.player.player_name, group_id, session)}\n"
            pb_component = TextDisplayComponent(content=pb_text)
            components.append(pb_component)
        # print(f"[HALL OF FAME]Final components list: {components}")
        # print(f"[HALL OF FAME]Component types: {[type(c) for c in components]}")
        return components, summary_content
    
    def _get_team_size_string(self, team_size: int):
        match team_size:
            case 1 | "Solo":
                return "Solo"
            case 2 | "Duo":
                return "Duo"
            case 3 | "Trio":
                return "Trio"
            case _:
                return f"{team_size} players"

    def _get_pbs(self, group_id: int, npc_name: str):
        """
        Get the personal bests for a given group and npc name
        """
        npc_ids = session.query(NpcList.npc_id).filter(NpcList.npc_name == npc_name).all()
        npc_ids = [npc_id[0] for npc_id in npc_ids]
        group = session.query(Group).filter(Group.group_id == group_id).first()
        players = group.get_players()
        player_ids = [player.player_id for player in players]
        # player_ids = session.query(text("player_id FROM user_group_association WHERE group_id = :group_id")).params(group_id=group_id).all()
        # player_ids = [player_id[0] for player_id in player_ids]
        ## Remove duplicates
        player_ids = list(set(player_ids))
        pbs = session.query(PersonalBestEntry).filter(PersonalBestEntry.player_id.in_(player_ids), PersonalBestEntry.npc_id.in_(npc_ids)).all()
        personal_bests = {}
        # # print(f"[HALL OF FAME]Got {len(pbs)} pbs")
        unique_team_sizes = set()
        for pb in pbs:
            if pb.team_size not in unique_team_sizes:
                unique_team_sizes.add(pb.team_size)
        # # print(f"[HALL OF FAME]Unique team sizes: {unique_team_sizes}")
        if len(unique_team_sizes) > 5:
            ## Remove the largest team sizes if there are more than 5
            pbs = [pb for pb in pbs if pb.team_size in ["Solo", "2", 2, "3", 3, "4", 4, "5", 5]]
        for pb in pbs:
            if pb.team_size not in personal_bests:
                # # print(f"[HALL OF FAME]Adding team size: {pb.team_size}")
                personal_bests[pb.team_size] = []
            personal_bests[pb.team_size].append(pb)
        for team_size in personal_bests:
            ## Sort the entries by the lowest personal best
            personal_bests[team_size].sort(key=lambda x: x.personal_best)
        return personal_bests

    def _get_linked_name(self, npc: NpcList):
        return f"[{npc.npc_name}]({self._get_npc_url(npc)})"

    def _get_npc_img_url(self, npc: NpcList):
        return f"https://www.droptracker.io/img/npcdb/{npc.npc_id}.png"
    
    def _get_npc_url(self, npc: NpcList):
        npc_name = npc.npc_name.replace(" ", "-")
        return f"https://www.droptracker.io/npcs/{npc_name}.{npc.npc_id}/view"

    def _components_equal(self, component1, component2):
        """
        Compare two components for equality, handling common differences like trailing whitespace
        """
        if type(component1) != type(component2):
            return False
        
        dict1 = component1.__dict__.copy()
        dict2 = component2.__dict__.copy()
        
        # Recursively normalize the dictionaries
        self._normalize_dict(dict1, visited=set())
        self._normalize_dict(dict2, visited=set())
        
        return dict1 == dict2
    
    def _normalize_dict(self, d, visited=None):
        """
        Recursively normalize a dictionary by stripping trailing whitespace from strings
        and handling nested structures
        """
        if visited is None:
            visited = set()
        
        # Get the object's id to track visited objects
        obj_id = id(d)
        if obj_id in visited:
            return
        visited.add(obj_id)
        
        for key, value in list(d.items()):  # Use list() to avoid dict modification during iteration
            if isinstance(value, str):
                # Strip trailing whitespace from strings
                d[key] = value.rstrip()
            elif isinstance(value, list):
                # Handle lists of components or other objects
                for i, item in enumerate(value):
                    if hasattr(item, '__dict__'):
                        # Check if it's a component object (not a SQLAlchemy model)
                        if hasattr(item, '__class__') and 'Component' in item.__class__.__name__:
                            # If it's an object with attributes, normalize its dict
                            item_dict = item.__dict__.copy()
                            self._normalize_dict(item_dict, visited)
                            # Update the item's dict in place
                            for k, v in item_dict.items():
                                setattr(item, k, v)
                    elif isinstance(item, str):
                        # If it's a string in the list, strip it
                        value[i] = item.rstrip()
            elif isinstance(value, dict):
                # Recursively handle nested dictionaries
                self._normalize_dict(value, visited)
            elif hasattr(value, '__dict__'):
                # Only normalize component objects, not SQLAlchemy models
                if hasattr(value, '__class__') and 'Component' in value.__class__.__name__:
                    # Handle nested objects with their own attributes
                    nested_dict = value.__dict__.copy()
                    self._normalize_dict(nested_dict, visited)
                    # Update the nested object's attributes
                    for k, v in nested_dict.items():
                        setattr(value, k, v)

    # -------------------- New: Queue, Rate Limiter and Hashing Utilities --------------------

    async def _enqueue_job(self, group_id: int, npc_id: int):
        key = f"{group_id}:{npc_id}"
        if key in self._pending_jobs:
            return
        self._pending_jobs.add(key)
        job = HOFJob(group_id=group_id, npc_id=npc_id)
        try:
            await self._hof_queue.put(job)
            try:
                qsize = self._hof_queue.qsize()
            except Exception:
                qsize = "?"
            print(f"[HALL OF FAME] Job enqueued {key}. Queue size: {qsize}")
        except Exception:
            self._pending_jobs.discard(key)

    async def _worker(self, worker_index: int):
        while True:
            job: HOFJob = await self._hof_queue.get()
            key = f"{job.group_id}:{job.npc_id}"
            try:
                #print(f"[HALL OF FAME][Worker {worker_index}] Processing job {key}")
                await self._process_job(job)
                #print(f"[HALL OF FAME][Worker {worker_index}] Finished job {key}")
            except Exception as e:
                import traceback
                print(f"[HALL OF FAME][Worker {worker_index}] Job {key} failed: {e}\n{traceback.format_exc()}")
            finally:
                self._hof_queue.task_done()
                self._pending_jobs.discard(key)

    async def _process_job(self, job: "HOFJob"):
        lock = self._get_guild_lock(job.group_id)
        async with lock:
            # Skip processing if group is temporarily forbidden
            forbidden_until = self._group_forbidden_until.get(job.group_id)
            if forbidden_until and time.monotonic() < forbidden_until:
                # print(f"[HALL OF FAME] Group {job.group_id} in forbidden cooldown; skipping job")
                return
            await self._process_boss_update(job)

    async def _process_boss_update(self, job: "HOFJob"):
        group = session.query(Group).filter(Group.group_id == job.group_id).first()
        print(f"[HALL OF FAME] Processing job for group id {job.group_id} (npc id {job.npc_id})")
        npc = session.query(NpcList).filter(NpcList.npc_id == job.npc_id).first()
        if not group or not npc:
            return
        # Ensure the bot is a member of the target guild before proceeding
        guild = session.query(Group).filter(Group.group_id == job.group_id).first()
        if not guild or not await self.guild_has_bot(guild.guild_id):
            guild_id = guild.guild_id if guild else None
            print(f"[HALL OF FAME] Skipping job {job.group_id}:{job.npc_id} - bot not in guild {guild_id} or guild missing")
            return
        # Skip if no changes (hash check)
        components = await self._finalize_boss_components(npc, group)
        new_hash = self._compute_components_hash(components)
        if self._is_same_hash(job.group_id, job.npc_id, new_hash):
            print(f"[HALL OF FAME] Skipping job {job.group_id}:{job.npc_id} - no changes")
            return
        # Attempt send/edit with retries and rate limiting
        max_attempts = 5
        base_delay = 0.5
        for attempt in range(1, max_attempts + 1):
            try:
                await self._rate_limited_send_or_edit("send_or_edit", job.group_id)
                success = await self._send_boss_components(job.group_id, npc, components)
                if success:
                    self._store_components_hash(job.group_id, job.npc_id, new_hash)
                    print(f"[HALL OF FAME] Updated components for {job.group_id}:{job.npc_id}")
                    return
            except Exception as e:
                # If Forbidden (403), put this group into cooldown and stop retrying until next loop
                if self._is_forbidden_error(e):
                    self._group_forbidden_until[job.group_id] = time.monotonic() + 330.0
                    print(f"[HALL OF FAME] 403 Forbidden for group {job.group_id}; skipping all jobs for this group until next loop")
                    return
                # Handle rate-limits and transient errors with backoff
                retry_after = getattr(e, "retry_after", None)
                if retry_after is None:
                    msg = str(e)
                    if "429" in msg or "rate" in msg.lower():
                        retry_after = min(2 ** attempt, 15)
                delay = retry_after if retry_after is not None else base_delay * attempt
                delay += random.uniform(0.05, 0.25)
                await asyncio.sleep(delay)
                print(f"[HALL OF FAME] Retry {attempt} for {job.group_id}:{job.npc_id} after {delay:.2f}s")

    def _get_guild_lock(self, group_id: int) -> asyncio.Lock:
        lock = self._guild_locks.get(group_id)
        if lock is None:
            lock = asyncio.Lock()
            self._guild_locks[group_id] = lock
        return lock

    def _get_guild_limiter(self, group_id: int) -> "RateLimiter":
        limiter = self._guild_limiters.get(group_id)
        if limiter is None:
            # Allow up to 2 actions per second per guild
            limiter = RateLimiter(max_calls=2, period_seconds=1.0)
            self._guild_limiters[group_id] = limiter
        return limiter

    async def _rate_limited_send_or_edit(self, _op: str, group_id: int):
        # Acquire both global and per-guild limiter slots
        await asyncio.gather(
            self._global_limiter.acquire(),
            self._get_guild_limiter(group_id).acquire(),
        )

    def _components_plain(self, obj, visited=None):
        if visited is None:
            visited = set()
        oid = id(obj)
        if oid in visited:
            return None
        visited.add(oid)
        if obj is None:
            return None
        if isinstance(obj, (str, int, float, bool)):
            return obj
        if isinstance(obj, list):
            return [self._components_plain(x, visited) for x in obj]
        if isinstance(obj, dict):
            return {str(k): self._components_plain(v, visited) for k, v in obj.items()}
        if hasattr(obj, "__dict__"):
            data = {k: v for k, v in obj.__dict__.items() if not str(k).startswith("_")}
            # Reduce noisy attrs
            pruned = {k: self._components_plain(v, visited) for k, v in data.items()}
            pruned["__class__"] = obj.__class__.__name__
            return pruned
        return str(obj)

    def _is_forbidden_error(self, e: Exception) -> bool:
        try:
            # interactions Forbidden might present as attribute or type
            status = getattr(e, "status", None) or getattr(e, "code", None)
            if status == 403:
                return True
            text = str(e).lower()
            if " 403" in text or "forbidden" in text:
                return True
        except Exception:
            pass
        return False

    def _compute_components_hash(self, components: List[BaseComponent]) -> str:
        plain = self._components_plain(components)
        # Normalize trailing whitespace for stability
        if isinstance(plain, dict):
            self._normalize_dict(plain, visited=set())
        s = json.dumps(plain, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    def _hash_key(self, group_id: int, npc_id: int) -> str:
        return f"hof:hash:{group_id}:{npc_id}"

    def _is_same_hash(self, group_id: int, npc_id: int, new_hash: str) -> bool:
        try:
            key = self._hash_key(group_id, npc_id)
            existing = redis_client.client.get(key)
            if not existing:
                return False
            if isinstance(existing, bytes):
                existing = existing.decode("utf-8")
            return existing == new_hash
        except Exception:
            return False

    def _store_components_hash(self, group_id: int, npc_id: int, new_hash: str):
        try:
            key = self._hash_key(group_id, npc_id)
            # Short TTL ensures auto-refresh; 7 days default
            redis_client.client.set(key, new_hash, ex=7 * 24 * 3600)
        except Exception:
            pass


class RateLimiter:
    def __init__(self, max_calls: int, period_seconds: float):
        self.max_calls = max_calls
        self.period = period_seconds
        self.calls = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            while self.calls and now - self.calls[0] > self.period:
                self.calls.popleft()
            if len(self.calls) < self.max_calls:
                self.calls.append(now)
                return
            # Need to wait
            sleep_for = self.period - (now - self.calls[0])
            sleep_for = max(sleep_for, 0.0) + random.uniform(0.01, 0.05)
            await asyncio.sleep(sleep_for)
            # After sleeping, record the call time
            now2 = time.monotonic()
            while self.calls and now2 - self.calls[0] > self.period:
                self.calls.popleft()
            self.calls.append(now2)


@dataclass
class HOFJob:
    group_id: int
    npc_id: int

