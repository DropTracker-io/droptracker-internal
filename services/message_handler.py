from datetime import datetime
import os
import interactions
from interactions import ChannelType, ContextMenuContext, Extension, listen, Message, message_context_menu
from interactions.api.events import MessageCreate, Component
from db.models import Group, GroupPatreon, PlayerPet, session, Player, ItemList, PersonalBestEntry
#from db.update_player_total import update_player_in_redis
from db.xf.recent_submissions import create_xenforo_entry
from services.components import InfoActionRow
from utils.embeds import update_boss_pb_embed
# Removed circular import - these will be imported lazily inside functions if needed
from utils.msg_logger import HighThroughputLogger
from data.submissions import clog_processor, ca_processor, pb_processor, drop_processor
from utils.ge_value import get_true_item_value
from utils.format import convert_to_ms, convert_from_ms, get_true_boss_name
from utils.redis import redis_client
from db.app_logger import AppLogger
from interactions import AutocompleteContext, BaseContext, GuildText, Permissions, SlashCommand, UnfurledMediaItem, PartialEmoji, ActionRow, Button, ButtonStyle, SlashCommandOption, check, is_owner, Extension, slash_command, slash_option, SlashContext, Embed, OptionType, GuildChannel, SlashCommandChoice
from interactions.api.events import Startup, Component, ComponentCompletion, ComponentError, ModalCompletion, ModalError, MessageCreate
from interactions.models import ContainerComponent, ThumbnailComponent, SeparatorComponent, UserSelectMenu, SlidingWindowSystem, SectionComponent, SeparatorComponent, TextDisplayComponent, ThumbnailComponent, MediaGalleryComponent, MediaGalleryItem, OverwriteType



app_logger = AppLogger()
bot_token = os.getenv("DISCORD_TOKEN")
ignored_list = []
last_xf_transfer = datetime.now()


class MessageHandler(Extension):
    def __init__(self, bot: interactions.Client):
        self.bot = bot

    # @listen(MessageCreate)
    # async def on_message_create(self, event: MessageCreate):
    #     def embed_to_dict(embed: Embed):
    #         if embed.fields:
    #             return {f.name: f.value for f in embed.fields}
    #         return {}
    #     global last_xf_transfer
    #     global ignored_list
    #     bot: interactions.Client = event.bot
    #     if bot.is_closed:
    #         await bot.astart(token=bot_token)
    #     await bot.wait_until_ready()
    #     if isinstance(event, Message):
    #         message = event
    #     else:
    #         message = event.message
    #     if message.channel.type == ChannelType.GUILD_TEXT:
    #         if str(message.channel.id) == "1210765281371365477" or str(message.channel.id) == "1369771179094708254":
    #             if "!welcome" in message.content.lower():
    #                 await message.delete()
    #                 await self.send_welcome_page(message)
    #             elif "!invite" in message.content.lower():
    #                 await message.delete()
    #                 await self.send_invite_page(message)
    #         if "!forcehof" in message.content.lower():
    #             group_id = 19
    #             hall_of_fame = self.bot.get_ext("services.hall_of_fame")
    #             if hall_of_fame:
    #                 try:
    #                     group = session.query(Group).filter(Group.group_id == group_id).first()
    #                     if not group:
    #                         return await message.channel.send("Group not found.")
    #                     await hall_of_fame._update_group_hof(group) 
    #                 except Exception as e:
    #                     print(f"Error updating boss component: {e}")
    #                     pass
    #     if message.author.id and str(message.author.id) == "528746710042804247":
    #         if "!logs" in message.content.lower():
    #                 await message.delete()
    #                 await self.send_runelite_logs_guide(message)
    #     if message.author.system:  # or message.author.bot:
    #         return
    #     if message.author.id == bot.user.id:
    #         return
    #     if message.channel.type == ChannelType.DM or message.channel.type == ChannelType.GROUP_DM:
    #         return
        

    #     channel_id = message.channel.id

    #     # if str(message.channel.id) == "1262137292315688991":
    #     skip_check = True
    #     target_guilds = ["1172737525069135962",
    #                     "900855778095800380",
    #                     "597397938989432842",
    #                     "702992720909828168",
    #                     "1120606216972947468"]
    #     if str(message.guild.id) in target_guilds:
    #         #print(f"Guild name: {message.guild.name}")
    #     #if str(channel_id) == "1262137292315688991":
    #         item_name = ""
    #         player_name = ""
    #         item_id = 0
    #         npc_name = "none"
    #         value = 0
    #         quantity = 0
    #         sheet_id = ""
    #         source_type = ""
    #         imageUrl = ""
    #         token = ""
    #         account_hash = ""
    #         for embed in message.embeds:
    #             embed_data = embed_to_dict(embed)
    #             if message.attachments:
    #                 for attachment in message.attachments:
    #                     if attachment.url:
    #                         embed_data['attachment_url'] = attachment.url
    #                         embed_data['attachment_type'] = attachment.content_type
    #             field_names = [field.name for field in embed.fields]
    #             if embed_data:
    #                 field_values = [field.value.lower().strip() for field in embed.fields]
    #                 if "source_type" in field_names and "loot chest" in field_values:
    #                     ## Skip pvp
    #                     continue
    #                 rsn = ""
    #                 embed_data['used_api'] = False
    #                 if embed_data['type'] == "collection_log":
    #                     await clog_processor(embed_data)
    #                     continue
    #                 elif "combat_achievement" in field_values:
    #                     await ca_processor(embed_data)
    #                     continue
    #                 elif "npc_kill" in field_values or "kill_time" in field_values:
    #                     await pb_processor(embed_data)
    #                     continue
    #                 elif embed.title and "received some drops" in embed.title or "drop" in field_values:
    #                     await drop_processor(embed_data)
    #                     continue
    #                 elif "experience_update" in field_values or "experience_milestone" in field_values or "level_up" in field_values:
    #                     #await experience_processor(embed_data)
    #                     continue
    #                 elif "quest_completion" in field_values:
    #                     #await quest_processor(embed_data)
    #                     continue
    #                 elif "adventure_log" in field_values:
    #                     if embed.fields:
    #                         for field in embed.fields:
    #                             if field.name == "player":
    #                                 player_name = field.value
    #                                 break
    #                     player_object = session.query(Player).filter(Player.player_name == player_name).first()
    #                     if player_object:
    #                         player_id = player_object.player_id
    #                     else:
    #                         continue
    #                     if embed.fields:
    #                         for field in embed.fields:
    #                             if field.name == "player":
    #                                 player_name = field.value
    #                             elif field.name == "acc_hash":
    #                                 account_hash = field.value
    #                             if field.name != "type" and field.name != "player" and field.name != "acc_hash":
    #                                 try:
    #                                     field_int = int(field.name)
    #                                     pb_content = field.value
    #                                     personal_bests = pb_content.split("\n")
    #                                     for pb in personal_bests:
    #                                         boss_name, rest = pb.split(" - ")
    #                                         team_size, time = rest.split(" : ")
    #                                         boss_name = boss_name.strip()
    #                                         team_size = team_size.strip()
    #                                         boss_name, team_size, time = boss_name.replace("`", ""), team_size.replace("`", ""), time.replace("`", "")
    #                                         time = time.strip()
    #                                         real_boss_name, npc_id = get_true_boss_name(boss_name)
    #                                         existing_pb = session.query(PersonalBestEntry).filter(PersonalBestEntry.player_id == player_id, PersonalBestEntry.npc_id == npc_id,
    #                                                                                             PersonalBestEntry.team_size == team_size).first()
    #                                         time_ms = convert_to_ms(time)
    #                                         if existing_pb:
    #                                             if time_ms < existing_pb.personal_best:
    #                                                 existing_pb.personal_best = time_ms
    #                                                 session.commit()
    #                                         else:
    #                                             new_pb = PersonalBestEntry(player_id=player_id, npc_id=npc_id, 
    #                                                                     team_size=team_size, personal_best=time_ms, 
    #                                                                     kill_time=time_ms, new_pb=True)
    #                                             session.add(new_pb)
    #                                             session.commit()
                                    
    #                                 except ValueError:
    #                                     pet_list = field.value
    #                                     pet_list = pet_list.replace("[", "")
    #                                     pet_list = pet_list.replace("]", "")
    #                                     pet_list = pet_list.split(",")
    #                                     if len(pet_list) > 0:
    #                                         for pet in pet_list:
    #                                             pet = int(pet.strip())
    #                                             item_object: ItemList = session.query(ItemList).filter(ItemList.item_id == pet).first()
    #                                             if item_object:
    #                                                 player_pet = PlayerPet(player_id=player_id, item_id=item_object.item_id, pet_name=item_object.item_name)
    #                                                 try:
    #                                                     session.add(player_pet)
    #                                                     session.commit()
    #                                                     print("Added a pet to the database for", player_name, account_hash, item_object.item_name, item_object.item_id)
    #                                                 except Exception as e:
    #                                                     print("Couldn't add a pet to the database:", e)
    #                                                     session.rollback()

    
    @listen(Component)
    async def on_component(self, event: Component):
        ctx = event.ctx
        custom_id = ctx.custom_id
        if custom_id.startswith("patreon_group_"):
            group_id = int(custom_id.split("_")[2])
            valid_patreon = session.query(GroupPatreon).filter(GroupPatreon.user_id == ctx.user.id).first()
            if valid_patreon and valid_patreon.group_id == None:
                valid_patreon.group_id = group_id
                group = session.query(Group).filter(Group.group_id == group_id).first()
                await ctx.send(f"You have assigned your DropTracker Patreon subscription perks to {group.group_name}!")
                try:
                    await ctx.message.delete()
                except Exception as e:
                    print("Couldn't delete the message:", e)
                return
            else:
                await ctx.send("You don't have a valid Patreon subscription, or you already have a group assigned to it.")
                return
            
    async def send_invite_page(self, message: Message):
        channel = await self.bot.fetch_channel(message.channel.id)
        invite_components = [
            ContainerComponent(
                SeparatorComponent(divider=True),
                SectionComponent(
                    components=[
                        TextDisplayComponent(
                            content="## Invite me to your Discord Server",
                        ),
                    ],
                    accessory=Button(
                        label="Invite the DropTracker.io Bot",
                        style=ButtonStyle.LINK,
                        url="https://discord.com/oauth2/authorize?client_id=1172933457010245762&permissions=8&scope=bot"
                    )
                ),
                SeparatorComponent(divider=True),
            )
        ]
        components = invite_components
        await channel.send(components=components)


    async def send_runelite_logs_guide(self, message: Message):
        channel = await self.bot.fetch_channel(message.channel.id)
        log_components = [
            ContainerComponent(
                SeparatorComponent(divider=True),
                TextDisplayComponent(
                            content="### Finding your RuneLite client logs for debugging purposes:\n" +
                            "-# There are two primary ways to locate your `client.log` file:\n" +
                            "-# By right-clicking the <:screenshot:1380839233123651695> screenshot icon in the top-right corner of the RuneLite client\n" +
                            "-# or, navigate to:\n" +
                            "-# Windows: `%userprofile%.runelite\logs`\n" +
                            "-# Linux/MacOS: $HOME/.runelite/logs\n\n"
                            "-# You should see a file named `client.log` in this folder. Please drag and drop it here.",
                        ),
                SeparatorComponent(divider=True),
            )
        ]
        components = log_components
        await channel.send(components=components)
            

    async def send_welcome_page(self, message: Message):
        channel = await self.bot.fetch_channel(message.channel.id)
        logo_media = UnfurledMediaItem(
            url="https://www.droptracker.io/img/droptracker-small.gif"
        )
        welcome_page = [
            ContainerComponent(
                SeparatorComponent(divider=True),
                TextDisplayComponent(
                    content="# Welcome to the DropTracker.io Discord Server"
                ),
                SeparatorComponent(divider=True),
                SectionComponent(
                    components=[
                        TextDisplayComponent(
                            content="-# The DropTracker is an all-in-one loot and achievement" + 
                            "tracking system, built for Old School RuneScape players & groups.\n" +
                            "-# Our small team of developers work hard to provide a fun extension to the game:\n\n" + 
                            "-# <@528746710042804247> - Primary Development\n" +
                            "-# <@232236164776460288> - RuneLite plugin / community help\n\n" +
                            "-# We are always looking for more help!\n" + 
                            "-# If you are interested in joining the team, please reach out.",
                        ),
                    ],
                    accessory=ThumbnailComponent(
                        media=logo_media
                    )
                ),
                SeparatorComponent(divider=True),
                SectionComponent(
                    components=[
                        TextDisplayComponent(
                            content="### How do I use this app?",
                        ),
                        TextDisplayComponent(
                            content="-# We strive to provide the most simple integration for players and groups alike.\n" +
                            "-# All you *need* to do is install our [RuneLite plugin](https://www.droptracker.io/runelite), and your drops & achievements should automatically be tracked by the <@1172933457010245762> Discord bot.\n" +
                            "\n-# It is highly recommended, however, to enable **API connections** in our plugin configuration to ensure the most reliable tracking functionality."
                        )
                        
                    ],
                    accessory=ThumbnailComponent(
                        media=UnfurledMediaItem(
                            url="https://cdn2.steamgriddb.com/icon/1071f2d716fafebd789062219cec9c83/32/128x128.png"
                        )
                    )
                ),
                SeparatorComponent(divider=True),
                SectionComponent(
                    components=[
                        TextDisplayComponent(
                            content="### How does it work?",
                        ),
                        TextDisplayComponent(
                            content=(
                                "-# The DropTracker has two methods of tracking players using our plugin:\n" + 
                                "-# - Discord Webhooks\n" + 
                                "-# - API Connections (*preferred*)\n" +
                                "-# When you receive a drop or complete an achievement, your client will auto" +
                                "matically communicate this information with our system through whichever method you choose (by default, using Discord Webhooks).\n\n" +
                                "-# Once it arrives on our server, we determine based on WiseOldMan and our registered group listings whether or not it qualifies to have a notification sent via Discord.\n"
                            )
                        ),
                    ],
                    accessory=Button(
                            label="Read the Wiki",
                            style=ButtonStyle.LINK,
                            url="https://www.droptracker.io/wiki"
                        ),
                ),
                SeparatorComponent(divider=True),

                SectionComponent(
                    components=[
                        TextDisplayComponent(
                            content="-# We also offer some additional features for [players who upgrade their accounts](https://www.droptracker.io/account/upgrades).\n" +
                            "-# Please consider subscribing to support the continued development of the project.",
                        ),
                    ],
                    accessory=Button(
                        label="Upgrade",
                        style=ButtonStyle.LINK,
                        emoji=PartialEmoji(name="supporter", id=1263827303712948304),
                        url="https://www.droptracker.io/account/upgrades"
                    )
                ),
                SeparatorComponent(divider=True),
                InfoActionRow,
                SeparatorComponent(divider=True)
            ),
            
        ]

        components = welcome_page
        await channel.send(components=components)

