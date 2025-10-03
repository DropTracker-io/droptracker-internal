from io import IOBase
import json
import os
import random
import re
from secrets import token_hex
from data.submissions import try_create_player
from db.clan_sync import insert_xf_group
from interactions import AutocompleteContext, BaseContext, GuildText, Permissions, SlashCommand, UnfurledMediaItem, PartialEmoji, ActionRow, Button, ButtonStyle, SlashCommandOption, check, is_owner, Extension, slash_command, slash_option, SlashContext, Embed, OptionType, GuildChannel, SlashCommandChoice
from interactions.api.events import Startup, Component, ComponentCompletion, ComponentError, ModalCompletion, ModalError, MessageCreate
from interactions.models import ContainerComponent, ThumbnailComponent, SeparatorComponent, UserSelectMenu, SlidingWindowSystem, SectionComponent, SeparatorComponent, TextDisplayComponent, ThumbnailComponent, MediaGalleryComponent, MediaGalleryItem, OverwriteType
import interactions
import time
import subprocess
import platform
from db.models import GroupEmbed, GroupPatreon, GroupRecentDrops, NotificationQueue, NotifiedSubmission, NpcList, Session, User, Group, Guild, Player, Drop, Webhook, session, UserConfiguration, GroupConfiguration, user_group_association
from services import message_handler
from services.components import help_components
from services.points import award_points_to_player
from utils.format import format_time_since_update, format_number, get_command_id, get_npc_image_url, replace_placeholders
from utils.wiseoldman import check_user_by_id, check_user_by_username, check_group_by_id, fetch_group_members
from utils.redis import RedisClient
from db.ops import DatabaseOperations, associate_player_ids
from lootboard.generator import generate_server_board, generate_timeframe_board
from datetime import datetime, timedelta
import asyncio  
from sqlalchemy import text
redis_client = RedisClient()
db = DatabaseOperations()


# Commands for the general user to interact with the bot
class UserCommands(Extension):
    def __init__(self, bot: interactions.Client):
        self.bot = bot
        self.message_handler = bot.get_ext("services.message_handler")

    @slash_command(name="help",
                   description="View helpful commands/links for the DropTracker")
    
    async def help(self, ctx):
        user = session.query(User).filter_by(discord_id=ctx.user.id).first()
        if not user:
            await try_create_user(ctx=ctx)
        user = session.query(User).filter(User.discord_id == ctx.author.id).first()
        return await ctx.send(components=help_components, ephemeral=True)
    # @slash_command(name="global-board",
    #                description="View the current global loot leaderboard")
    # async def global_lootboard_cmd(self, ctx: SlashContext):
    #     embed = await db.get_group_embed(embed_type="lb", group_id=1)
    #     return await ctx.send(f"Here you are!", embeds=embed, ephemeral=True)
    #     pass

    @slash_command(name="dm-settings",
                   description="View or change your direct message settings")
    @slash_option(name="dm_type",
                  description="Select which type of direct message setting you want to edit",
                  required=True,
                  opt_type=OptionType.STRING,
                  autocomplete=True)
    @slash_option(name="toggle",
                  description="Select whether you want to enable or disable the direct message setting",
                  required=True,
                  opt_type=OptionType.STRING,
                  autocomplete=True)
    async def dm_settings_cmd(self, ctx: SlashContext, dm_type: str, toggle: str):
        def set_dm_config(user, config_keys, value):
            """Helper to set one or more config values for a user."""
            for config_key in config_keys:
                config_entry = session.query(UserConfiguration).filter(
                    UserConfiguration.user_id == user.user_id,
                    UserConfiguration.config_key == config_key
                ).first()
                if config_entry:
                    config_entry.config_value = value
                else:
                    # If config entry doesn't exist, create it
                    config_entry = UserConfiguration(
                        user_id=user.user_id,
                        config_key=config_key,
                        config_value=value
                    )
                    session.add(config_entry)

        user = session.query(User).filter_by(discord_id=str(ctx.user.id)).first()
        if not user:
            await try_create_user(ctx=ctx)
            user = session.query(User).filter(User.discord_id == str(ctx.user.id)).first()

        # Determine which config keys to update
        config_keys = []
        if dm_type == "updates":
            config_keys = ["dm_on_update_logs"]
        elif dm_type == "points":
            config_keys = ["dm_on_points_earned"]
        elif dm_type == "both":
            config_keys = ["dm_on_update_logs", "dm_on_points_earned"]

        value = "true" if toggle == "enable" else "false"
        set_dm_config(user, config_keys, value)
        session.commit()
        if dm_type == "both":
            desc_ext = "- Update logs\n- Points earned"
        elif dm_type == "updates":
            desc_ext = "- Update logs"
        elif dm_type == "points":
            desc_ext = "- Points earned"

        if toggle == "enable":
            embed = Embed(
                title="Success!",
                description=f"You have enabled direct-message notifications from me for:\n" + desc_ext
            )
        else:
            embed = Embed(
                title="Success!",
                description=f"You have disabled direct-message notifications from me for:\n" + desc_ext
            )
        await ctx.send(embed=embed, ephemeral=True)

    @dm_settings_cmd.autocomplete("dm_type")
    async def dm_settings_autocomplete_dm_type(self, ctx: AutocompleteContext):
        await ctx.send(
            choices=[
                {
                    "name": "Update Logs",
                    "value": "updates"
                },
                {
                    "name": "Points earned",
                    "value": "points"
                },
                {
                    "name": "Both",
                    "value": "both"
                }
            ]
        )

    @dm_settings_cmd.autocomplete("toggle")
    async def dm_settings_autocomplete_toggle(self, ctx: AutocompleteContext):
        await ctx.send(
            choices=[
                {
                    "name": "Enable",
                    "value": "enable"
                },
                {
                    "name": "Disable", 
                    "value": "disable"
                }
            ]
        )
    
    @slash_command(name="pingme",
                   description="Toggle whether or not you want to be pinged when your submissions are sent to Discord")
    @slash_option(name="type",
                  description="Select whether you want to toggle global, or clan-specific pings.",
                  required=True,
                  opt_type=OptionType.STRING,
                  autocomplete=True)
    async def pingme_cmd(self, ctx: SlashContext, type: str):
        user = session.query(User).filter_by(discord_id=str(ctx.user.id)).first()
        if not user:
            await try_create_user(ctx=ctx)
            user = session.query(User).filter(User.discord_id == ctx.author.id).first()
        if type == "global":
            user.global_ping = not user.global_ping
            session.commit()
            if user.global_ping:
                embed = Embed(title="Success!",
                              description=f"You will now be pinged when your submissions are sent to Discord.")
                await ctx.send(embed=embed, ephemeral=True)
            else:
                embed = Embed(title="Success!",
                              description=f"You will **no longer** be pinged when your submissions are sent to Discord.")
                await ctx.send(embed=embed, ephemeral=True)
        elif type == "group":
            user.group_ping = not user.group_ping
            session.commit()
            if user.group_ping:
                embed = Embed(title="Success!",
                              description=f"You will now be pinged when your submissions are sent to Discord.")
                await ctx.send(embed=embed, ephemeral=True)
            else:
                embed = Embed(title="Success!",
                              description=f"You will **no longer** be pinged when your submissions are sent to Discord.")
                await ctx.send(embed=embed, ephemeral=True)
        elif type == "everywhere":
            user.never_ping = not user.never_ping
            session.commit()
            if user.never_ping:
                embed = Embed(title="Success!",
                              description=f"You will **no longer** be pinged `anywhere` when your submissions are sent to Discord.")
                await ctx.send(embed=embed, ephemeral=True)
            else:
                embed = Embed(title="Success!",
                              description=f"You **will now be pinged** `anywhere` when your submissions are sent to Discord.")
                await ctx.send(embed=embed, ephemeral=True)
    @pingme_cmd.autocomplete("type")
    async def pingme_autocomplete_type(self, ctx: AutocompleteContext):
        string_in = ctx.input_text
        await ctx.send(
            choices=[
                {
                    "name": f"Globally",
                    "value": "global"
                },
                {
                    "name": f"In my group",
                    "value": "group"
                },
                {
                    "name": f"Everywhere",
                    "value": "everywhere"
                }
            ]
        )
    
    @slash_command(name="hideme",
                   description="Toggle whether or not you will appear anywhere in the global discord server / side panel / etc.")
    @slash_option(name="account",
                  description="Select which of your accounts you want to hide from our global listings (all for all).",
                  required=True,
                  opt_type=OptionType.STRING,
                  autocomplete=True)
    async def hideme_cmd(self, ctx: SlashContext, account: str):
        user = session.query(User).filter_by(discord_id=str(ctx.user.id)).first()
        if not user:
            await try_create_user(ctx=ctx)
            user = session.query(User).filter(User.discord_id == ctx.author.id).first()
        if account == "all":
            user.hidden = not user.hidden
            session.commit()
            if user.hidden:
                embed = Embed(title="Success!", 
                              description=f"All of your accounts will **no longer** be visible in our global listings.")
                return await ctx.send(embed=embed, ephemeral=True)
            else:
                embed = Embed(title="Success!",
                              description=f"All of your accounts will now **be visible** in our global listings.")
                return await ctx.send(embed=embed, ephemeral=True)
        else:
            player = session.query(Player).filter_by(player_name=account).first()
            if not player:
                return await ctx.send(f"You don't have any accounts by that name.", ephemeral=True)
            player.hidden = not player.hidden
            session.commit()
            if player.hidden:
                embed = Embed(title="Success!",
                              description=f"Your account, `{player.player_name}` will **no longer** be visible in our global listings.")
                return await ctx.send(embed=embed, ephemeral=True)
            else:
                embed = Embed(title="Success!",
                              description=f"Your account, `{player.player_name}` will now **be visible** in our global listings.")
                return await ctx.send(embed=embed, ephemeral=True)
            

    @hideme_cmd.autocomplete("account")
    async def hideme_autocomplete_account(self, ctx: AutocompleteContext):
        string_in = ctx.input_text
        user = session.query(User).filter_by(discord_id=str(ctx.user.id)).first()
        
        if not user:
            # User not found in database
            return await ctx.send(
                choices=[
                    {
                        "name": "All accounts",
                        "value": "all"
                    }
                ]
            )
        
        # Query for the user's accounts
        accounts = session.query(Player).filter_by(user_id=user.user_id).all()
        
        # Always include "All accounts" option
        choices = [
            {
                "name": "All accounts",
                "value": "all"
            }
        ]
        
        # Add player accounts if they exist
        if accounts:
            choices.extend([
                {
                    "name": account.player_name,
                    "value": account.player_name
                }
                for account in accounts
            ])
        
        return await ctx.send(choices=choices)
            
    @slash_command(name="accounts",
                   description="View your currently claimed RuneScape character names, if you have any")
    async def user_accounts_cmd(self, ctx):
        print("User accounts command...")
        user = session.query(User).filter_by(discord_id=str(ctx.user.id)).first()
        if not user:
            await try_create_user(ctx=ctx)
            user = session.query(User).filter(User.discord_id == ctx.author.id).first()
        accounts = session.query(Player).filter_by(user_id=user.user_id)
        account_names = ""
        count = 0
        if accounts:
            for account in accounts:
                count += 1
                last_updated_unix = format_time_since_update(account.date_updated)
                account_names += f"`" + account.player_name.strip() + f"` (id: {account.player_id})\n> Last updated: {last_updated_unix}\n"
        account_emb = Embed(title="Your Registered Accounts:",
                            description=f"{account_names}(total: `{count}`)")
        account_emb.add_field(name="/claim-rsn",value="To claim another, you can use the </claim-rsn:1269466219841327108> command.", inline=False)
        account_emb.set_footer(text="https://www.droptracker.io/")
        await ctx.send(embed=account_emb, ephemeral=True)
    
    @slash_command(name="claim-rsn",
                    description="Claim ownership of your RuneScape account names in the DropTracker database")
    @slash_option(name="rsn",
                  opt_type=OptionType.STRING,
                  description="Please type the in-game-name of the account you want to claim, **exactly as it appears**!",
                  required=True)
    async def claim_rsn_command(self, ctx, rsn: str):
        user = session.query(User).filter_by(discord_id=str(ctx.user.id)).first()
        group = None
        if not user:
            await try_create_user(ctx=ctx)
            user = session.query(User).filter(User.discord_id == ctx.author.id).first()
        if ctx.guild:
            guild_id = ctx.guild.id
            group = session.query(Group).filter(Group.guild_id.ilike(guild_id)).first()
        if not group:
            group = session.query(Group).filter_by(group_id=2).first()
        player = session.query(Player).filter(Player.player_name.ilike(rsn)).first()
        ## User should be made now
        if not player:
            try:
                wom_data = await check_user_by_username(rsn)
            except Exception as e:
                print("Couldn't get player data. e:", e)
                return await ctx.send(f"An error occurred claiming your account.\n" +
                                      "Try again later, or reach out in our Discord server",
                                      ephemeral=True)
            if wom_data:
                player, player_name, player_id, log_slots = wom_data
                try:
                    print("Creating a player with user ID", user.user_id, "associated with it")
                    ## We need to create the Player with a temporary acc hash for now
                    if group:
                        new_player = Player(wom_id=player_id, 
                                            player_name=rsn, 
                                            user_id=str(user.user_id), 
                                            user=user, 
                                            log_slots=log_slots,    
                                            group=group,
                                            account_hash=None)
                    else:
                        new_player = Player(wom_id=player_id, 
                                            player_name=rsn, 
                                            user_id=str(user.user_id), 
                                            log_slots=log_slots,
                                            account_hash=None,
                                            user=user)
                    session.add(new_player)
                    session.commit()
                    user_players = session.query(Player).filter(Player.user_id == user.user_id).all()
                    if len(user_players) == 1:
                        award_points_to_player(player_id=user_players[0].player_id, amount=10, source=f'Claimed account: {rsn}', expires_in_days=60)
                except Exception as e:
                    print(f"Could not create a new player:", e)
                    session.rollback()
                finally:
                    return await ctx.send(f"Your account ({player_name}), with ID `{player_id}` has " +
                                         "been added to the database & associated with your Discord account.",ephemeral=True)
            else:
                return await ctx.send(f"Your account was not found in the WiseOldMan database.\n" +
                                     f"You could try to manually update your account on their website by [clicking here](https://www.wiseoldman.net/players/{rsn}), then try again, or wait a bit.")
        else:
            joined_time = format_time_since_update(player.date_added)
            if player.user:
                user: User = player.user
                if str(user.discord_id) != str(ctx.user.id):
                    await ctx.send(f"Uh-oh!\n" +
                                f"It looks like somebody else may have claimed your account {joined_time}!\n" +
                                f"<@{player.user.discord_id}> (discord id: {player.user.discord_id}) currently owns it in our database.\n" + 
                                "If this is some type of mistake, please reach out in our discord server:\n" + 
                                "https://www.droptracker.io/discord",
                                ephemeral=True)
                else:
                    await ctx.send(f"It looks like you've already claimed this account ({player.player_name}) {joined_time}\n" + 
                                "\nSomething not seem right?\n" +
                                "Please reach out in our discord server:\n" + 
                                "https://www.droptracker.io/discord",
                                ephemeral=True)
            else:
                player.user = user
                session.commit()
                embed = Embed(title="Success!",
                              description=f"Your in-game name has been successfully associated with your Discord account.\n" +
                              "That's it!") 
                embed.add_field(name=f"What's next?",value=f"If you'd like, you can [register an account on our website] to stay informed " + 
                                "on updates & to make your voice heard relating to bugs & suggestions.",inline=False)
                embed.set_thumbnail(url="https://www.droptracker.io/img/droptracker-small.gif")
                embed.set_footer(text="Powered by the DropTracker | https://www.droptracker.io/")
                await ctx.send(embed=embed)

    @slash_command(name="dm-broken-groups",
                   description="Send a DM to administrators of groups that are not properly configured yet.",
                   default_member_permissions=Permissions.ADMINISTRATOR)
    async def dm_broken_groups(self, ctx: SlashContext):
        if str(ctx.user.id) != "528746710042804247":
            return await ctx.send("You are not authorized to use this command.", ephemeral=True)
        await ctx.defer(ephemeral=True)

        # ORM-based query to find guilds with broken configuration
        from db.models import Guild, GroupConfiguration

        # Subquery for lootboard_channel_id = '0'
        lootboard_subq = (
            session.query(GroupConfiguration.group_id)
            .filter(
                GroupConfiguration.config_key == 'lootboard_channel_id',
                GroupConfiguration.config_value == '0'
            )
            .subquery()
        )

        # Subquery for authed_users = '[]'
        authed_users_subq = (
            session.query(GroupConfiguration.group_id)
            .filter(
                GroupConfiguration.config_key == 'authed_users',
                GroupConfiguration.config_value == '[]'
            )
            .subquery()
        )

        # Intersect the two subqueries to get group_ids that match both
        broken_group_ids = (
            session.query(Guild.guild_id)
            .join(lootboard_subq, Guild.group_id == lootboard_subq.c.group_id)
            .join(authed_users_subq, Guild.group_id == authed_users_subq.c.group_id)
            .distinct()
            .all()
        )
        print("Got broken group ids:")
        print(broken_group_ids)

        async def create_dm_notice(bot: interactions.Client) -> Embed:
            embed_title = f"⚠️ **NOTICE** ⚠️"
            embed = interactions.Embed(
                title=embed_title,
                color=0x00ff00,
                timestamp=interactions.Timestamp.now()
            )
            description_parts = []
            description_parts.append(f"### :rotating_light: **Your registered group with the DropTracker has been flagged as improperly configured, or not set up at all.**")
            description_parts.append("You will have a total of 7 days from the time this message was sent to set our Discord bot up.")
            description_parts.append("-# __If you don't act before then__, **all of your group data will be wiped & the bot will leave your guild**!\n\n")
            description_parts.append("**If you need help:**")
            description_parts.append("- Join our [discord server](https://www.droptracker.io/discord)")
            description_parts.append(f"- Try the </help:{await get_command_id(bot, 'help')}> command")
            description_parts.append("You can also optionally remove our bot from your server now, if you decide you don't want to use it.")
            description_parts.append("**-# We contacted you because you were the owner of the discord guild we were added to.\nThank you for your time!**")
            embed.description = "\n".join(description_parts).strip()
            embed.set_footer(text=f"Powered by the DropTracker | https://www.droptracker.io/", icon_url="https://www.droptracker.io/img/droptracker-small.gif")
            return embed

        # If you want to test with a specific guild, uncomment the next line
        # broken_group_ids = [(1034567162116972575,)]

        for guild_id_tuple in broken_group_ids:
            guild_id = guild_id_tuple[0]
            # Remove the override below to use real guild_id
            guild = await ctx.bot.fetch_guild(guild_id)
            if guild:
                continue ## TODO - dont continue if guild is found once we delete old data
                try:
                    #await ctx.channel.send(f"Got guild owner - <@{guild._owner_id}>")
                    guild_owner = await self.bot.fetch_user(guild._owner_id)
                    await guild_owner.send(content=f"## Hey, <@{guild._owner_id}>!",embed=await create_dm_notice(ctx.bot))
                    #await ctx.channel.send(f"Sent DM to guild owner - <@{guild._owner_id}>")
                except Exception as e:
                    #await ctx.channel.send(f"Couldn't send DM to guild owner - <@{guild._owner_id}>")
                    print("Couldn't send DM to guild owner:", e)
            # Remove break to process all guilds, or keep for only one
            else:
                try:
                    group_id_row = session.query(Guild.group_id).filter(Guild.guild_id == guild_id).first()
                    group_id = group_id_row[0] if group_id_row else None
                    if not group_id:
                        continue
                    from sqlalchemy import delete
                    # Prevent premature autoflush while we clean up
                    with session.no_autoflush:
                        # Delete association/dependent rows first
                        session.execute(
                            delete(user_group_association).where(
                                user_group_association.c.group_id == group_id
                            )
                        )
                        session.execute(delete(NotificationQueue).where(NotificationQueue.group_id == group_id))
                        session.execute(delete(NotifiedSubmission).where(NotifiedSubmission.group_id == group_id))
                        session.execute(delete(GroupEmbed).where(GroupEmbed.group_id == group_id))
                        session.execute(delete(GroupPatreon).where(GroupPatreon.group_id == group_id))
                        session.execute(delete(GroupRecentDrops).where(GroupRecentDrops.group_id == group_id))
                        # Also remove group configuration to avoid FK updates to NULL on flush
                        session.execute(delete(GroupConfiguration).where(GroupConfiguration.group_id == group_id))

                        # Now delete ORM parents
                        group = session.query(Group).filter(Group.guild_id == guild_id).first()
                        if group:
                            session.delete(group)
                        guild_obj = session.query(Guild).filter(Guild.guild_id == guild_id).first()
                        if guild_obj:
                            session.delete(guild_obj)
                    session.commit()
                    await ctx.channel.send(f"Guild with id `{guild_id}` not found & is likely safe to be removed.")
                except Exception as e:
                    session.rollback()
                    await ctx.channel.send(f"Cleanup failed for guild `{guild_id}`: {e}")
        ## TODO - remove hard coded test
    
    
        
    
    
    # @slash_command(
    #     name="force_msg",
    #     description="Force a re-processing of a webhook message",
    #     default_member_permissions=Permissions.ADMINISTRATOR,
    # )
    # @slash_option(
    #     name="message_id",
    #     description="The message ID to re-process",
    #     opt_type=OptionType.STRING,
    #     required=True
    # )
    # @slash_option(
    #     name="channel_id",
    #     description="The channel ID the message is inside of",
    #     opt_type=OptionType.STRING,
    #     required=True
    # )
    # async def force_msg(self, ctx: SlashContext, channel_id: str, message_id: str):
    #     await ctx.send("Force message re-processing initiated.")
    #     #await message_data_logger.log("force_msg", {"message_id": ctx.message.id, "channel_id": ctx.channel.id})
        
    #     channel = await ctx.bot.fetch_channel(channel_id)
    #     message = await channel.fetch_message(message_id)
    #     if message:
    #         try:
    #             print("Re-processing message...")
    #             if message.embeds:
    #                 for embed in message.embeds:
    #                     for field in embed.fields:
    #                         if field.name == "player":
    #                             field.value = "joelhalen"
    #                         elif field.name == "acc_hash":
    #                             field.value = "-3718503131431628598"
    #             #await self.message_handler.on_message_create(self.message_handler, message)
    #         except Exception as e:
    #             print("Error re-processing message:", e)
    #             await ctx.send(f"Error re-processing message: {e}")
    #     else:
    #         await ctx.send("Message not found.")


    @slash_command(name="new_webhook",
                    description="Generate a new webhook, adding it to the database and the GitHub list.",
                    default_member_permissions=Permissions.ADMINISTRATOR)
    async def new_webhook_generator(self, ctx: SlashContext):
        if not str(ctx.user.id) == "528746710042804247":
            return await ctx.send("You are not authorized to use this command.", ephemeral=True)
        await ctx.defer(ephemeral=True)
        for i in range(30):
            with Session() as session:
                main_parent_ids = [1332506635775770624, 1332506742801694751, 1369779266945814569, 1369779329382482005, 1369803376598192128]
                hooks_parent_ids = [1332506904840372237, 1332506935886348339, 1369779098246975638, 1369779125035991171]
                hooks_2_parent_ids = [1369777536975900773, 1369777572577284167, 1369778911264641034, 1369778925919670432, 1369778911264641034]
                hooks_3_parent_ids = [1369780179064590418, 1369780228930670705, 1369780244583547073, 1369780261000183848, 1369780569080332369]

                all_parent_ids = main_parent_ids + hooks_parent_ids + hooks_2_parent_ids + hooks_3_parent_ids
                try:
                    parent_id = random.choice(all_parent_ids)
                    parent_channel = await ctx.bot.fetch_channel(parent_id)
                    num = 35
                    channel_name = f"drops-{num}"
                    while channel_name in [channel.name for channel in parent_channel.channels]:
                        num += 1
                        channel_name = f"drops-{num}"
                    new_channel: GuildText = await parent_channel.create_text_channel(channel_name)
                    logo_path = '/store/droptracker/disc/static/assets/img/droptracker-small.gif'
                    avatar = interactions.File(logo_path)
                    webhook: interactions.Webhook = await new_channel.create_webhook(name=f"DropTracker Webhooks ({num})", avatar=avatar)
                    webhook_url = webhook.url
                    db_webhook = Webhook(webhook_id=str(webhook.id), webhook_url=str(webhook_url))
                    session.add(db_webhook)
                    session.commit()
                except Exception as e:
                    await ctx.send(f"Couldn't create a new webhook:{e}",ephemeral=True)
            pass
        print("Created 30 new webhooks.")

## Auth-related functions ##
async def is_admin(ctx: BaseContext):
    perms_value = ctx.author.guild_permissions.value
    print("Guild permissions:", perms_value)
    if perms_value & 0x00000008:  # 0x8 is the bit flag for administrator
        return True
    return False

def is_user_authorized(user_id, group: Group):
    # Check if the user is an admin or an authorized user for this group
    group_config = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == group.group_id).all()
    # Transform group_config into a dictionary for easy access
    config = {conf.config_key: conf.config_value for conf in group_config}
    authed_user = False
    user_data: User = session.query(User).filter(User.user_id == user_id).first()
    if user_data:
        discord_id = user_data.discord_id
    else:
        return False
    if "authed_users" in config:
        authed_users = config["authed_users"]
        if isinstance(authed_users, int):
            authed_users = f"{authed_users}"  # Get the list of authorized user IDs
        print("Authed users:", authed_users)
        authed_users = json.loads(authed_users)
        # Loop over authed_users and check if the current user is authorized
        for authed_id in authed_users:
            if str(authed_id) == str(discord_id):  # Compare the authed_id with the current user's ID
                authed_user = True
                return True  # Exit the loop once the user is found
    return authed_user


# Commands that help configure or change clan-specifics.
class ClanCommands(Extension):
    @slash_command(name="create-group",
                    description="Create a new group with the DropTracker",
                    default_member_permissions=Permissions.ADMINISTRATOR)
    @slash_option(name="group_name",
                  opt_type=OptionType.STRING,
                  description="How would you like your group's name to appear?",
                  required=True)
    @slash_option(name="wom_id",
                  opt_type=OptionType.STRING,
                  description="Enter your group's WiseOldMan group ID",
                  max_length=6,
                  min_length=3,
                  required=True)
    async def create_group_cmd(self, 
                               ctx: SlashContext, 
                               group_name: str,
                               wom_id: str):
        try:
            wom_id = int(wom_id)
        except Exception as e:
            pass
        if not ctx.guild_id:
            return await ctx.send(f"You must use this command in a Discord server")
        if ctx.author_permissions.ALL:
            print("Comparing:")
            user = session.query(User).filter(User.discord_id == ctx.author.id).first()
            if not user:
                await try_create_user(ctx=ctx)
            user = session.query(User).filter(User.discord_id == ctx.author.id).first()
            guild = session.query(Guild).filter(Guild.guild_id == ctx.guild_id).first()
            if not guild:
                guild = Guild(guild_id=str(ctx.guild_id),
                                  date_added=datetime.now())
                session.add(guild)
                session.commit()
            else:
                if guild.group_id != None:
                    group = session.query(Group).filter(Group.group_id == guild.group_id).first()
                    if group and group.wom_id == wom_id:
                        return await ctx.send(f"You have already registered this group with the DropTracker! Please continue to [the website](https://www.droptracker.io/groups/{group.group_id}) to configure your group.")
                    ## Otherwise the group wom id was different than the one they provided
                    return await ctx.send(f"This Discord server is already associated with a DropTracker group (using wom id {group.wom_id}).\n" + 
                                        "If this is a mistake, please reach out in Discord", ephemeral=True)
        
            group = session.query(Group).filter(Group.wom_id == wom_id).first()
            if group:
                return await ctx.send(f"This WOM group (`{wom_id}`) already exists in our database.\n" + 
                                    "Please reach out in our Discord server if this appears to be a mistake.",
                                    ephemeral=True)
            else:
                # Create the group but don't commit yet
                group = Group(group_name=group_name,
                            wom_id=wom_id,
                            guild_id=guild.guild_id)
                session.add(group)
                print("Created a group")
                user.add_group(group)
                
                # Initialize these variables with defaults
                total_members = 0
                try:
                    session.commit()
                    print(f"Successfully committed group {group.group_id}")
                except Exception as e:
                    session.rollback()
                    print(f"Failed to commit group: {e}")
                    return await ctx.send(f"Unable to create your group due to a database error.\n" + 
                                        f"Please try again later or reach out in the DropTracker Discord server.",
                                        ephemeral=True)
                    
            # Now assign the group_id to the guild and commit
            guild.group_id = group.group_id
            try:
                session.commit()
                print(f"Successfully linked guild {guild.guild_id} to group {group.group_id}")
            except Exception as e:
                session.rollback()
                print(f"Error linking guild to group: {e}")
                
            # Create success embed with proper variable handling
            try:
                embed = Embed(title="New group created",
                            description=f"Your group has been created (ID: `{group.group_id}`)!")
                embed.add_field(name=f"WOM group `{group.wom_id}` (`{total_members}` members) is now assigned to your Discord server `{group.guild_id}`",
                                value=f"<a:loading:1180923500836421715> Please wait while we initialize some other things for you...",
                                inline=False)
            except NameError as e:
                print(f"Variable error in embed creation: {e}")
                # Fallback embed
                embed = Embed(title="New group created",
                            description=f"Your Group has been created (ID: `{group.group_id}`).")
                embed.add_field(name=f"WOM group `{group.wom_id}` is now assigned to your Discord server",
                                value=f"<a:loading:1180923500836421715> Please wait while we initialize some other things for you...",
                                inline=False)
            embed.set_footer(f"https://www.droptracker.io/discord")
            try:
                await insert_xf_group(group)
            except Exception as e:
                print(f"Error inserting group into XenForo: {e}")
            await ctx.send(f"Success!\n",embed=embed,
                                            ephemeral=True)
            default_config = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == 1).all()
            ## grab the default configuration options from the database
            new_config = []
            for option in default_config:
                option_value = option.config_value
                if option.config_key == "clan_name":
                    option_value = group_name
                if option.config_key == "authed_users":
                    authed_list = []
                    authed_list.append(str(ctx.author_id))
                    # Format as a proper JSON array of strings
                    option_value = json.dumps(authed_list)
                default_option = GroupConfiguration(
                    group_id=group.group_id,
                    config_key=option.config_key,
                    config_value=option_value,
                    updated_at=datetime.now(),
                    group=group
                )
                new_config.append(default_option)
            try:
                session.add_all(new_config)
                session.commit()
                print(f"Successfully created {len(new_config)} configuration entries for group {group.group_id}")
            except Exception as e:
                session.rollback()
                print("Error occured trying to save configs::", e)
                # Don't return here - group is already created, just warn about configs
                await ctx.send(f"⚠️ Your group was created successfully, but there was an issue setting up default configurations.\n" + 
                              f"Please visit the website to configure your group settings manually: https://www.droptracker.io/login",
                              ephemeral=True)
                    # send_email(subject=f"New Group: {group_name}",
                    #         recipients=["support@droptracker.io"],
                    #         body=f"A new group was registered in the database." + 
                    #                 f"\nDropTracker Group ID: {group.group_id}\n" + 
                    #                 f"Discord server ID: {str(ctx.guild_id)}")
            await asyncio.sleep(5)
            await ctx.send(f"To continue setting up, please [sign in on the website](https://www.droptracker.io/login) using your Discord account.",
                            ephemeral=True)
        else:
            await ctx.send(f"You do not have the necessary permissions to use this command inside of this Discord server.\n" + 
                           "Please ask the server owner to execute this command.",
                           ephemeral=True)

    @slash_command(name="send_player_faq",
                   description="Send a message from the DropTracker bot to help outline some player FAQs.",
                   default_member_permissions=interactions.Permissions.ADMINISTRATOR)
    async def send_player_faq_cmd(self, ctx: SlashContext):
        logo_media = UnfurledMediaItem(
            url="https://www.droptracker.io/img/droptracker-small.gif"
        )
        player_setup = [
            ContainerComponent(
                SeparatorComponent(divider=True),
                TextDisplayComponent(
                    content="## Player FAQs - DropTracker.io",
                ),
                SeparatorComponent(divider=True),
                SectionComponent(
                    components=[
                        TextDisplayComponent(
                            content="-# **What is the DropTracker?**\n" +
                            "-# > A community-driven, all-in-one loot and achievement tracking system built for Old School RuneScape groups.\n" +
                            "-# > We leverage the *[WiseOldMan](https://wiseoldman.net)* to manage group memberships, and provide group leaders a seamless way to configure their group's achievement notification settings.\n\n" +
                            "-# **How do I get started?**\n" +
                            "-# > 1. Install the **DropTracker** plugin on your RuneLite client, via the plugin hub.\n" +
                            "-# > 2. Visit the plugin settings panel (gear tab on RuneLite side panel) to configure which achievements you *personally* want tracked.\n" +
                            "-# > 3. (Optionally) Claim your in-game-name using the </claim-rsn:1369493380358209537> command to associate your Discord account with your character(s).\n\n" +
                            "-# **How can I get pinged when my account(s) have notifications sent?**\n" +
                            "-# > Using the </claim-rsn:1369493380358209537> command, entering your in-game-name **exactly as it appears**.\n\n" +
                            "-# **How can I prevent my submissions from being shared to the global DropTracker discord channels?**\n" +
                            "-# > Using the </hideme:1369493380358209544> command, and selecting which account(s)/context(s) you want to be hidden from.\n\n" +
                            "-# **How can I get (or not get) pinged by the <@1172933457010245762> bot when my account(s) have notifications sent?**\n" +
                            "-# > Using the </pingme:1369493380358209541> command, and selecting which account(s)/context(s) you do or do not want to receive pings for.\n\n" +
                            "-# **What types of information does the DropTracker store about me and my account(s)?**\n" +
                            "-# 1. Your account(s) unique identifier, or 'account hash'. This is provided by Jagex, and is unique to each individual character; remaining consistent thru name changes.\n" +
                            "-# 2. Your submitted achievements/drops.\n\n" +
                            "-# 3. Your Discord ID (if you claim your account or execute commands through our bot)\n\n" +
                            "-# **What can I do to support the continued development of the DropTracker project?**\n\n" +
                            "-# This passion project began as something far more simple, and has continued to evolve into what you see before you today.\n" + 
                            "-# Without the continued support of our premium groups, the development work we do would be impossible.\n" +
                            "-# If you feel as though we've provided a notable value to your OSRS experience, feel free to show support through our [Patreon](https://www.patreon.com/droptracker).\n" +
                            "-# Players who have subscribed and then upgraded their groups using that subscription are provided early access to new features, alongside a few premium-only functionalities."
                        )
                    ],
                    accessory=ThumbnailComponent(
                        media=logo_media
                    )
                ),
                SeparatorComponent(divider=True),
            )
        ]
        await ctx.channel.send(components=player_setup)



async def try_create_user(discord_id: str = None, username: str = None, ctx: SlashContext = None):
    if discord_id == None and username == None:
        if ctx:
            username = ctx.user.username
            discord_id = ctx.user.id
    user = None
    try:
        group = None
        if ctx:
            if ctx.guild_id:
                guild_ob = session.query(Guild).filter(Guild.guild_id == ctx.guild_id).first()
                if guild_ob:
                    group = session.query(Group).filter(Group.group_id == guild_ob.group_id).first()
        if group:
            new_user: User = User(auth_token="", discord_id=str(discord_id), username=str(username), groups=[group])
        else:
            new_user: User = User(auth_token="", discord_id=str(discord_id), username=str(username))
        if new_user:
            session.add(new_user)
            session.commit()

    except Exception as e:
        print("An error occured trying to add a new user to the database:", e)
        if ctx:
            return await ctx.author.send(f"An error occurred attempting to register your account in the database.\n" + 
                                    f"Please reach out for help: https://www.droptracker.io/discord",ephemeral=True)
    default_config = session.query(UserConfiguration).filter(UserConfiguration.user_id == 1).all()
    ## grab the default configuration options from the database
    if new_user:
        user = new_user
    if not user:
        user = session.query(User).filter(User.discord_id == discord_id).first()

    new_config = []
    for option in default_config:
        option_value = option.config_value
        default_option = UserConfiguration(
            user_id=user.user_id,
            config_key=option.config_key,
            config_value=option_value,
            updated_at=datetime.now()
        )
        new_config.append(default_option)
    try:
        session.add_all(new_config)
        session.commit()
    except Exception as e:
        session.rollback()
    try:
        droptracker_guild: interactions.Guild = await ctx.bot.fetch_guild(guild_id=1172737525069135962)
        dt_member = droptracker_guild.get_member(member_id=discord_id)
        if dt_member:
            registered_role = droptracker_guild.get_role(role_id=1210978844190711889)
            await dt_member.add_role(role=registered_role)
    except Exception as e:
        print("Couldn't add the user to the registered role:", e)
    # xf_user = await xf_api.try_create_xf_user(discord_id=str(discord_id),
    #                                 username=username,
    #                                 auth_key=str(auth_token))
    # if xf_user:
    #     user.xf_user_id = xf_user['user_id']
    session.commit()
    if ctx:
        claim_rsn_cmd_id = await get_command_id(ctx.bot, 'claim-rsn')
        cmd_id = str(claim_rsn_cmd_id)
        if str(ctx.command_id) != cmd_id:
            reg_embed=Embed(title="Account Registered",
                                 description=f"Your account has been created. (DT ID: `{user.user_id}`)")
            reg_embed.add_field(name="Please claim your accounts!",
                                value=f"The next thing you should do is " + 
                                f"use </claim-rsn:{await get_command_id(ctx.bot, 'claim-rsn')}>" + 
                                "for each of your in-game names, so you can associate them with your Discord account.",
                                inline=False)
            reg_embed.add_field(name="Change your configuration settings:",
                                value=f"Feel free to visit the website to configure privacy settings related to your drops & more",
                                inline=False)
            await ctx.send(embed=reg_embed,ephemeral=True)
            reg_embed=Embed(title="Account Registered",
                                 description=f"Your account has been created. (DT ID: `{user.user_id}`)")
            reg_embed.add_field(name="Change your configuration settings:",
                                value=f"Feel free to [sign in on the website](https://www.droptracker.io/) to configure your user settings.",
                                inline=False)
            return await ctx.send(embed=reg_embed)
        else:
            reg_embed=Embed(title="Account Registered",
                                 description=f"Your account has been created. (DT ID: `{user.user_id}`)")
            reg_embed.add_field(name="Change your configuration settings:",
                                value=f"Feel free to [sign in on the website](https://www.droptracker.io/) to configure your user settings.",
                                inline=False)
            await ctx.author.send(embed=reg_embed)
            return True
            
            

async def get_external_latency():
        host = "amazon.com"
        ping_command = ["ping", "-c", "1", host]

        try:
            output = subprocess.check_output(ping_command, stderr=subprocess.STDOUT, universal_newlines=True)
            if "time=" in output:
                ext_latency_ms = output.split("time=")[-1].split(" ")[0]
                return ext_latency_ms
        except subprocess.CalledProcessError:
            return "N/A"  

        return "N/A"