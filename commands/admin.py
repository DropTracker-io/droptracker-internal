"""
Admin Commands Module

Contains Discord slash commands that require administrator permissions.
These commands handle group management, webhooks, and administrative functions.

Classes:
    ClanCommands: Extension containing admin-level slash commands

Author: joelhalen
"""

import json
import random
import asyncio
from datetime import datetime
from interactions import (
    SlashContext, Embed, OptionType, Extension, slash_command, slash_option,
    Permissions, GuildText, UnfurledMediaItem, ContainerComponent, SeparatorComponent,
    TextDisplayComponent, SectionComponent, ThumbnailComponent, File
)
from sqlalchemy import delete
from db.clan_sync import insert_xf_group
from db.models import (
    Session, User, Group, Guild, GroupConfiguration, GroupEmbed, GroupPatreon, 
    GroupRecentDrops, NotificationQueue, NotifiedSubmission, Webhook, 
    user_group_association, session
)
from utils.format import get_command_id
from .utils import try_create_user


class ClanCommands(Extension):
    """
    Extension containing administrator-level Discord slash commands.
    
    This extension provides commands that require administrator permissions
    and handle group management, webhook creation, and other admin functions.
    """

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
    async def create_group_cmd(self, ctx: SlashContext, group_name: str, wom_id: str):
        """
        Create a new DropTracker group linked to a Discord server.
        
        Creates a new group entry in the database, links it to the current Discord server,
        and sets up default configurations. Requires administrator permissions.
        
        Args:
            ctx (SlashContext): The slash command context
            group_name (str): Display name for the group
            wom_id (str): Wise Old Man group ID (3-6 digits)
        """
        try:
            wom_id = int(wom_id)
        except Exception as e:
            pass
            
        if not ctx.guild_id:
            return await ctx.send(f"You must use this command in a Discord server")
            
        if ctx.author_permissions.ALL:
            user = session.query(User).filter(User.discord_id == ctx.author.id).first()
            if not user:
                await try_create_user(ctx=ctx)
            user = session.query(User).filter(User.discord_id == ctx.author.id).first()
            
            guild = session.query(Guild).filter(Guild.guild_id == ctx.guild_id).first()
            if not guild:
                guild = Guild(guild_id=str(ctx.guild_id), date_added=datetime.now())
                session.add(guild)
                session.commit()
            else:
                if guild.group_id != None:
                    group = session.query(Group).filter(Group.group_id == guild.group_id).first()
                    if group and group.wom_id == wom_id:
                        return await ctx.send(f"You have already registered this group with the DropTracker! Please continue to [the website](https://www.droptracker.io/groups/{group.group_id}) to configure your group.")
                    # Otherwise the group wom id was different than the one they provided
                    return await ctx.send(f"This Discord server is already associated with a DropTracker group (using wom id {group.wom_id}).\\n" + 
                                        "If this is a mistake, please reach out in Discord", ephemeral=True)
        
            group = session.query(Group).filter(Group.wom_id == wom_id).first()
            if group:
                return await ctx.send(f"This WOM group (`{wom_id}`) already exists in our database.\\n" + 
                                    "Please reach out in our Discord server if this appears to be a mistake.",
                                    ephemeral=True)
            else:
                # Create the group but don't commit yet
                group = Group(group_name=group_name, wom_id=wom_id, guild_id=guild.guild_id)
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
                    return await ctx.send(f"Unable to create your group due to a database error.\\n" + 
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
                embed.add_field(name=f"WOM group `{group.wom_id}` is now assigned to your Discord server `{group.guild_id}`",
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
                
            await ctx.send(f"Success!\\n", embed=embed, ephemeral=True)
            
            # Set up default configurations
            default_config = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == 1).all()
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
                print("Error occured trying to save configs:", e)
                # Don't return here - group is already created, just warn about configs
                await ctx.send(f"⚠️ Your group was created successfully, but there was an issue setting up default configurations.\\n" + 
                              f"Please visit the website to configure your group settings manually: https://www.droptracker.io/login",
                              ephemeral=True)
                              
            await asyncio.sleep(5)
            await ctx.send(f"To continue setting up, please [sign in on the website](https://www.droptracker.io/login) using your Discord account.",
                            ephemeral=True)
        else:
            await ctx.send(f"You do not have the necessary permissions to use this command inside of this Discord server.\\n" + 
                           "Please ask the server owner to execute this command.",
                           ephemeral=True)

    @slash_command(name="dm-broken-groups",
                   description="Send a DM to administrators of groups that are not properly configured yet.",
                   default_member_permissions=Permissions.ADMINISTRATOR)
    async def dm_broken_groups(self, ctx: SlashContext):
        """
        Send DMs to administrators of improperly configured groups.
        
        Identifies groups with broken configurations and sends warning messages
        to their Discord server owners. Restricted to bot owner only.
        
        Args:
            ctx (SlashContext): The slash command context
        """
        if str(ctx.user.id) != "528746710042804247":
            return await ctx.send("You are not authorized to use this command.", ephemeral=True)
        await ctx.defer(ephemeral=True)

        # ORM-based query to find guilds with broken configuration
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
        print("Got broken group ids:", broken_group_ids)

        async def create_dm_notice(bot) -> Embed:
            """Create the DM notice embed for broken groups."""
            embed_title = f"⚠️ **NOTICE** ⚠️"
            embed = Embed(title=embed_title, color=0x00ff00, timestamp=datetime.now())
            
            description_parts = []
            description_parts.append(f"### :rotating_light: **Your registered group with the DropTracker has been flagged as improperly configured, or not set up at all.**")
            description_parts.append("You will have a total of 7 days from the time this message was sent to set our Discord bot up.")
            description_parts.append("-# __If you don't act before then__, **all of your group data will be wiped & the bot will leave your guild**!\\n\\n")
            description_parts.append("**If you need help:**")
            description_parts.append("- Join our [discord server](https://www.droptracker.io/discord)")
            description_parts.append(f"- Try the </help:{await get_command_id(bot, 'help')}> command")
            description_parts.append("You can also optionally remove our bot from your server now, if you decide you don't want to use it.")
            description_parts.append("**-# We contacted you because you were the owner of the discord guild we were added to.\\nThank you for your time!**")
            embed.description = "\\n".join(description_parts).strip()
            embed.set_footer(text=f"Powered by the DropTracker | https://www.droptracker.io/", icon_url="https://www.droptracker.io/img/droptracker-small.gif")
            return embed

        for guild_id_tuple in broken_group_ids:
            guild_id = guild_id_tuple[0]
            guild = await ctx.bot.fetch_guild(guild_id)
            if guild:
                continue  # TODO - don't continue if guild is found once we delete old data
                try:
                    guild_owner = await self.bot.fetch_user(guild._owner_id)
                    await guild_owner.send(content=f"## Hey, <@{guild._owner_id}>!", embed=await create_dm_notice(ctx.bot))
                except Exception as e:
                    print("Couldn't send DM to guild owner:", e)
            else:
                try:
                    group_id_row = session.query(Guild.group_id).filter(Guild.guild_id == guild_id).first()
                    group_id = group_id_row[0] if group_id_row else None
                    if not group_id:
                        continue
                        
                    # Prevent premature autoflush while we clean up
                    with session.no_autoflush:
                        # Delete association/dependent rows first
                        session.execute(delete(user_group_association).where(user_group_association.c.group_id == group_id))
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

    @slash_command(name="new_webhook",
                    description="Generate a new webhook, adding it to the database and the GitHub list.",
                    default_member_permissions=Permissions.ADMINISTRATOR)
    async def new_webhook_generator(self, ctx: SlashContext):
        """
        Generate new webhooks for the DropTracker system.
        
        Creates 30 new Discord webhooks across various channels and adds them
        to the database for use by the DropTracker system. Bot owner only.
        
        Args:
            ctx (SlashContext): The slash command context
        """
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
                    avatar = File(logo_path)
                    webhook = await new_channel.create_webhook(name=f"DropTracker Webhooks ({num})", avatar=avatar)
                    webhook_url = webhook.url
                    db_webhook = Webhook(webhook_id=str(webhook.id), webhook_url=str(webhook_url))
                    session.add(db_webhook)
                    session.commit()
                except Exception as e:
                    await ctx.send(f"Couldn't create a new webhook:{e}", ephemeral=True)
        print("Created 30 new webhooks.")

    @slash_command(name="send_player_faq",
                   description="Send a message from the DropTracker bot to help outline some player FAQs.",
                   default_member_permissions=Permissions.ADMINISTRATOR)
    async def send_player_faq_cmd(self, ctx: SlashContext):
        """
        Send a comprehensive FAQ message for players.
        
        Posts a detailed FAQ message with information about the DropTracker,
        how to get started, and common questions. Admin only.
        
        Args:
            ctx (SlashContext): The slash command context
        """
        logo_media = UnfurledMediaItem(url="https://www.droptracker.io/img/droptracker-small.gif")
        
        player_setup = [
            ContainerComponent(
                SeparatorComponent(divider=True),
                TextDisplayComponent(content="## Player FAQs - DropTracker.io"),
                SeparatorComponent(divider=True),
                SectionComponent(
                    components=[
                        TextDisplayComponent(
                            content="-# **What is the DropTracker?**\\n" +
                            "-# > A community-driven, all-in-one loot and achievement tracking system built for Old School RuneScape groups.\\n" +
                            "-# > We leverage the *[WiseOldMan](https://wiseoldman.net)* to manage group memberships, and provide group leaders a seamless way to configure their group's achievement notification settings.\\n\\n" +
                            "-# **How do I get started?**\\n" +
                            "-# > 1. Install the **DropTracker** plugin on your RuneLite client, via the plugin hub.\\n" +
                            "-# > 2. Visit the plugin settings panel (gear tab on RuneLite side panel) to configure which achievements you *personally* want tracked.\\n" +
                            "-# > 3. (Optionally) Claim your in-game-name using the </claim-rsn:1369493380358209537> command to associate your Discord account with your character(s).\\n\\n" +
                            "-# **How can I get pinged when my account(s) have notifications sent?**\\n" +
                            "-# > Using the </claim-rsn:1369493380358209537> command, entering your in-game-name **exactly as it appears**.\\n\\n" +
                            "-# **How can I prevent my submissions from being shared to the global DropTracker discord channels?**\\n" +
                            "-# > Using the </hideme:1369493380358209544> command, and selecting which account(s)/context(s) you want to be hidden from.\\n\\n" +
                            "-# **How can I get (or not get) pinged by the <@1172933457010245762> bot when my account(s) have notifications sent?**\\n" +
                            "-# > Using the </pingme:1369493380358209541> command, and selecting which account(s)/context(s) you do or do not want to receive pings for.\\n\\n" +
                            "-# **What types of information does the DropTracker store about me and my account(s)?**\\n" +
                            "-# 1. Your account(s) unique identifier, or 'account hash'. This is provided by Jagex, and is unique to each individual character; remaining consistent thru name changes.\\n" +
                            "-# 2. Your submitted achievements/drops.\\n\\n" +
                            "-# 3. Your Discord ID (if you claim your account or execute commands through our bot)\\n\\n" +
                            "-# **What can I do to support the continued development of the DropTracker project?**\\n\\n" +
                            "-# This passion project began as something far more simple, and has continued to evolve into what you see before you today.\\n" + 
                            "-# Without the continued support of our premium groups, the development work we do would be impossible.\\n" +
                            "-# If you feel as though we've provided a notable value to your OSRS experience, feel free to show support through our [Patreon](https://www.patreon.com/droptracker).\\n" +
                            "-# Players who have subscribed and then upgraded their groups using that subscription are provided early access to new features, alongside a few premium-only functionalities."
                        )
                    ],
                    accessory=ThumbnailComponent(media=logo_media)
                ),
                SeparatorComponent(divider=True),
            )
        ]
        await ctx.channel.send(components=player_setup)
