"""
Command Utilities Module

Contains utility functions and helpers used by various command modules.
Includes authentication helpers, user creation functions, and common utilities.

Functions:
    try_create_user: Create a new user in the database
    is_admin: Check if user has administrator permissions
    is_user_authorized: Check if user is authorized for a specific group
    get_external_latency: Get external network latency

Author: joelhalen
"""

import json
import subprocess
from datetime import datetime
from interactions import SlashContext, Embed, BaseContext
from db.models import Session, User, Group, Guild, UserConfiguration, session
from services.points import award_points_to_player
from utils.format import get_command_id


async def try_create_user(discord_id: str = None, username: str = None, ctx: SlashContext = None):
    """
    Create a new user in the database with default configurations.
    
    Creates a User entry with associated UserConfiguration entries based on
    default settings. Also handles group association if the user is in a
    registered Discord server.
    
    Args:
        discord_id (str, optional): Discord user ID. Defaults to None.
        username (str, optional): Discord username. Defaults to None.
        ctx (SlashContext, optional): Slash command context. Defaults to None.
        
    Returns:
        bool: True if user creation was successful
        
    Note:
        If ctx is provided, discord_id and username will be extracted from it.
        The function also attempts to add the user to a "registered" role in
        the main DropTracker Discord server.
    """
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
            return await ctx.author.send(f"An error occurred attempting to register your account in the database.\\n" + 
                                    f"Please reach out for help: https://www.droptracker.io/discord",ephemeral=True)
    default_config = session.query(UserConfiguration).filter(UserConfiguration.user_id == 1).all()
    # grab the default configuration options from the database
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
        droptracker_guild = await ctx.bot.fetch_guild(guild_id=1172737525069135962)
        dt_member = droptracker_guild.get_member(member_id=discord_id)
        if dt_member:
            registered_role = droptracker_guild.get_role(role_id=1210978844190711889)
            await dt_member.add_role(role=registered_role)
    except Exception as e:
        print("Couldn't add the user to the registered role:", e)
    
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


async def is_admin(ctx: BaseContext):
    """
    Check if the user has administrator permissions in the current guild.
    
    Args:
        ctx (BaseContext): The command context
        
    Returns:
        bool: True if user has administrator permissions, False otherwise
    """
    perms_value = ctx.author.guild_permissions.value
    print("Guild permissions:", perms_value)
    if perms_value & 0x00000008:  # 0x8 is the bit flag for administrator
        return True
    return False


def is_user_authorized(user_id, group: Group):
    """
    Check if a user is authorized to perform actions for a specific group.
    
    Checks the group's configuration to see if the user is in the list of
    authorized users for administrative actions.
    
    Args:
        user_id (int): The user's database ID
        group (Group): The Group object to check authorization for
        
    Returns:
        bool: True if user is authorized, False otherwise
    """
    # Check if the user is an admin or an authorized user for this group
    from db.models import GroupConfiguration
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


async def get_external_latency():
    """
    Get external network latency by pinging amazon.com.
    
    Performs a single ping to amazon.com and extracts the latency value
    from the output. Used for network diagnostics.
    
    Returns:
        str: Latency in milliseconds, or "N/A" if ping fails
    """
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
