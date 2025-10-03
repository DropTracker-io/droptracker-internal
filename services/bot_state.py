"""
    Handles join/leaves of the bot in Discord guilds
"""
from interactions import Button, Extension, listen
import interactions
from interactions.api.events import GuildJoin, GuildLeft
from interactions.models import ContainerComponent, TextDisplayComponent, ActionRow, SeparatorComponent, SectionComponent
from db.models import Guild, session
from datetime import datetime

## Guild Events ##
class BotState(Extension):
    @listen(GuildJoin)
    async def joined_guild(event: GuildJoin):
        guild = event.guild
        authed_list = []
        for member in guild.members:
            if member.has_permission(interactions.Permissions.ADMINISTRATOR):
                authed_list.append(member.id)
        droptracker_guild = await event.bot.fetch_guild(1172737525069135962)
        global_members = droptracker_guild.members
        for member in authed_list:
            if member in global_members:
                ## If the player is an admin & they also exist in our guild, we can try to DM them
                components = [
                    ContainerComponent(
                        SeparatorComponent(divider=True),
                        TextDisplayComponent(
                            text="The DropTracker Discord bot has been added to your server!" + 
                            "\nPlease enter your guild, and use the </create-group:1369493380358209543> command to begin setting up your new group.",
                            style=interactions.TextStyle.SUCCESS
                        ),
                        SeparatorComponent(divider=True),
                        ActionRow(
                            Button(
                                label="Learn More",
                                style=interactions.ButtonStyle.SUCCESS,
                                custom_id="clan_setup_info"
                            )   
                        )
                    )
                    
                ]
                await member.send(components=components)

        db_guild = session.query(Guild).filter(Guild.guild_id == event.guild_id).first()
        if not db_guild:
            db_guild = Guild(guild_id=str(event.guild_id),
                                date_added=datetime.now())
            session.add(db_guild)
            session.commit()
        pass

    @listen(GuildLeft)
    async def left_guild(event: GuildLeft):
        global total_guilds
        total_guilds -= 1
        pass