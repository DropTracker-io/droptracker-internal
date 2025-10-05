"""
User Commands Module

Contains all user-level Discord slash commands that don't require special permissions.
These commands are available to all users and handle personal settings, account management,
and basic bot interactions.

Classes:
    UserCommands: Extension containing user-level slash commands

Author: joelhalen
"""

import json
from secrets import token_hex
from data.submissions import try_create_player
from interactions import AutocompleteContext, SlashContext, Embed, OptionType, Extension, slash_command, slash_option
from db.models import Session, User, Group, Guild, Player, UserConfiguration, session
from services.components import help_components
from services.points import award_points_to_player
from utils.format import format_time_since_update, get_command_id
from utils.wiseoldman import check_user_by_username
from .utils import try_create_user


class UserCommands(Extension):
    """
    Extension containing user-level Discord slash commands.
    
    This extension provides commands that regular users can execute to manage
    their accounts, configure settings, and interact with the DropTracker system.
    """
    
    def __init__(self, bot):
        """
        Initialize the UserCommands extension.
        
        Args:
            bot: The Discord bot instance
        """
        self.bot = bot
        self.message_handler = bot.get_ext("services.message_handler")

    @slash_command(name="help",
                   description="View helpful commands/links for the DropTracker")
    async def help(self, ctx: SlashContext):
        """
        Display help information and useful links for the DropTracker.
        
        Shows a comprehensive help interface with buttons and links to
        various DropTracker resources and commands.
        
        Args:
            ctx (SlashContext): The slash command context
        """
        user = session.query(User).filter_by(discord_id=ctx.user.id).first()
        if not user:
            await try_create_user(ctx=ctx)
        user = session.query(User).filter(User.discord_id == ctx.author.id).first()
        return await ctx.send(components=help_components, ephemeral=True)

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
        """
        Configure direct message notification settings.
        
        Allows users to enable or disable different types of direct message
        notifications from the bot, including update logs and points earned.
        
        Args:
            ctx (SlashContext): The slash command context
            dm_type (str): Type of DM setting ("updates", "points", "both")
            toggle (str): Whether to "enable" or "disable" the setting
        """
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
            desc_ext = "- Update logs\\n- Points earned"
        elif dm_type == "updates":
            desc_ext = "- Update logs"
        elif dm_type == "points":
            desc_ext = "- Points earned"

        if toggle == "enable":
            embed = Embed(
                title="Success!",
                description=f"You have enabled direct-message notifications from me for:\\n" + desc_ext
            )
        else:
            embed = Embed(
                title="Success!",
                description=f"You have disabled direct-message notifications from me for:\\n" + desc_ext
            )
        await ctx.send(embed=embed, ephemeral=True)

    @dm_settings_cmd.autocomplete("dm_type")
    async def dm_settings_autocomplete_dm_type(self, ctx: AutocompleteContext):
        """Provide autocomplete options for DM settings type."""
        await ctx.send(
            choices=[
                {"name": "Update Logs", "value": "updates"},
                {"name": "Points earned", "value": "points"},
                {"name": "Both", "value": "both"}
            ]
        )

    @dm_settings_cmd.autocomplete("toggle")
    async def dm_settings_autocomplete_toggle(self, ctx: AutocompleteContext):
        """Provide autocomplete options for enable/disable toggle."""
        await ctx.send(
            choices=[
                {"name": "Enable", "value": "enable"},
                {"name": "Disable", "value": "disable"}
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
        """
        Configure ping settings for submission notifications.
        
        Allows users to control when they get pinged by the bot for their
        submissions in different contexts (global, group, or nowhere).
        
        Args:
            ctx (SlashContext): The slash command context
            type (str): Ping type ("global", "group", "everywhere")
        """
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
        """Provide autocomplete options for ping types."""
        await ctx.send(
            choices=[
                {"name": f"Globally", "value": "global"},
                {"name": f"In my group", "value": "group"},
                {"name": f"Everywhere", "value": "everywhere"}
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
        """
        Configure visibility settings for accounts in public listings.
        
        Allows users to hide their accounts from global leaderboards and
        public displays while still participating in group activities.
        
        Args:
            ctx (SlashContext): The slash command context
            account (str): Account name to hide, or "all" for all accounts
        """
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
        """Provide autocomplete options for user accounts."""
        user = session.query(User).filter_by(discord_id=str(ctx.user.id)).first()
        
        if not user:
            # User not found in database
            return await ctx.send(
                choices=[{"name": "All accounts", "value": "all"}]
            )
        
        # Query for the user's accounts
        accounts = session.query(Player).filter_by(user_id=user.user_id).all()
        
        # Always include "All accounts" option
        choices = [{"name": "All accounts", "value": "all"}]
        
        # Add player accounts if they exist
        if accounts:
            choices.extend([
                {"name": account.player_name, "value": account.player_name}
                for account in accounts
            ])
        
        return await ctx.send(choices=choices)
            
    @slash_command(name="accounts",
                   description="View your currently claimed RuneScape character names, if you have any")
    async def user_accounts_cmd(self, ctx: SlashContext):
        """
        Display all accounts claimed by the user.
        
        Shows a list of all OSRS accounts associated with the user's Discord account,
        including their IDs and last update times.
        
        Args:
            ctx (SlashContext): The slash command context
        """
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
                account_names += f"`" + account.player_name.strip() + f"` (id: {account.player_id})\\n> Last updated: {last_updated_unix}\\n"
                
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
    async def claim_rsn_command(self, ctx: SlashContext, rsn: str):
        """
        Claim ownership of a RuneScape account.
        
        Associates an OSRS account with the user's Discord account, allowing
        them to receive notifications and participate in group activities.
        
        Args:
            ctx (SlashContext): The slash command context
            rsn (str): The RuneScape username to claim
        """
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
        
        if not player:
            try:
                wom_data = await check_user_by_username(rsn)
            except Exception as e:
                print("Couldn't get player data. e:", e)
                return await ctx.send(f"An error occurred claiming your account.\\n" +
                                      "Try again later, or reach out in our Discord server",
                                      ephemeral=True)
            if wom_data:
                player, player_name, player_id, log_slots = wom_data
                try:
                    print("Creating a player with user ID", user.user_id, "associated with it")
                    # Create the Player with a temporary acc hash for now
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
                    if player_id is not None:

                        return await ctx.send(f"Your account ({player_name}), with ID `{player_id}` has " +
                                         "been added to the database & associated with your Discord account.",ephemeral=True)
                    else:
                        return await ctx.send(f"An error occurred while attempting to register your account. Please try again, or [reach out in Discord](https://www.droptracker.io/discord)",ephemeral=True)
            else:
                return await ctx.send(f"Your account was not found in the WiseOldMan database.\\n" +
                                     f"You could try to manually update your account on their website by [clicking here](https://www.wiseoldman.net/players/{rsn}), then try again, or wait a bit.")
        else:
            joined_time = format_time_since_update(player.date_added)
            if player.user:
                user: User = player.user
                if str(user.discord_id) != str(ctx.user.id):
                    await ctx.send(f"Uh-oh!\\n" +
                                f"It looks like somebody else may have claimed your account {joined_time}!\\n" +
                                f"<@{player.user.discord_id}> (discord id: {player.user.discord_id}) currently owns it in our database.\\n" + 
                                "If this is some type of mistake, please reach out in our discord server:\\n" + 
                                "https://www.droptracker.io/discord",
                                ephemeral=True)
                else:
                    await ctx.send(f"It looks like you've already claimed this account ({player.player_name}) {joined_time}\\n" + 
                                "\\nSomething not seem right?\\n" +
                                "Please reach out in our discord server:\\n" + 
                                "https://www.droptracker.io/discord",
                                ephemeral=True)
            else:
                player.user = user
                session.commit()
                embed = Embed(title="Success!",
                              description=f"Your in-game name has been successfully associated with your Discord account.\\n" +
                              "That's it!") 
                embed.add_field(name=f"What's next?",value=f"If you'd like, you can [register an account on our website] to stay informed " + 
                                "on updates & to make your voice heard relating to bugs & suggestions.",inline=False)
                embed.set_thumbnail(url="https://www.droptracker.io/img/droptracker-small.gif")
                embed.set_footer(text="Powered by the DropTracker | https://www.droptracker.io/")
                await ctx.send(embed=embed)
