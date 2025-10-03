"""
    Handles updating channel names based on group member count/loot value
"""
import interactions
from interactions import Extension, Task, IntervalTrigger, ChannelType
from db.models import Group, GroupConfiguration, session, Player
from datetime import datetime, timedelta
from sqlalchemy import text
from utils.format import format_number
from utils.redis import redis_client
from db.ops import associate_player_ids
from utils.wiseoldman import fetch_group_members
import time
import asyncio

class ChannelNames(Extension):
    def __init__(self, bot: interactions.Client):
        self.bot = bot
        asyncio.create_task(self.update_channel_names())
        print("Channel names service initialized.")

    async def update_channel_names(self):
        while True:
            bot: interactions.Client = self.bot
            loot_channel_id_configs = session.query(GroupConfiguration).filter(GroupConfiguration.config_key == 'vc_to_display_monthly_loot').all()
            #print("Got all loot channel id configs", loot_channel_id_configs)
            for channel_setting in loot_channel_id_configs:
                #print("Channel setting is:", channel_setting)
                if channel_setting.config_value != "":
                    #print("Channel setting value is not empty")
                    try:
                        channel = await bot.fetch_channel(channel_id=channel_setting.config_value)
                        if channel:
                            #print("Channel is not None")
                            if channel.type == ChannelType.GUILD_VOICE:
                                #print("Channel is a voice channel")
                                template = session.query(GroupConfiguration).filter(GroupConfiguration.config_key == 'vc_to_display_monthly_loot_text',
                                                                                    GroupConfiguration.group_id == channel_setting.group_id).first()
                                template_str = template.config_value
                                if template_str == "" or not template_str:
                                    template_str = "{month}: {gp_amount} gp"
                                if channel_setting.group_id != 2:
                                    group_wom_id = session.query(Group.wom_id).filter(Group.group_id == channel_setting.group_id).first()
                                    if group_wom_id:
                                        group_wom_id = group_wom_id[0]
                                    if group_wom_id:
                                        #print("Finding group members?")
                                        try:
                                            wom_member_list = await fetch_group_members(wom_group_id=int(group_wom_id))
                                        except Exception as e:
                                            #print("Couldn't get the member list", e)
                                            return
                                    player_ids = await associate_player_ids(wom_member_list)
                                    clan_player_ids = wom_member_list if wom_member_list else []
                                else:
                                    clan_player_ids = session.query(Player.player_id).all()
                                    player_ids = [player_id[0] for player_id in clan_player_ids]
                                player_id = player_ids[0]
                                group_total = 0
                                from datetime import datetime
                                partition = datetime.now().year * 100 + datetime.now().month
                                for player_id in player_ids:
                                    player_total_month = f"player:{player_id}:{partition}:total_loot"
                                    player_month_total = redis_client.get(player_total_month)
                                    if player_month_total is None:
                                        player_month_total = 0
                                    group_total += int(player_month_total)
                                month_str = datetime.now().strftime("%B")
                                #group_rank, ranked_in_group, group_total_month = calculate_clan_overall_rank(player_id, player_ids)
                                #group_total_month = format_number(group_total_month)
                                fin_text = template_str.replace("{month}", month_str).replace("{gp_amount}", format_number(group_total))
                                await channel.edit(name=f"{fin_text}")
                        else:
                            continue
                            #print("Channel is not found for group ID", channel_setting.group_id, "and config value", channel_setting.config_value)
                    except Exception as e:
                        print("Couldn't edit the channel. e:", e)
            member_channel_id_configs = session.query(GroupConfiguration).filter(GroupConfiguration.config_key == 'vc_to_display_droptracker_users',
                                                                                GroupConfiguration.config_value != "").all()
            print("Updating group member channel names for", len(member_channel_id_configs), "channels")
            for channel_setting in member_channel_id_configs:
                if channel_setting.group_id == 2:
                    total_members = session.query(Player.player_id).count()
                else:
                    group = session.query(Group).filter(Group.group_id == channel_setting.group_id).first()
                    total_members = group.get_player_count()
                if channel_setting.config_value != "":
                    try:
                        channel = await bot.fetch_channel(channel_id=channel_setting.config_value)
                        template = session.query(GroupConfiguration).filter(GroupConfiguration.config_key == 'vc_to_display_droptracker_users_text',
                                                                            GroupConfiguration.group_id == channel_setting.group_id).first()
                        template_str = template.config_value
                        if template_str == "" or not template_str:
                            template_str = "{member_count} members"
                        if channel:
                            await channel.edit(name=template_str.replace("{member_count}", str(total_members)))
                    except Exception as e:
                        print("Couldn't edit the channel. e:", e)
            await asyncio.sleep(600)