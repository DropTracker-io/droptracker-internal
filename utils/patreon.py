import requests
import asyncio
import os
from dotenv import load_dotenv
# Assuming User and session are correctly defined in your db.models
from db.models import Group, User, session, GroupPatreon
from sqlalchemy import func, text
from utils.messages import new_patreon_sub
from utils.logger import LoggerClient
load_dotenv()
logger = LoggerClient(token=os.getenv('LOGGER_TOKEN'))

from interactions import Task, IntervalTrigger

@Task.create(IntervalTrigger(minutes=60))
async def patreon_sync():
    await logger.log("access", "Patreon sync task started...", "patreon_sync")
    new_data = await get_creator_patreon_data()
    
    if new_data is not None:
        # Fetch existing data from the database
        existing_entries = session.query(GroupPatreon).all()
        existing_user_ids = {entry.user_id for entry in existing_entries}

        # Determine new subscribers
        new_subscribers = [member for member in new_data if member['discord_id'] not in existing_user_ids]

        # Clear the GroupPatreon table
        session.query(GroupPatreon).delete()
        session.commit()

        # Insert new data
        for member in new_data:
            user = session.query(User).filter(User.discord_id == member['discord_id']).first()
            if user:
                new_group_patreon = GroupPatreon(
                    user_id=user.user_id,
                    group_id=None, 
                    patreon_tier=member['tier'],
                    date_added=func.now()
                )
                session.add(new_group_patreon)

        session.commit()

        # Send notifications for new subscribers
        for member in new_subscribers:
            try:
                print(f"New subscriber: {member['full_name']} with tier {member['tier']}")
                tier = member['tier']
                discord_id = member['discord_id']
                user = session.query(User).filter(User.discord_id == discord_id).first()
                if user:
                    user_id = user.user_id
                else:
                    user_id = None
                
                # Use execute with parameter binding
                session.execute(
                    text("INSERT INTO patreon_notification (discord_id, user_id, tier, status) VALUES (:discord_id, :user_id, :tier, :status)"),
                    {"discord_id": discord_id, "user_id": user_id, "tier": tier, "status": 0}
                )
                session.commit()  # Commit the transaction
            except Exception as e:
                print("Couldn't send patreon sub msg:", e)


async def get_creator_patreon_data():
    url = "https://www.patreon.com/api/oauth2/v2/campaigns/12053510/members"
    headers = {
        "Authorization": f"Bearer {os.getenv('PATREON_ACCESS_TOKEN')}"
    }
    params = {
        "include": "currently_entitled_tiers,user",  # Include user info
        "fields[member]": "patron_status,pledge_relationship_start,full_name,email",
        "fields[user]": "social_connections"
    }
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        patreon_members = parse_patreon_members(data)
        print("Got Patreon response, updating database.")
        return patreon_members
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None

def parse_patreon_members(data):
    patreon_members = []
    included_users = {user['id']: user for user in data.get('included', [])}

    for member in data.get('data', []):
        attributes = member.get('attributes', {})
        relationships = member.get('relationships', {})
        
        full_name = attributes.get('full_name', 'Unknown')
        email = attributes.get('email', 'Unknown')
        patron_status = attributes.get('patron_status', 'None')
        discord_id = None
        currently_entitled_tiers = relationships.get('currently_entitled_tiers', [])
        tier = 0
        if currently_entitled_tiers:
            tiers = currently_entitled_tiers['data']
            if tiers:
                for pt_tier in tiers:
                    if int(pt_tier['id']) == 22736754:
                        tier = 1
                    elif int(pt_tier['id']) == 22736762:
                        tier = 2
                    elif int(pt_tier['id']) == 22736787:
                        tier = 3
                    elif int(pt_tier['id']) == 23798233:
                        tier = 4
                    elif int(pt_tier['id']) == 23798236:
                        tier = 5
                    else:
                        continue

        user_relationship = relationships.get('user', {}).get('data', {})
        user_id = user_relationship.get('id')
        if user_id and user_id in included_users:
            social_connections = included_users[user_id].get('attributes', {}).get('social_connections', {})
            if social_connections.get('discord', None):
                discord_id = social_connections.get('discord', {}).get('user_id')
        patreon_members.append({
            'full_name': full_name,
            'email': email,
            'patron_status': patron_status,
            'discord_id': discord_id,
            'tier': tier
        })
    
    return patreon_members
