from datetime import datetime, timedelta

from sqlalchemy import text
from lootboard import generator
from db.models import Group, Session, XenforoSession
import asyncio
import os

last_board_updates = {}

async def lootboard_update_loop():
    print("Starting lootboard update loop")
    try:
        await update_boards()
    except Exception as e:
        print(f"Exception in lootboard_update_loop: {e}")
    # Wait 2 minutes before the next iteration
    return True

def get_fresh_session():
    """Create a new database session - no global session management"""
    return Session()

def get_fresh_xenforo_session():
    """Create a new XenForo database session - no global session management"""
    return XenforoSession()




async def update_specific_board(group_id: int, force: bool = False):
    try:
        # Fetch the group with a short-lived session
        group_data = None
        with Session() as group_session:
            group = group_session.query(Group).filter(Group.group_id == group_id).first()
            if not group:
                print(f"Group {group_id} not found")
                return
            if not group.guild_id or group.guild_id == 0:
                print(f"Group {group_id} has no valid guild_id")
                return
            # Store group data outside the session
            group_data = {
                'group_name': group.group_name,
                'wom_id': group.wom_id
            }

        # Determine premium status with a separate short-lived session
        is_premium = False
        if group_id == 2:
            is_premium = True
        else:
            with get_fresh_xenforo_session() as xf_session:
                premium_status = xf_session.execute(
                    text("SELECT * FROM xf_user_upgrade_active WHERE group_id = :group_id"),
                    {"group_id": group_id}
                ).first()
                if premium_status:
                    is_premium = True

        # Non-premium throttling unless forced
        if not is_premium and not force:
            if group_id not in last_board_updates:
                last_board_updates[group_id] = datetime.now() - timedelta(days=7)
            if last_board_updates[group_id] > datetime.now() - timedelta(minutes=59):
                print(f"Skipping group {group_id}: within 60-minute window for non-premium")
                return

        # Ensure destination directory exists
        save_dir = f"/store/droptracker/disc/static/assets/img/clans/{group_id}/lb"
        if not os.path.exists(save_dir):
            os.makedirs(save_dir, exist_ok=True)

        # Generate using a fresh session that will be closed quickly
        print("Generating board for group:", group_id, "using a fresh session...")
        try:
            with Session() as gen_session:
                new_path = await generator.generate_server_board_temporary(
                    group_id=group_id,
                    wom_group_id=group_data['wom_id'],
                    session_to_use=gen_session
                )
            print(f"Board generated for {group_data['group_name']}")
            print(f"Board path: {new_path}")
            if not is_premium:
                last_board_updates[group_id] = datetime.now()
        except Exception as e:
            print(f"Error generating board for group {group_id}: {e}")
    except Exception as e:
        print(f"Exception in update_specific_board({group_id}): {e}")
    finally:
        print("Finished cycle and closed sessions.")

async def update_boards():
    try:
        # Get all groups with a dedicated session that's immediately closed
        original_groups = []
        with Session() as temp_session:
            try:
                original_groups = temp_session.query(Group).all()
            except Exception as e:
                temp_session.rollback()
                try:
                    original_groups = temp_session.query(Group).all()
                except Exception as e:
                    print(f"Error getting groups: {e}")
                    return
            
        # Create a clean list of groups outside the session
        groups = []
        for g in original_groups:
            if g.guild_id and g.guild_id != 0:
                temp_group = Group(group_name=g.group_name, guild_id=g.guild_id, wom_id=g.wom_id)
                temp_group.group_id = g.group_id
                groups.append(temp_group)
                
        # Process each group independently with its own session
        print(f"Found {len(groups)} groups to process")
        for group in groups:
            is_premium = False
            ## Determine if the group has premium status with a separate short-lived session
            if group.group_id == 2:
                is_premium = True
            else:
                with get_fresh_xenforo_session() as xenforo_session:
                    premium_status = xenforo_session.execute(
                                text("SELECT * FROM xf_user_upgrade_active WHERE group_id = :group_id"), 
                                {"group_id": group.group_id}
                            ).first()
                    if not premium_status:
                        if group.group_id not in last_board_updates:
                            last_board_updates[group.group_id] = datetime.now() - timedelta(days=7)
                        if last_board_updates[group.group_id] > datetime.now() - timedelta(minutes=59):
                            # Non-premium groups only get a 60-minute board refresh
                            continue
                    else:
                        is_premium = True
            
            try:
                if not os.path.exists(f"/store/droptracker/disc/static/assets/img/clans/{group.group_id}/lb"):
                    os.makedirs(f"/store/droptracker/disc/static/assets/img/clans/{group.group_id}/lb")
                
                # Create a completely new session for each group that will be closed quickly
                print("Generating board for group:", group.group_id, "using a fresh session...")
                try:
                    with Session() as group_session:
                        new_path = await generator.generate_server_board_temporary(group_id=group.group_id, wom_group_id=group.wom_id, session_to_use=group_session)
                    print(f"Board generated for {group.group_name}")
                    print(f"Board path: {new_path}")
                except Exception as e:
                    print(f"Error generating board for group {group.group_id}: {e}")
                    # No need to explicitly rollback - the context manager will handle it
            except Exception as e:
                print(f"Error in group processing for {group.group_id}: {e}")
                continue
        
    except Exception as e:
        print(f"Error updating boards: {e}")
    finally:
        print("Finished cycle and closed sessions.")
    
    print("Completed lootboard update loop. Waiting 2 minutes to continue")

async def startup():
    print("Starting lootboard update loop")
    await lootboard_update_loop()

if __name__ == "__main__":
    asyncio.run(startup())
    exit()
