from interactions import User
from sqlalchemy import text
from db.models import Group, GroupPatreon, xenforo_engine, XenforoSession, Session

def check_active_upgrade(group_id: int = None, user_id: int = None):
    print(f"RETURNING TRUE FOR UPGRADES ON DEV INSTANCE")
    return True
    # Check if at least one parameter is provided
    if user_id is None and group_id is None:
        return None
        ## Check for legacy patreon upgrades
    if group_id:
        with Session() as session:
            group = session.query(Group).filter(Group.group_id == group_id).first()
            if group:
                if group.group_patreon:
                    for patreon in group.group_patreon:
                        patreon: GroupPatreon = patreon
                        if patreon.patreon_tier >= 1:
                            return True
    if user_id:
        with Session() as session:
            patreon = session.query(GroupPatreon).filter(GroupPatreon.user_id == user_id).first()
            if patreon:
                if patreon.patreon_tier >= 1:
                    return True
    

    # Build the appropriate query based on parameters for the new XenForo-based account upgrades
    if user_id is not None and group_id is None:
        raw_query = """
            SELECT *
            FROM xf_user_upgrade_active
            WHERE user_id = :user_id
            AND is_cancelled = 0
        """
    elif group_id is not None and user_id is None:
        raw_query = """
            SELECT *
            FROM xf_user_upgrade_active
            WHERE group_id = :group_id
            AND is_cancelled = 0
        """
    else:  # Both user_id and group_id are provided
        raw_query = """
            SELECT *
            FROM xf_user_upgrade_active
            WHERE user_id = :user_id
            AND group_id = :group_id
            AND is_cancelled = 0
        """
        
    with XenforoSession() as session:
        # Get the Xenforo user ID from external ID
        xf_uid = None
        if user_id is not None:  # Changed from 'if user_id:'
            print(f"Checking active upgrade for user {user_id}")
            xf_user_result = session.execute(
                text("SELECT user_id FROM xf_user WHERE external_id = :user_id"), 
                {"user_id": user_id}
            ).first()
            
            # Check if user exists in Xenforo
            if not xf_user_result:
                print(f"No Xenforo user found with external_id {user_id}")
                return None
            else:
                print(f"Xenforo user found with external_id {user_id}")
            xf_uid = xf_user_result[0]  # Extract the actual ID value
        
        # Execute the appropriate query based on parameters
        if user_id is not None and group_id is None:
            print(f"Executing query for user {user_id}")
            result = session.execute(text(raw_query), {"user_id": xf_uid}).fetchall()
        elif group_id is not None and user_id is None:
            print(f"Executing query for group {group_id}")
            result = session.execute(text(raw_query), {"group_id": group_id}).fetchall()        

        else:  # Both parameters provided
            print(f"Executing query for user {user_id} and group {group_id}")
            result = session.execute(text(raw_query), {"user_id": xf_uid, "group_id": group_id}).fetchall()

            
        return result
    
    