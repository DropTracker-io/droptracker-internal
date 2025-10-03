import aiohttp
import asyncio

xf_key = "y0goxY3I9v5ZsD_PFEDOl5cwE2oGN58k"
user_id = 1
headers = {
    'XF-Api-User': f'{user_id}',
    'XF-Api-Key': f'{xf_key}'
}

async def get_user_id(player_id: int):
    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://www.droptracker.io/api/player/{player_id}/get-user-id', headers=headers) as response:
            data = await response.json()
            return data.get('user_id', None)
        
async def create_alert(user_id: int, alert: str, link_url: str, link_title: str):
    data = {
        "to_user_id": user_id, ## The user ID of the user who will receive the alert
        "alert": alert, ## The text shown in the alert
        "from_user_id": user_id, ## The user ID of the user who created the alert
        "link_url": link_url, ## The URL of the link
        "link_title": link_title ## The title of the link
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(f'https://www.droptracker.io/api/alerts', headers=headers, json=data) as response:
            return await response.json()