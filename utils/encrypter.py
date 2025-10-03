from cryptography.fernet import Fernet
from base64 import b64encode, b64decode
from db.models import GroupConfiguration, Session
import os
from dotenv import load_dotenv

load_dotenv()

# The key must be 32 url-safe base64-encoded bytes
def get_encryption_key() -> str:
    with Session() as session:
        encryption_key = session.query(GroupConfiguration).where(GroupConfiguration.group_id == 2,
                                                                 GroupConfiguration.config_key == "encryption-gh").first()
        if encryption_key:
            return encryption_key.config_value
        else:
            raise Exception("Encryption key not found")

def encrypt_webhook(webhook_url: str, encryption_key: str = None) -> str:
    try:
        if encryption_key is None:
            encryption_key = get_encryption_key()
        f = Fernet(encryption_key)
        encrypted_webhook = f.encrypt(webhook_url.encode())
        # Return as base64 string for storage
        return b64encode(encrypted_webhook).decode()
    except Exception as e:
        raise Exception(f"Encryption failed: {str(e)}")

def decrypt_webhook(webhook_hash: str) -> str:
    try:
        encryption_key = get_encryption_key()
        f = Fernet(encryption_key)
        # Decode from base64 string back to bytes
        encrypted_data = b64decode(webhook_hash)
        decrypted_webhook = f.decrypt(encrypted_data)
        return decrypted_webhook.decode()
    except Exception as e:
        raise Exception(f"Decryption failed: {str(e)}")