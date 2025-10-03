import asyncio
import os
import aiohttp
import github
import schedule
import time
from github import Github
from db.models import GroupConfiguration, Webhook, NewWebhook, Session, WebhookPendingDeletion, session as db_sesh
from dotenv import load_dotenv
from interactions import IntervalTrigger, Task
import json
from utils.encrypter import encrypt_webhook, decrypt_webhook
from utils.logger import LoggerClient
from datetime import datetime, timedelta
from db.app_logger import AppLogger
load_dotenv()

logger = LoggerClient()

app_logger = AppLogger()

total_hooks = 0

updates = []

class GithubPagesUpdater:
    def __init__(self):
        """
        Initialize the GitHubPagesUpdater.
        """
        load_dotenv()  # Load environment variables

        # GitHub Token and Repo Info
        self.github_token = os.getenv("GITHUB_TOKEN")  # Load GitHub token from .env
        repo_name = "droptracker-io/droptracker-io.github.io"  # GitHub repository name
        self.new_file = "content/core.json"
        self.branch = "main"
        # Initialize GitHub API
        self.github = Github(self.github_token)
        self.repo = self.github.get_repo(repo_name)

        # Log the repo and file path for verification
        # print(f"Repo: {repo_name}")

    def fetch_webhooks_from_database(self, limit=80):
        """
        Fetch the webhook URLs from the database and format them as a list of URLs.
        
        Args:
            limit: Maximum number of webhooks to fetch
            
        Returns:
            list of encrypted webhooks
        """
        try:
            webhooks = db_sesh.query(Webhook).limit(limit).all()  # Grab limited number of webhooks
            
            main_urls = [w.webhook_url for w in webhooks if w.webhook_url]
            main_encrypted = []
            
            # Try to encrypt each webhook, skipping any that fail
            for url in main_urls:
                try:
                    encrypted = encrypt_webhook(url)
                    main_encrypted.append(encrypted)
                except Exception as e:
                    print(f"Failed to encrypt webhook {url}: {e}")
            
            if not main_encrypted:
                raise ValueError("No webhooks could be encrypted. Check encryption key configuration.")
            
            return main_encrypted
        except Exception as e:
            print(f"Error fetching webhook URLs from the database: {e}")
            # Check if this is an encryption key error
            if "Fernet key must be 32 url-safe base64-encoded bytes" in str(e):
                print("Encryption key error detected. Attempting to generate a valid key...")
                with Session() as session:
                    # Try to update the encryption key
                    encryption_config = session.query(GroupConfiguration).where(
                        GroupConfiguration.group_id == 2,
                        GroupConfiguration.config_key == "encryption-gh"
                    ).first()
                    
                    if encryption_config:
                        new_key = self._generate_fernet_key()
                        encryption_config.config_value = new_key
                        session.commit()
                        print(f"Updated encryption key to: {new_key}")
                    else:
                        # Create a new encryption key config if it doesn't exist
                        new_key = self._generate_fernet_key()
                        new_config = GroupConfiguration(
                            group_id=2,
                            config_key="encryption-gh",
                            config_value=new_key
                        )
                        session.add(new_config)
                        session.commit()
                        print(f"Created new encryption key: {new_key}")
            
            # Re-raise the exception to be handled by the caller
            raise

    def find_files_by_name(self, path="", file_name=""):
        """
        Search the repository for files with the specified file name.
        :param path: The path to start searching from (empty for root).
        :param file_name: The name of the file to search for.
        :return: A list of file paths matching the specified file name.
        """
        matching_files = []

        try:
            # print(f"Searching for files named '{file_name}' in repository: {self.repo.full_name}, path: {path or 'root'}")
            contents = self.repo.get_contents(path)  # Get contents of the specified path

            for content_file in contents:
                if content_file.type == "dir":
                    # Recursively search in subdirectories
                    matching_files += self.find_files_by_name(content_file.path, file_name)
                elif content_file.name == file_name:
                    # If the file name matches, add to the list
                    print(f"Found file: {content_file.path}")
                    matching_files.append(content_file.path)

        except github.GithubException as e:
            # print(f"Failed to search files: {e}")
            if isinstance(e, github.GithubException):
                print(f"Error response from GitHub: {e.data}")

        return matching_files

    def list_repo_files(self, path=""):
        """
        List all files in the repository starting from a specific path.
        :param path: The path in the repository to start listing files from (e.g., 'content/'). Leave empty for root.
        """
        try:
            # print(f"Listing files in repository: {self.repo.full_name}, path: {path or 'root'}")
            contents = self.repo.get_contents(path)  # Get contents of the specified path
            if not contents:
                print("No files found.")
                return
            
            for content_file in contents:
                if content_file.type == "dir":
                    # print(f"Directory: {content_file.path}")
                    # Recursively list files in subdirectories
                    self.list_repo_files(content_file.path)
                # print(f"File: {content_file.path}")

        except Exception as e:
            print(f"Failed to list files: {e}")
            if isinstance(e, github.GithubException):
                # print(f"Error response from GitHub: {e.data}")
                pass

    async def update_github_pages(self, watchdog=None):
        global total_hooks, updates

        ex_hooks = db_sesh.query(Webhook).count()
        
        print("Loading initial total webhook data & sending update...")
        total_hooks = ex_hooks
        # Check only the webhooks we'll use
        await check_limited_webhooks(80, watchdog)
        await asyncio.to_thread(self._update_github_pages)

    def _update_github_pages(self):
        """
        Fetch the latest webhooks from the database and update all matching GitHub Pages files.
        Also updates news and encryption key files in the same commit.
        """
        # List repository files to debug the file path issue
        self.list_repo_files()
        
        # Collect all files that need to be updated
        files_to_update = []
        
        # Add news file if needed
        news_file = self._prepare_news_update()
        if news_file:
            files_to_update.append(news_file)
        
        # Add encryption key file if needed
        encryption_key_file = self._prepare_encryption_key_update()
        if encryption_key_file:
            files_to_update.append(encryption_key_file)
        
        # Fetch webhook URLs from the database (limited to 80)
        try:    
            encrypted_webhooks = self.fetch_webhooks_from_database(limit=80)
        except Exception as e:
            print(f"Error fetching webhook URLs from the database: {e}")
            return
        
        # Check if we have enough webhooks
        if len(encrypted_webhooks) < 30:
            print("Generated list is too short:", len(encrypted_webhooks))
            return
        
        # Split webhooks into chunks of 40 for different files
        webhook_chunks = []
        chunk_size = 40
        for i in range(0, len(encrypted_webhooks), chunk_size):
            webhook_chunks.append(encrypted_webhooks[i:i + chunk_size])
        
        file_exists = False  # If it doesn't exist we need to initially create it

        # Update core.json file with first chunk
        if webhook_chunks:
            core_json_content = json.dumps(webhook_chunks[0], indent=4)
            
            for file_path in self.find_files_by_name(file_name=self.new_file):
                file_exists = True
                try:
                    file = self.repo.get_contents(file_path, ref=self.branch)
                    old_content = file.decoded_content.decode('utf-8')
                    if "{" in old_content:
                        old_json = json.loads(old_content)
                    else:
                        old_json = []
                        
                    # Check if content has changed
                    changes_detected = len(old_json) != len(webhook_chunks[0])
                    if not changes_detected:
                        github_webhooks_set = set()
                        db_webhooks_set = set()
                        for webhook in old_json:
                            try:
                                decrypted_webhook = decrypt_webhook(webhook)
                                github_webhooks_set.add(decrypted_webhook)
                            except Exception as e:
                                print(f"Error decrypting webhook from GitHub: {e}")
                        for encrypted_webhook in webhook_chunks[0]:
                            try:
                                decrypted_webhook = decrypt_webhook(encrypted_webhook)
                                db_webhooks_set.add(decrypted_webhook)
                            except Exception as e:
                                print(f"Error decrypting webhook from database: {e}")
                        changes_detected = github_webhooks_set != db_webhooks_set
                        
                    if changes_detected:
                        files_to_update.append((file_path, core_json_content))
                        added = len(webhook_chunks[0]) - len(old_json)
                        print(f"Webhook changes detected. Updating with {added} new webhooks.")
                    else:
                        print(f"No GitHub webhook changes detected. Skipping update.")
                except github.GithubException as e:
                    print(f"Failed to update GitHub Pages for {file_path}: {e}")
                except Exception as e:
                    print(f"Unexpected error: {e}")
                    raise e
            
            if not file_exists:
                files_to_update.append((self.new_file, core_json_content))

        # Store encrypted webhooks in the database
        db_sesh.query(NewWebhook).delete()
        for webhook_hash in encrypted_webhooks:
            db_sesh.add(NewWebhook(webhook_hash=webhook_hash))
        db_sesh.commit()

        # Also create a date-based backup file with second chunk if available
        if len(webhook_chunks) > 1:
            # Get current date and tomorrow's date
            current_date = datetime.now()
            tomorrow_date = current_date + timedelta(days=1)
            
            # Format dates for filenames
            current_date_str = current_date.strftime("%Y%m%d")
            tomorrow_date_str = tomorrow_date.strftime("%Y%m%d")
            
            # Create paths for both dates
            current_backup_file = f"content/{current_date_str}.json"
            tomorrow_backup_file = f"content/{tomorrow_date_str}.json"
            
            # Check if files exist
            current_file_paths = self.find_files_by_name(file_name=f"{current_date_str}.json")
            tomorrow_file_paths = self.find_files_by_name(file_name=f"{tomorrow_date_str}.json")
            
            backup_json_content = json.dumps(webhook_chunks[1], indent=4)
            
            # Add current date file if it doesn't exist
            if current_file_paths:
                files_to_update.append((current_file_paths[0], backup_json_content))
            else:
                files_to_update.append((current_backup_file, backup_json_content))
                
            # Add tomorrow's date file if it doesn't exist
            if tomorrow_file_paths:
                files_to_update.append((tomorrow_file_paths[0], backup_json_content))
            else:
                files_to_update.append((tomorrow_backup_file, backup_json_content))

        # Only perform the commit if there are files to update
        if files_to_update:
            self.update_multiple_files(
                files_to_update,
                commit_message="Updating files based on changes in the database.",
                branch=self.branch
            )

    def _prepare_news_update(self):
        """
        Prepare the news file update but don't commit it yet.
        Returns a tuple of (file_path, content) if an update is needed, None otherwise.
        """
        with Session() as session:
            current_news_data = session.query(GroupConfiguration).where(GroupConfiguration.group_id == 2,
                                                                        GroupConfiguration.config_key == "news-gh").first()
            if current_news_data:
                news_content = f"{current_news_data.config_value}" if current_news_data.config_value and current_news_data.config_value != "" else current_news_data.long_value if current_news_data.long_value and current_news_data.long_value != "" else ""
                news_file_path = "content/news.txt"
                
                # Check if file exists and content has changed
                try:
                    file = self.repo.get_contents(news_file_path, ref=self.branch)
                    old_content = file.decoded_content.decode('utf-8')
                    if old_content != news_content:
                        print(f"News content has changed. Updating {news_file_path}")
                        return (news_file_path, news_content)
                    else:
                        print(f"No news content changes. Skipping update.")
                        return None
                except github.GithubException as e:
                    if e.status == 404:
                        # File doesn't exist, create it
                        print(f"News file doesn't exist. Creating {news_file_path}")
                        return (news_file_path, news_content)
                    else:
                        print(f"Error checking news file: {e}")
                        return None
        return None

    def _prepare_encryption_key_update(self):
        """
        Prepare the encryption key file update but don't commit it yet.
        Returns a tuple of (file_path, content) if an update is needed, None otherwise.
        """
        with Session() as session:
            current_encryption_key = session.query(GroupConfiguration).where(GroupConfiguration.group_id == 2,
                                                                            GroupConfiguration.config_key == "encryption-gh").first()
            if current_encryption_key:
                # Get current date and tomorrow's date
                current_date = datetime.now()
                tomorrow_date = current_date + timedelta(days=1)
                
                # Format dates for filenames
                current_date_str = current_date.strftime("%Y%m%d")
                tomorrow_date_str = tomorrow_date.strftime("%Y%m%d")
                
                # Create paths for both dates
                current_key_file = f"content/{current_date_str}-k.txt"
                tomorrow_key_file = f"content/{tomorrow_date_str}-k.txt"
                
                encryption_key_content = current_encryption_key.config_value
                
                # Validate the encryption key format
                if not self._is_valid_fernet_key(encryption_key_content):
                    # Generate a new valid key if the current one is invalid
                    new_key = self._generate_fernet_key()
                    print(f"Invalid encryption key detected. Generated new key: {new_key}")
                    
                    # Update the key in the database
                    current_encryption_key.config_value = new_key
                    session.commit()
                    
                    encryption_key_content = new_key
                
                # Check if today's file exists
                try:
                    self.repo.get_contents(current_key_file, ref=self.branch)
                    print(f"Today's encryption key file already exists. Skipping update.")
                except github.GithubException as e:
                    if e.status == 404:
                        # File doesn't exist, create it
                        print(f"Creating today's encryption key file: {current_key_file}")
                        return (current_key_file, encryption_key_content)
                    else:
                        print(f"Error checking encryption key file: {e}")
                        return None
                
                # Check if tomorrow's file exists
                try:
                    self.repo.get_contents(tomorrow_key_file, ref=self.branch)
                    print(f"Tomorrow's encryption key file already exists. Skipping update.")
                except github.GithubException as e:
                    if e.status == 404:
                        # File doesn't exist, create it
                        print(f"Creating tomorrow's encryption key file: {tomorrow_key_file}")
                        return (tomorrow_key_file, encryption_key_content)
                    else:
                        print(f"Error checking encryption key file: {e}")
                        return None
        return None

    def _is_valid_fernet_key(self, key):
        """
        Check if a key is a valid Fernet key (32 url-safe base64-encoded bytes).
        
        :param key: The key to validate
        :return: True if valid, False otherwise
        """
        import base64
        try:
            # A valid Fernet key is 32 bytes, base64-encoded
            decoded = base64.urlsafe_b64decode(key.encode('utf-8') + b'=' * (4 - len(key) % 4))
            return len(decoded) == 32
        except Exception:
            return False

    def _generate_fernet_key(self):
        """
        Generate a valid Fernet key (32 url-safe base64-encoded bytes).
        
        :return: A valid Fernet key as a string
        """
        from cryptography.fernet import Fernet
        return Fernet.generate_key().decode('utf-8')

    def update_news(self):
        """
        Update the news.txt file in the content directory.
        This is a standalone method that can be called independently.
        """
        news_file = self._prepare_news_update()
        if news_file:
            self.update_file(news_file[0], news_file[1])

    def update_encryption_key(self):
        """
        Updates the current encryption key.
        This is a standalone method that can be called independently.
        """
        encryption_key_file = self._prepare_encryption_key_update()
        if encryption_key_file:
            self.update_file(encryption_key_file[0], encryption_key_file[1])

    def update_file(self, file_path, new_content):
        """
        Update a single file in the repository, but only if the content has changed.
        
        :param file_path: Path to the file in the repository
        :param new_content: New content for the file
        :return: True if the file was updated, False otherwise
        """
        try:
            # Check if file exists
            try:
                file = self.repo.get_contents(file_path, ref=self.branch)
                exists = True
                old_content = file.decoded_content.decode('utf-8')
            except github.GithubException as e:
                if e.status == 404:
                    exists = False
                    old_content = ""
                else:
                    raise
            
            # Only update if content has changed
            if not exists or old_content != new_content:
                print(f"Updating file: {file_path}")
                
                if exists:
                    # Update existing file
                    self.repo.update_file(
                        path=file_path,
                        message=f"Update {file_path}",
                        content=new_content,
                        sha=file.sha,
                        branch=self.branch
                    )
                else:
                    # Create new file
                    self.repo.create_file(
                        path=file_path,
                        message=f"Create {file_path}",
                        content=new_content,
                        branch=self.branch
                    )
                return True
            else:
                print(f"No changes detected for {file_path}. Skipping update.")
                return False
            
        except github.GithubException as e:
            print(f"Failed to update file {file_path}: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error updating {file_path}: {e}")
            return False

    def update_multiple_files(self, files_to_update, commit_message, branch="main"):
        """
        Update multiple files in a single commit to avoid multiple GitHub Pages builds.

        :param files_to_update: List of tuples (file_path, new_content)
        :param commit_message: Commit message for the update
        :param branch: Branch to update (default: main)
        """
        repo = self.repo

        # 1. Get the latest commit and tree
        ref = repo.get_git_ref(f"heads/{branch}")
        latest_commit = repo.get_git_commit(ref.object.sha)
        base_tree = repo.get_git_tree(latest_commit.tree.sha)

        # 2. Create blobs for each file
        element_list = []
        for file_path, new_content in files_to_update:
            blob = repo.create_git_blob(new_content, "utf-8")
            element = github.InputGitTreeElement(
                path=file_path,
                mode="100644",
                type="blob",
                sha=blob.sha
            )
            element_list.append(element)

        # 3. Create a new tree
        new_tree = repo.create_git_tree(element_list, base_tree)

        # 4. Create a new commit
        new_commit = repo.create_git_commit(commit_message, new_tree, [latest_commit])

        # 5. Update the branch reference
        ref.edit(new_commit.sha)


async def test_webhook(webhook, session):
    """Test a single webhook and return its status"""
    try:
        start_time = time.time()
        if len(str(webhook.webhook_url)) < 5:
            return {
                'webhook_id': webhook.webhook_id if hasattr(webhook, 'webhook_id') else 'pending_deletion',
                'url': webhook.webhook_url,
                'status': 'Error',
                'elapsed': 0,
                'ok': False
            }
        async with session.get(webhook.webhook_url, timeout=10) as response:
            elapsed = time.time() - start_time
            status = response.status
            return {
                'webhook_id': webhook.webhook_id if hasattr(webhook, 'webhook_id') else 'pending_deletion',
                'url': webhook.webhook_url,
                'status': status,
                'elapsed': elapsed,
                'ok': 200 <= status < 400
            }
    except aiohttp.ClientError as e:
        return {
            'webhook_id': webhook.webhook_id if hasattr(webhook, 'webhook_id') else 'pending_deletion',
            'url': webhook.webhook_url,
            'status': 'Error',
            'error': str(e),
            'ok': False
        }


async def test_all_webhooks():
    with Session() as session:
        """Test all webhooks with a delay between requests"""
        webhooks = session.query(Webhook).all()
        secondary = session.query(WebhookPendingDeletion).all()
        all_webhooks = secondary + webhooks
        
        print(f"Testing {len(all_webhooks)} webhooks...")
        
        results = []
        passed = 0
        failed = 0
        async with aiohttp.ClientSession() as http_session:
            for i, webhook in enumerate(all_webhooks):
                #print(f"Testing webhook {i+1}/{len(all_webhooks)}: {webhook.webhook_url}...")
                result = await test_webhook(webhook, http_session)
                results.append(result)
                
                # Print result immediately
                if result['ok']:
                    passed += 1
                else:
                    failed += 1
                    ## Remove it from the database
                    session.delete(webhook)
                    session.commit()
                
                # Add delay between requests (2 seconds)
                if i < len(all_webhooks) - 1:  # Don't delay after the last request
                    await asyncio.sleep(0.25)
        
        #print(f"Checked {len(all_webhooks)} webhooks: {passed} passed, {failed} failed")

async def check_limited_webhooks(limit=80, watchdog=None):
    """
    Check only a limited number of webhooks to ensure they're working before updating GitHub Pages.
    This removes non-working webhooks from the database.
    
    Args:
        limit: Maximum number of webhooks to check
        watchdog: SystemdWatchdog instance to notify during long operations
    """
    print(f"Checking up to {limit} webhooks before GitHub update...")
    try:
        with Session() as session:
            webhooks = session.query(Webhook).limit(limit).all()
            
            print(f"Testing {len(webhooks)} webhooks...")
            
            results = []
            passed = 0
            failed = 0
            async with aiohttp.ClientSession() as http_session:
                for i, webhook in enumerate(webhooks):
                    #print(f"Testing webhook {i+1}/{len(webhooks)}: {webhook.webhook_url}...")
                    result = await test_webhook(webhook, http_session)
                    results.append(result)
                    
                    # Print result immediatelyif i % 10 == 0:
                    #print(f"Checked {i+1}/{len(webhooks)} webhooks: so far, {passed} passed, {failed} failed")
                        
                    if result['ok']:
                        passed += 1
                    else:
                        failed += 1
                        ## Remove it from the database
                        session.delete(webhook)
                        session.commit()
                    
                    # The watchdog is automatically notified by the SystemdWatchdog heartbeat loop
                    # No manual notification needed
                    
                    # Add delay between requests
                    if i < len(webhooks) - 1:  # Don't delay after the last request
                        await asyncio.sleep(0.25)
            
            #print(f"Checked {len(webhooks)} webhooks: {passed} passed, {failed} failed")
        print("Limited webhook check completed")
    except Exception as e:
        print(f"Error checking webhooks: {e}")

async def check_webhooks():
    """
    Check all webhooks to ensure they're working before updating GitHub Pages.
    This removes non-working webhooks from the database.
    """
    print("Checking webhooks before GitHub update...")
    await test_all_webhooks()
    print("Webhook check completed")