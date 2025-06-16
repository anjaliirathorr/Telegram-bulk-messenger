import asyncio
import csv
import logging
import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError, 
    UserPrivacyRestrictedError, 
    UserNotMutualContactError,
    PeerIdInvalidError,
    ChatWriteForbiddenError
)
from telethon.tl.types import User

# Configuration
@dataclass
class Config:
    api_id: int
    api_hash: str
    phone_number: str
    session_name: str = "bulk_sender"
    message_delay: int = 60  # seconds between messages
    max_retries: int = 3
    batch_size: int = 100  # Process users in batches

class TelegramBulkSender:
    def __init__(self, config: Config):
        self.config = config
        self.client = TelegramClient(
            config.session_name, 
            config.api_id, 
            config.api_hash
        )
        self.setup_logging()
        self.stats = {
            'sent': 0,
            'failed': 0,
            'skipped': 0,
            'total': 0
        }
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/telegram_bulk.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def load_users_from_csv(self, file_path: str) -> List[str]:
        """Load user IDs/usernames from CSV file"""
        users = []
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                next(reader)  # Skip header
                for row in reader:
                    if row and row[0].strip():
                        users.append(row[0].strip())
            self.logger.info(f"Loaded {len(users)} users from {file_path}")
            return users
        except Exception as e:
            self.logger.error(f"Error loading users from CSV: {e}")
            return []

    def load_message_template(self, file_path: str) -> str:
        """Load message template from file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                message = file.read().strip()
            self.logger.info(f"Loaded message template from {file_path}")
            return message
        except Exception as e:
            self.logger.error(f"Error loading message template: {e}")
            return ""

    def save_progress(self, sent_users: List[str], failed_users: List[Dict]):
        """Save progress to JSON files"""
        try:
            # Save sent users
            with open('data/sent_users.json', 'w') as f:
                json.dump(sent_users, f, indent=2)
            
            # Save failed users
            with open('data/failed_users.json', 'w') as f:
                json.dump(failed_users, f, indent=2)
                
            self.logger.info("Progress saved successfully")
        except Exception as e:
            self.logger.error(f"Error saving progress: {e}")

    def load_progress(self) -> tuple:
        """Load previous progress"""
        sent_users = []
        failed_users = []
        
        try:
            if os.path.exists('data/sent_users.json'):
                with open('data/sent_users.json', 'r') as f:
                    sent_users = json.load(f)
                    
            if os.path.exists('data/failed_users.json'):
                with open('data/failed_users.json', 'r') as f:
                    failed_users = json.load(f)
                    
            self.logger.info(f"Loaded progress: {len(sent_users)} sent, {len(failed_users)} failed")
        except Exception as e:
            self.logger.error(f"Error loading progress: {e}")
            
        return sent_users, failed_users

    async def validate_user(self, user_identifier: str) -> Optional[User]:
        """Validate if user exists and can be messaged"""
        try:
            # Try to get user entity
            user_entity = await self.client.get_entity(user_identifier)
            if isinstance(user_entity, User):
                return user_entity
            return None
        except Exception as e:
            self.logger.debug(f"Could not validate user {user_identifier}: {e}")
            return None

    async def send_message_with_retry(self, user_identifier: str, message: str) -> Dict:
        """Send message with retry logic and error handling"""
        result = {
            'user': user_identifier,
            'status': 'failed',
            'error': None,
            'timestamp': datetime.now().isoformat()
        }
        
        for attempt in range(self.config.max_retries):
            try:
                # Validate user first
                user_entity = await self.validate_user(user_identifier)
                if not user_entity:
                    result['error'] = 'User not found or invalid'
                    result['status'] = 'skipped'
                    return result
                
                # Send message
                await self.client.send_message(user_entity, message)
                result['status'] = 'sent'
                result['error'] = None
                self.stats['sent'] += 1
                self.logger.info(f"✓ Message sent to {user_identifier}")
                return result
                
            except FloodWaitError as e:
                wait_time = e.seconds
                self.logger.warning(f"Rate limit hit. Waiting {wait_time} seconds...")
                await asyncio.sleep(wait_time)
                continue
                
            except (UserPrivacyRestrictedError, UserNotMutualContactError):
                result['error'] = 'Privacy settings prevent messaging'
                result['status'] = 'skipped'
                self.stats['skipped'] += 1
                self.logger.warning(f"⚠ Cannot message {user_identifier}: Privacy restricted")
                return result
                
            except (PeerIdInvalidError, ChatWriteForbiddenError):
                result['error'] = 'Invalid peer or messaging forbidden'
                result['status'] = 'skipped'
                self.stats['skipped'] += 1
                self.logger.warning(f"⚠ Cannot message {user_identifier}: Invalid peer")
                return result
                
            except Exception as e:
                result['error'] = str(e)
                self.logger.error(f"✗ Error sending to {user_identifier}: {e}")
                if attempt == self.config.max_retries - 1:
                    self.stats['failed'] += 1
                    return result
                await asyncio.sleep(5)  # Short delay before retry
                
        return result

    async def send_bulk_messages(self, users: List[str], message: str, resume: bool = False):
        """Main function to send bulk messages"""
        sent_users, failed_users = [], []
        
        if resume:
            sent_users, failed_users = self.load_progress()
            # Filter out already processed users
            processed_users = set(sent_users + [f['user'] for f in failed_users])
            users = [user for user in users if user not in processed_users]
            self.logger.info(f"Resuming: {len(users)} users remaining")
        
        self.stats['total'] = len(users)
        
        if not users:
            self.logger.info("No users to process")
            return
            
        self.logger.info(f"Starting bulk messaging to {len(users)} users")
        self.logger.info(f"Rate limit: 1 message per {self.config.message_delay} seconds")
        
        start_time = datetime.now()
        
        for i, user in enumerate(users, 1):
            try:
                self.logger.info(f"Processing {i}/{len(users)}: {user}")
                
                # Send message
                result = await self.send_message_with_retry(user, message)
                
                if result['status'] == 'sent':
                    sent_users.append(user)
                elif result['status'] in ['failed', 'skipped']:
                    failed_users.append(result)
                
                # Save progress every 10 messages
                if i % 10 == 0:
                    self.save_progress(sent_users, failed_users)
                    self.print_stats(i, len(users), start_time)
                
                # Rate limiting - wait between messages
                if i < len(users):  # Don't wait after the last message
                    self.logger.info(f"Waiting {self.config.message_delay} seconds...")
                    await asyncio.sleep(self.config.message_delay)
                    
            except KeyboardInterrupt:
                self.logger.info("Process interrupted by user")
                self.save_progress(sent_users, failed_users)
                break
            except Exception as e:
                self.logger.error(f"Unexpected error processing {user}: {e}")
                continue
        
        # Final save and statistics
        self.save_progress(sent_users, failed_users)
        self.print_final_stats(start_time)

    def print_stats(self, current: int, total: int, start_time: datetime):
        """Print current statistics"""
        elapsed = datetime.now() - start_time
        rate = current / elapsed.total_seconds() * 60 if elapsed.total_seconds() > 0 else 0
        eta = timedelta(seconds=(total - current) * self.config.message_delay)
        
        print(f"\n{'='*50}")
        print(f"Progress: {current}/{total} ({current/total*100:.1f}%)")
        print(f"Sent: {self.stats['sent']}")
        print(f"Failed: {self.stats['failed']}")
        print(f"Skipped: {self.stats['skipped']}")
        print(f"Rate: {rate:.1f} messages/minute")
        print(f"ETA: {eta}")
        print(f"{'='*50}\n")

    def print_final_stats(self, start_time: datetime):
        """Print final statistics"""
        elapsed = datetime.now() - start_time
        print(f"\n{'='*60}")
        print(f"BULK MESSAGING COMPLETED")
        print(f"{'='*60}")
        print(f"Total processed: {self.stats['total']}")
        print(f"Successfully sent: {self.stats['sent']}")
        print(f"Failed: {self.stats['failed']}")
        print(f"Skipped: {self.stats['skipped']}")
        print(f"Total time: {elapsed}")
        print(f"Success rate: {self.stats['sent']/self.stats['total']*100:.1f}%")
        print(f"{'='*60}")

async def main():
    """Main function"""
    # Load configuration
    config = Config(
        api_id=int(os.getenv('TELEGRAM_API_ID', '0')),
        api_hash=os.getenv('TELEGRAM_API_HASH', ''),
        phone_number=os.getenv('TELEGRAM_PHONE', ''),
        message_delay=60  # 1 minute between messages
    )
    
    if not all([config.api_id, config.api_hash, config.phone_number]):
        print("Error: Please set TELEGRAM_API_ID, TELEGRAM_API_HASH, and TELEGRAM_PHONE environment variables")
        return
    
    # Initialize sender
    sender = TelegramBulkSender(config)
    
    try:
        # Connect to Telegram
        await sender.client.start(phone=config.phone_number)
        sender.logger.info("Connected to Telegram successfully")
        
        # Load users and message
        users = sender.load_users_from_csv('data/users.csv')
        message = sender.load_message_template('data/message.txt')
        
        if not users:
            sender.logger.error("No users loaded")
            return
            
        if not message:
            sender.logger.error("No message template loaded")
            return
        
        # Ask user if they want to resume
        resume = input("Do you want to resume from previous progress? (y/n): ").lower() == 'y'
        
        # Confirm before starting
        print(f"\nReady to send messages to {len(users)} users")
        print(f"Message preview: {message[:100]}...")
        print(f"Rate limit: 1 message per {config.message_delay} seconds")
        
        if input("\nProceed? (y/n): ").lower() != 'y':
            print("Operation cancelled")
            return
        
        # Start bulk messaging
        await sender.send_bulk_messages(users, message, resume)
        
    except Exception as e:
        sender.logger.error(f"Fatal error: {e}")
    finally:
        await sender.client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())