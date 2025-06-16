# Telegram Bulk Messenger

A responsible Telegram bulk messaging script that respects rate limits and follows best practices.

## Features
- Rate limiting (1 message per minute)
- Progress tracking and resume capability
- Error handling and retry logic
- Comprehensive logging
- User validation

## Quick Start
1. Install dependencies: `pip install -r requirements.txt`
2. Configure `.env` file with your API credentials
3. Add users to `data/users.csv`
4. Set your message in `data/message.txt`
5. Run: `python main.py`

## Important
- Only message users who have consented
- Comply with local laws and Telegram's ToS
- Test with small batches first
EOF

echo "Project structure created successfully!"
echo "Next steps:"
echo "1. Edit .env file with your actual API credentials"
echo "2. Add your users to data/users.csv"
echo "3. Customize your message in data/message.txt"
echo "4. Run: python -m venv telegram_env"
echo "5. Activate virtual environment and install dependencies"