# app.py
import os
import uuid
from flask import Flask, render_template, request, jsonify, url_for
from dotenv import load_dotenv

load_dotenv()
from bot_logic import BotLogic, logger

app = Flask(__name__)
# It's good practice to have a secret key for Flask sessions, even if not used heavily yet.
app.secret_key = os.getenv("FLASK_SECRET_KEY", "a-strong-default-secret-key-for-development")

bot_logic = BotLogic()
logger.info("BotLogic initialized for the web application.")

@app.route('/')
def index():
    user_id = str(uuid.uuid4())
    initial_response = bot_logic.process_user_input(
        user_id=user_id, input_type='command', data='start', user_name='Visitor'
    )
    return render_template(
        'index.html', user_id=user_id, 
        initial_text=initial_response.get("text", "Hello!"),
        initial_buttons=initial_response.get("buttons", [])
    )

@app.route('/ask', methods=['POST'])
def ask():
    data = request.get_json()
    user_input = data.get('question', '').strip()
    user_id = data.get('user_id')
    user_name = 'Visitor'

    if not user_id:
        return jsonify({'error': 'Missing user_id'}), 400
    if not user_input:
        return jsonify({'text': 'Please type a message.'})

    response_dict = bot_logic.process_user_input(
        user_id=user_id, input_type='text', data=user_input, user_name=user_name
    )

    # Convert relative photo paths to full, usable URLs
    if 'photos' in response_dict and response_dict['photos']:
        response_dict['photos'] = [
            url_for('static', filename=path) for path in response_dict['photos']
        ]

    return jsonify(response_dict)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
