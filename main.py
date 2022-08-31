from dotenv import load_dotenv
from flask import Flask
from controller import process_bp

load_dotenv()

app = Flask(__name__)
app.register_blueprint(process_bp)

if __name__ == '__main__':
    app.run()
