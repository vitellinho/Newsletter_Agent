import os
from dotenv import load_dotenv

# Absoluten Pfad zur .env laden, egal wo das Skript liegt
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

api_key = os.getenv("OPENAI_API_KEY")

if not api_key:
    raise ValueError("OPENAI_API_KEY ist nicht gesetzt")

print(api_key)
