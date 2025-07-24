import os
from dotenv import load_dotenv
from autogen_ext.models.openai import OpenAIChatCompletionClient
from config.constants import MODEL, OPENAI_API_KEY
# Load environment variables
load_dotenv()

api_key = os.getenv(OPENAI_API_KEY)


def get_model_client():
    if not api_key:
        raise ValueError(f"Please set the {OPENAI_API_KEY} environment variable.")
    
    # Initialize the OpenAI model client
    model_client = OpenAIChatCompletionClient(model=MODEL, api_key=api_key)
    return model_client