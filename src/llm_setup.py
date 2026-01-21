"""
LLM Setup Module - Azure OpenAI Configuration
Configures the Azure OpenAI LLM for the ETL Agent
"""

import os
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI


def get_llm() -> AzureChatOpenAI:
    """
    Initialize and return Azure OpenAI LLM instance.
    
    Returns:
        AzureChatOpenAI: Configured LLM instance
    """
    load_dotenv()
    
    llm = AzureChatOpenAI(
        deployment_name=os.getenv("OPENAI_DEPLOYMENT_NAME"),
        model_name=os.getenv("OPENAI_MODEL_NAME"),
        azure_endpoint=os.getenv("OPENAI_DEPLOYMENT_ENDPOINT"),
        openai_api_version=os.getenv("OPENAI_DEPLOYMENT_VERSION"),
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        temperature=0  # Deterministic for ETL decisions
    )
    
    return llm


if __name__ == "__main__":
    # Test the LLM connection
    llm = get_llm()
    response = llm.invoke("Say 'ETL Agent Ready!' if you can hear me.")
    print(response.content)
