from autogen_agentchat.agents import AssistantAgent

from agents.prompts.data_analyzer_prompt import DATA_ANALYZER_PROMPT

def getDataAnalyzerAgent(model_client):
    data_analyzer_agent = AssistantAgent(
        name = 'DataAnalyzerAgent',
        description = 'An agent that analyzes data and generates insights.',
        model_client=model_client,
        system_message=DATA_ANALYZER_PROMPT
    )

    return data_analyzer_agent