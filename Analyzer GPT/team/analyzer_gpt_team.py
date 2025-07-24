from agents.data_analyzer_agent import getDataAnalyzerAgent
from agents.code_executor_agent import getCodeExecutorAgent
from autogen_agentchat.teams import RoundRobinGroupChat,SelectorGroupChat
from autogen_agentchat.conditions import TextMentionTermination
from config.constants import TEXT_MENTION_TERMINATION


def getDataAnalyzerTeam(docker,model_client):
    code_executor_agent = getCodeExecutorAgent(docker)
    data_analyzer_agent = getDataAnalyzerAgent(model_client)

    team= SelectorGroupChat(
        participants=[data_analyzer_agent,code_executor_agent],
        max_turns=10,
        termination_condition=TextMentionTermination(TEXT_MENTION_TERMINATION),
        model_client=model_client,
        
        allow_repeated_speaker=True,
        selector_prompt= 'Select the agent that is most relevant to the task. You got Data analyzer agent for logic and code which will be send to code executor agent to execute the code and generate the result. and when you see the code given by data analyzer agent you will send it to code executor agent for execution. when code executor agent will execute the code and generate the result you will send it to data analyzer agent and data analyzer agent will generate the result and send it to you.' \
        '' \
        'for general query nd it to data analyzer agent and when you see the code given by data analyzer agent you will send it to code executor agent for execution. when code executor agent will execute the code and generate the result you will send it to data analyzer agent and data analyzer agent will generate the result and send it to you.' \
    )

    return team

