
from autogen_agentchat.agents import CodeExecutorAgent
from autogen_ext.code_executors.docker import DockerCommandLineCodeExecutor
from autogen_agentchat.messages import TextMessage
from autogen_core import CancellationToken
import asyncio
from autogen_agentchat.agents import AssistantAgent
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_core.tools import FunctionTool
import os
from autogen_agentchat.teams import RoundRobinGroupChat
from autogen_agentchat.conditions import TextMentionTermination
from dotenv import load_dotenv
from autogen_agentchat.base import TaskResult

# Load environment variables
# load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("Please set the OPENAI_API_KEY environment variable.")

# Initialize the OpenAI model client
openai_client = OpenAIChatCompletionClient(model="gpt-4o-mini", api_key=api_key)


problem_solver_expert = AssistantAgent(
    name='ProblemSolverExpert',
    description="An expert agent that solves problems using code execution.",
    model_client=openai_client,
    system_message='You are a problem solver agent that is an expert in solving DSA problems,' \
    'You will be working with code executor agent to execute code' \
    'You will be give a task and you should first provide a way to solve the task/problem' \
    'Then you should give the code in Python Block format so that it can be ran by code executor agent' \
    'You can provide Shell scipt as well if code fails due to missing libraries, make sure to use pip install command' \
    'You should only give a single code block and pass it to executor agent'\
    ' You should give the corrected code in Python Block format if error is there' \
    'Once the code has been successfully executed and you have the results. You should explain the results in detail' \
    'Make sure each code has 3 test cases and the output of each test case is printed' \
    'if you have to save the file, save it with output.png or output.txt or output.gif' \
    'Once everything is done, you should explain the results and say "STOP" to stop the conversation'
)

termination_condition = TextMentionTermination('STOP')

docker=DockerCommandLineCodeExecutor(
    work_dir='tmp',
    timeout=120
)

code_executor_agent = CodeExecutorAgent(
    name='CodeExecutorAgent',
    description="An agent that executes code in a Docker container.",
    code_executor=docker,
)

team = RoundRobinGroupChat(
    participants=[problem_solver_expert, code_executor_agent],
    termination_condition=termination_condition,
    max_turns=15)

async def run_code_executor_agent():
    try:
        await docker.start()

        task = 'Write a Python Code to check and solve rat in a maze small problem, generate gif which should move slow so that it is visible.'

        async for message in team.run_stream(task = task):
            print('='*200)
            if isinstance(message, TextMessage):
                print("Message from:", message.source)
                print("Content:", message.content)
            elif isinstance(message, TaskResult):
                print (message.stop_reason)
            print('='*200)


    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        await docker.stop()

if __name__=='__main__':
    import asyncio
    asyncio.run(run_code_executor_agent())
    print("Code execution completed.")