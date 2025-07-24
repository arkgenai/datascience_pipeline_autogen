import streamlit as st
import asyncio
import os
from config.docker_utils import get_docker_executor,start_docker_executor,stop_docker_executor
from config.model_client import get_model_client
from autogen_agentchat.base import TaskResult
from autogen_agentchat.messages import TextMessage
from agents.data_analyzer_agent import getDataAnalyzerAgent
from config.constants import DOCKER_WORK_DIR
from team.analyzer_gpt_team import getDataAnalyzerTeam

st.title('Analyzer GPT - Data analyzer')

if 'messages' not in st.session_state:
    st.session_state['messages'] = []
if 'autogen_team_state' not in st.session_state:
    st.session_state.autogen_team_state =  None #save_to_db(username,session_id,None)


file = st.file_uploader("Upload a CSV file", type=["csv"])

task = st.chat_input("Enter your task")

async def run_agent_team(docker,model_client,task):

    try:
        await start_docker_executor(docker)
        data_analyzer_team = getDataAnalyzerTeam(docker,model_client)

        if st.session_state.autogen_team_state is not None:
            await data_analyzer_team.load_state(st.session_state.autogen_team_state)

        async for message in data_analyzer_team.run_stream(task=task):

            if isinstance(message, TextMessage):
                st.markdown(msg:= f" {message.source}: {message.content}")
                # st.markdown(msg)
                st.session_state.messages.append(msg)

            elif isinstance(message, TaskResult):
                st.markdown(msg:=f'Task Result: {message.stop_reason}')
                st.session_state.messages.append(msg)
            
                

        
        st.session_state.autogen_team_state = await data_analyzer_team.save_state()


    except Exception as e:
        st.error(e)
    finally:
        await stop_docker_executor(docker)


if st.session_state.messages:
    for msg in st.session_state.messages:
        st.markdown(msg)

if task:
    try:
        if file is not None and task:
            if not os.path.exists(DOCKER_WORK_DIR):
                os.makedirs(DOCKER_WORK_DIR)

            with open(f"{DOCKER_WORK_DIR}/data.csv", "wb") as f:
                f.write(file.getbuffer())

        openai_model_client=get_model_client()
        docker = get_docker_executor()

        asyncio.run(run_agent_team(docker,openai_model_client,task))

        if os.path.exists(f'{DOCKER_WORK_DIR}/output.png'):
            st.image(f'{DOCKER_WORK_DIR}/output.png',caption='Generated Image')
            


    except  Exception as e:
        st.error("Please upload a file and enter a task")   