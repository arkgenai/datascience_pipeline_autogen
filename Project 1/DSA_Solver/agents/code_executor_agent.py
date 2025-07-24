from autogen_agentchat.agents import CodeExecutorAgent



def get_code_executor_agent(docker_executor):
    """
    Returns an instance of the CodeExecutorAgent configured to execute code in a Docker container.
    
    Args:
        docker_executor (DockerCommandLineCodeExecutor): The Docker command line code executor to use.
    
    Returns:
        CodeExecutorAgent: Configured code executor agent.
    """
    
    code_executor_agent = CodeExecutorAgent(
        name='CodeExecutorAgent',
        description="An agent that executes code in a Docker container.",
        code_executor=docker_executor,
    )
    
    return code_executor_agent