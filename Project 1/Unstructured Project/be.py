
from autogen_agentchat.agents import CodeExecutorAgent
from autogen_ext.code_executors.docker import DockerCommandLineCodeExecutor
from autogen_agentchat.messages import TextMessage
from autogen_core import CancellationToken


docker=DockerCommandLineCodeExecutor(
    work_dir='tmp',
    timeout=120
)

code_executor_agent = CodeExecutorAgent(
    name='CodeExecutorAgent',
    code_executor=docker,
)

task = TextMessage(
        content='''Here is some code
```python
print('Hello woooooooooooorld')
```
''',
        source="user",
    )

async def run_code_executor_agent():
    try:
        await docker.start()
        result = await code_executor_agent.run(
            task=task,
            cancellation_token=CancellationToken(),
        )
        print(result)
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        await docker.stop()


if __name__=='__main__':
    import asyncio
    asyncio.run(run_code_executor_agent())
    print("Code execution completed.")