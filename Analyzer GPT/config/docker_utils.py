from autogen_ext.code_executors.docker import DockerCommandLineCodeExecutor

from config.constants import DOCKER_TIMEOUT,DOCKER_WORK_DIR


def get_docker_executor():
    """
    Returns a DockerCommandLineCodeExecutor instance configured with the specified work directory and timeout.
    
    Returns:
        DockerCommandLineCodeExecutor: Configured Docker command line code executor.
    """
    docker_executor = DockerCommandLineCodeExecutor(
        image='amancevice/pandas',
        work_dir=DOCKER_WORK_DIR,
        timeout=DOCKER_TIMEOUT
    )
    return docker_executor


async def start_docker_executor(docker_executor):
    """
    Starts the Docker command line code executor.
    
    Args:
        docker_executor (DockerCommandLineCodeExecutor): The Docker command line code executor to start.
    """
    await docker_executor.start()
    print("Docker executor started.")

async def stop_docker_executor(docker_executor):
    """
    Stops the Docker command line code executor.
    Args:
        docker_executor (DockerCommandLineCodeExecutor): The Docker command line code executor to stop.
    """
    await docker_executor.stop()
    print("Docker executor stopped.")