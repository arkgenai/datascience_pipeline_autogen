from autogen_agentchat.agents import AssistantAgent



def get_problem_solver_expert(model_client):
    """
    Returns an instance of the ProblemSolverExpert agent configured to solve DSA problems.
    
    Returns:
        AssistantAgent: Configured problem solver expert agent.
    """

    problem_solver_expert = AssistantAgent(
        name='ProblemSolverExpert',
        description="An expert agent that solves problems using code execution.",
        model_client=model_client,
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
        'Once the code is executed successfully,you should also give the code to save the file as solutions.py and make it run strictly via Code Executor Agent like below  after the first check i.e. after the solution actually works.'\
        'The code to save should be in Python Block format' \
        'like this: '
        ' ```python' \
        ' code = "print(Hello World)" ' \
        'with open("solutions.py", "w") as f:'\
        'f.write(code) ' \
        '```' \
        'Make sure to send above code to code executor agent to save the file before you finish' \
        'Once everything is done, you should explain the results and say "STOP" to stop the conversation'
    )

    return problem_solver_expert
