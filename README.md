# Graph Machine Learning: Theory, Practice, Tools and Techniques

This is a course from Graphlet AI taught by Russell Jurney.

## Environment Setup

This project uses Docker: `docker compose up -d && docker logs jupyter -f --tail 100` and visit the url it displays.

## VSCode Setup

To edit code in VSCode you may want a local Anaconda Python environment with the class's PyPi libraries installed. 
This will enable VSCode to parse the code, understand APIs and highlight errors.

Note: if you do not use Anaconda, consider using it :) You can use a Python 3 [venv](https://docs.python.org/3/library/venv.html) in the same way as `conda`.

### Class Anaconda Environment

Create a new Anaconda environment:

```bash
conda create -n graphml python=3.10 -y
```

Activate the environment:

```bash
conda activate graphml
```

Install the project's libraries:

```bash
pip install -r requirements.txt
```

### VSCode Interpretter

You can use a [Python environment](https://code.visualstudio.com/docs/python/environments) in VSCode by typing:

```
SHIFT-CMD-P
```

to bring up a command search window. Now type `Python` or `Interpreter` or if you see it, select `Python: Select Interpreter`. Now choose the path to your conda environment. It will end with the name of the environment, such as 
