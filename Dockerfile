# Start from a core stack version
FROM jupyter/scipy-notebook:python-3.10.11

ENV POETRY_VIRTUALENVS_CREATE=false \
    POETRY_VERSION=1.4.2 \
    GRANT_SUDO=yes

USER root
RUN sudo apt update && \
    sudo apt upgrade -y && \
    sudo apt install curl -y && \
    rm -rf /var/lib/apt/lists/*

USER ${NB_USER}
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH "/home/jovyan/.local/bin:$PATH"

COPY --chown=${NB_UID}:${NB_GID} pyproject.toml "/home/${NB_USER}/work/"
COPY --chown=${NB_UID}:${NB_GID} poetry.lock    "/home/${NB_USER}/work/"

WORKDIR "/home/${NB_USER}/work"
RUN poetry config virtualenvs.create false && \
    poetry config installer.max-workers 10 && \
    poetry install --no-interaction --no-ansi --no-root -vvv
