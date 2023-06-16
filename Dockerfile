# Start from a core stack version
FROM jupyter/scipy-notebook:latest

COPY --chown=${NB_UID}:${NB_GID} pyproject.toml "/home/${NB_USER}/work"
COPY --chown=${NB_UID}:${NB_GID} poetry.lock "/home/${NB_USER}/work"

WORKDIR "/home/${NB_USER}/work"
RUN pip install --no-cache-dir 'poetry' && \
    poetry config installer.max-workers 10 && \
    poetry install --no-cache --no-root --no-ansi -vvv && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"
