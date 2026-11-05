FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY app ./app
RUN pip install --no-cache-dir '.[dev]'
COPY alembic.ini ./
COPY migrations ./migrations
COPY tests ./tests
CMD ["alembic", "upgrade", "head"]
