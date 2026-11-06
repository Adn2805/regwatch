"""RegWatch read API. Importing the app does not connect to external services."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from app.api.repository import EntityNotFound
from app.api.routers import router
from app.core.db import make_engine


def create_app(sessions: sessionmaker[Session] | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        if sessions is not None:
            application.state.sessions = sessions
            yield
        else:
            engine = make_engine()
            application.state.sessions = sessionmaker(engine)
            try:
                yield
            finally:
                engine.dispose()

    application = FastAPI(title="RegWatch", version="0.1.0", lifespan=lifespan, redoc_url=None)

    @application.exception_handler(EntityNotFound)
    async def not_found(request: Request, error: EntityNotFound) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(error)})

    @application.exception_handler(SQLAlchemyError)
    async def database_unavailable(request: Request, error: SQLAlchemyError) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": "Database unavailable"})

    application.include_router(router)
    return application


app = create_app()
