from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from web.api.dashboard import legacy_router as dashboard_legacy_router
from web.api.dashboard import router as dashboard_router
from web.api.meta import router as meta_router
from web.pages import PAGES


def create_app() -> FastAPI:
    app = FastAPI(title="YARN 资源看板")
    app.mount("/static", StaticFiles(directory="web/static"), name="static")
    templates = Jinja2Templates(directory="web/templates")

    app.include_router(meta_router)
    app.include_router(dashboard_router)
    app.include_router(dashboard_legacy_router)

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request) -> HTMLResponse:
        return templates.TemplateResponse("index.html", {"request": request, "pages": PAGES})

    return app


app = create_app()
