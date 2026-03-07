from __future__ import annotations

from fastapi import APIRouter

from web.pages import PAGES

router = APIRouter(prefix="/api/meta", tags=["meta"])


@router.get('/pages')
def get_pages():
    return PAGES
