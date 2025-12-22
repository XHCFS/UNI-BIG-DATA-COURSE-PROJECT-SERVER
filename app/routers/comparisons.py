from fastapi import APIRouter, Query
from app.spark import spark

router = APIRouter(prefix="/comparisons", tags=["comparisons"])