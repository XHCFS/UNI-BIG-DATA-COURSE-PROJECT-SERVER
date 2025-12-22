from fastapi import APIRouter, Query
from app.spark import spark

router = APIRouter(prefix="/extreme_events", tags=["extreme_events"])
