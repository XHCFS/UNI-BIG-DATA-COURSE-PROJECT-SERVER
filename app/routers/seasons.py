from fastapi import APIRouter, Query
from app.spark import spark

router = APIRouter(prefix="/seasons", tags=["seasons"])
