from fastapi import FastAPI

from app.routers import (
    overview,
    map,
    trends,
    seasons,
    extreme_events,
    statistics,
    comparisons,
    coverage,
)

app = FastAPI(title="GHCND API")

app.include_router(overview.router)
app.include_router(map.router)
app.include_router(trends.router)
app.include_router(seasons.router)
app.include_router(extreme_events.router)
app.include_router(statistics.router)
app.include_router(comparisons.router)
app.include_router(coverage.router)

# test
@app.get("/test")
def test_connection():
    return "HI :>"
