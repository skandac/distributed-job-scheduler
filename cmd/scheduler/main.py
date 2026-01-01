from fastapi import FastAPI
from internal.api.scheduler_api import router as scheduler_router
app = FastAPI(title="Scheduler Service")

# Attach HTTP API
app.include_router(scheduler_router)
