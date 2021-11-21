from fastapi import FastAPI
from starlette.responses import RedirectResponse

from app.models.api_models import ScopeBoxParams
from app.models.database import Base, engine

from app.utils.scope_box import ScopeBox

Base.metadata.create_all(bind=engine)

app = FastAPI()


@app.get("/")
def read_root():
    response = RedirectResponse(url='/docs')
    return response


@app.get("/query")
def recommended_scope_box_data(params: ScopeBoxParams):
    scope_box = ScopeBox(params)
    return {"msg": "done"}
