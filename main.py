from fastapi import FastAPI
from sqlalchemy.engine import LegacyRow
from starlette.responses import RedirectResponse

from app.models.api_models import ScopeBoxParams
from app.models.database import Base, engine, get_raw_query_qs

from app.utils.scope_box import ScopeBox

Base.metadata.create_all(bind=engine)

app = FastAPI()


@app.get("/")
def read_root():
    response = RedirectResponse(url='/docs')
    return response


@app.post("/query")
def recommended_scope_box_data(params: ScopeBoxParams):
    # scope_box = ScopeBox(params)
    # scope_box.execute_queryies()

    query = f'SELECT * FROM usp_get_recommended_scopebox_data( ' \
            f'"p_project_id" := {params.project_id}, ' \
            f'"p_scopebox_id" := {params.scope_box_id},' \
            f'"p_file_id" := {params.file_id},"p_regenerate_status" := {params.regenerate_status})'
    qs = get_raw_query_qs(query)
    res = []
    obj: LegacyRow
    for obj in qs:
        res.append(dict(obj))
    return {"msg": "done", "payload": res}
