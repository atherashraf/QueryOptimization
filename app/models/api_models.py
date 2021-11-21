from pydantic import BaseModel


class ScopeBoxParams(BaseModel):
    project_id: int
    scope_box_id: int
    file_id: int
    regenerate_status: bool

    class Config:
        schema_extra = {
            "example": {
                "project_id": 458,
                "scope_box_id": 44564,
                "file_id": 1804,
                "regenerate_status": True
            }
        }

    pass
