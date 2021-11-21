from app.models.datatype import ScopeBoxParams


class ScopeBox():
    params: ScopeBoxParams

    def __init__(self, params):
        self.params = params
