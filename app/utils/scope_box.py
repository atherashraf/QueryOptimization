from app.models.api_models import ScopeBoxParams
from app.models.database import get_raw_query_qs


class ScopeBox():
    params: ScopeBoxParams

    def __init__(self, params):
        self.params = params

    def get_global_offeset_of_3D_files(self):
        query = "SELECT ROUND(CAST(tf.globaloffset->>'x' AS NUMERIC), 9)*(-1)," \
                "ROUND(CAST(tf.globaloffset->>'y' AS NUMERIC), 9)*(-1)," \
                "ROUND(CAST(tf.globaloffset->>'z' AS NUMERIC), 9)*(-1)" \
                "FROM tbl_files tf WHERE tf.pk_fileid = p_file_id AND fk_file_type_id = 1;"
        qs = get_raw_query_qs(query)
        for obj in qs:
            print(obj)
