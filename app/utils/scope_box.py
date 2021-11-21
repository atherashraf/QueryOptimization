from app.models.api_models import ScopeBoxParams
from app.models.database import get_raw_query_qs


class ScopeBox():
    params: ScopeBoxParams
    depth_of_slab = None
    is_eight_ft_above = None
    slabmaterial = None
    globaloffset_x = None
    globaloffset_y = None
    globaloffset_z = None
    jbox_comp = []
    sup_comp = []
    eq_comp = []
    tray_comp_ex_sl = []
    iwd_comp = []
    lf_comp = []
    fd_comp = []
    length_of_rods_parameter = []

    def __init__(self, params):
        self.params = params

    def execute_queryies(self):
        query = f"SELECT ROUND(CAST(tf.globaloffset->>'x' AS NUMERIC), 9)*(-1) as X, " \
                f"ROUND(CAST(tf.globaloffset->>'y' AS NUMERIC), 9)*(-1) as Y, " \
                f"ROUND(CAST(tf.globaloffset->>'z' AS NUMERIC), 9)*(-1) as Z " \
                f"FROM tbl_files tf WHERE tf.pk_fileid = {self.params.file_id} AND fk_file_type_id = 1;"
        qs = get_raw_query_qs(query)
        for obj in qs:
            # print(obj)
            self.globaloffset_x = obj.X
            self.globaloffset_y = obj.Y
            self.globaloffset_z = obj.Z
        sql_jbox_comp = f"SELECT array_agg(fcm.pk_fam_comp_id) as jbox_comp FROM tbl_family_component_master fcm " \
                        f"WHERE fcm.revitbased = true AND fcm.fk_family_id = 2 AND pk_fam_comp_id NOT IN (21,30)"
        qs = get_raw_query_qs(sql_jbox_comp)
        for obj in qs:
            self.jbox_comp = obj.jbox_comp
        sql_sup_comp = "SELECT array_agg(fcm.pk_fam_comp_id) as sup_comp FROM tbl_family_component_master fcm " \
                       "WHERE fcm.revitbased = true AND fcm.fk_family_id = 3"
        qs = get_raw_query_qs(sql_sup_comp)
        for obj in qs:
            self.sup_comp = obj.sup_comp
        sql_eq_comp = "SELECT array_agg(fcm.pk_fam_comp_id) as eq_comp FROM tbl_family_component_master fcm " \
                      "WHERE fcm.revitbased = true AND fcm.fk_family_id = 4"
        qs = get_raw_query_qs(sql_eq_comp)
        for obj in qs:
            self.eq_comp = obj.eq_comp
        sql_tray_comp_ex_sl = "SELECT array_agg(fcm.pk_fam_comp_id) as tray_comp_ex_sl FROM tbl_family_component_master fcm " \
                              "WHERE fcm.revitbased = true AND fcm.fk_family_id = 5 AND fcm.pk_fam_comp_id NOT IN (43,51,54)"
        qs = get_raw_query_qs(sql_tray_comp_ex_sl)
        for obj in qs:
            self.tray_comp_ex_sl = obj.tray_comp_ex_sl
        sql_iwd_comp = "SELECT array_agg(fcm.pk_fam_comp_id) as iwd_comp FROM tbl_family_component_master fcm " \
                       "WHERE fcm.revitbased = true AND fcm.fk_family_id = 6"
        qs = get_raw_query_qs(sql_iwd_comp)
        for obj in qs:
            self.iwd_comp = obj.iwd_comp
        sql_lf_comp = "SELECT array_agg(fcm.pk_fam_comp_id) as lf_comp FROM tbl_family_component_master fcm " \
                      "WHERE fcm.revitbased = true AND fcm.fk_family_id = 7"
        qs = get_raw_query_qs(sql_lf_comp)
        for obj in qs:
            self.lf_comp = obj.lf_comp
        sql_fd_comp = "SELECT array_agg(fcm.pk_fam_comp_id) as fd_comp FROM tbl_family_component_master fcm " \
                      "WHERE fcm.revitbased = true AND fcm.fk_family_id = 8"
        qs = get_raw_query_qs(sql_fd_comp)
        for obj in qs:
            self.fd_comp = obj.fd_comp
        self.length_of_rods_parameter = [26, 44, 51, 246, 258, 267, 276, 968, 65, 66, 74, 75, 83, 84, 93, 94, 404, 405,
                                         417, 418]
        # vol_comp = (self.jbox_comp || self.sup_comp || self.tray_comp_ex_sl | | self.iwd_comp || self.lf_comp || self.fd_comp || self.eq_comp)
        # ug_slab_excep_comp = (jbox_comp || sup_comp || tray_comp_ex_sl | | iwd_comp || lf_comp);
        sql_depth_of_slab = f"SELECT t1.depth_of_slab::numeric/12 * -1 as depth_of_slab FROM tbl_pc_project_details t1 " \
                            f"WHERE t1.fk_project_id = {self.params.project_id} AND t1.depth_of_slab > 0 LIMIT 1"
        qs = get_raw_query_qs(sql_depth_of_slab)
        for obj in qs:
            self.depth_of_slab = obj.depth_of_slab
        sql = f"SELECT CASE WHEN flushmount_panel_connection_status = 1 THEN true else false END FROM " \
              f"tbl_pc_project_details t1 WHERE t1.fk_project_id = {self.params.project_id} LIMIT 1"
        qs = get_raw_query_qs(sql)
        for obj in qs:
            self.is_eight_ft_above = obj.case
        sql = f"SELECT material_of_slab FROM tbl_pc_project_details WHERE fk_project_id = {self.params.project_id} LIMIT 1"
        qs = get_raw_query_qs(sql)
        for obj in qs:
            self.slabmaterial = obj.material_of_slab
        print('Initiating Stage (%)')
        sql = f"CREATE temp table comp_data ON COMMIT DROP AS select * from mt_view_faimly " \
              f"WHERE rcm_deleted = false AND {self.params.file_id} IN (SELECT unnest(fl_fk_fileid)) " \
              f"AND {self.params.file_id} IN (SELECT unnest(fn_fk_fileid)) AND ol_fk_fileid = {self.params.file_id};"
        get_raw_query_qs(sql)
        sql = f"CREATE index inx_temp_comp_id ON comp_data (objectid, fk_fam_comp_id) WHERE fk_fam_comp_id IS NOT NULL;"
        # get_raw_query_qs(sql)
        print('Fetched component mapping data (%)')

    def fetching_component_mapping_data(self):
        sql = "CREATE temp table temp_box_intersection ON COMMIT DROP AS "
