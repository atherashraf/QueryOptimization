CREATE MATERIALIZED VIEW mt_view_faimly AS
		SELECT unnest(string_to_array( LEFT(OL.all_objectids,-1),',')::integer[]) AS objectid,
			RCM.fk_fam_comp_id, OL.fk_familytype_id, FL.family_type,FN.family_name, FCM.component_name,
			rcm.deleted AS rcm_deleted,
    fl.fk_fileid AS fl_fk_fileid,
    fn.fk_fileid AS fn_fk_fileid,
    ol.fk_fileid AS ol_fk_fileid
		FROM tbl_dextract_objectidslist OL
			LEFT JOIN tbl_dextract_familytypelist FL
				ON FL.pk_familytype_id=OL.fk_familytype_id
			LEFT JOIN tbl_dextract_familynamelist FN
				ON FN.pk_familyname_id=FL.fk_family_id
			LEFT JOIN tbl_revit_inspire_component_mapping RCM
				ON RCM.revit_familytype_id=OL.fk_familytype_id
			JOIN tbl_family_component_master FCM
					ON FCM.pk_fam_comp_id = RCM.fk_fam_comp_id