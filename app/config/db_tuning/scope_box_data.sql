
CREATE OR REPLACE FUNCTION public.usp_generate_scopebox_intersection_result(p_project_id integer, p_scopebox_id integer, p_file_id integer)
 RETURNS TABLE(objectid integer, obj_comp_id integer, ct integer, oc integer, at_start integer, at_end integer, in_mid integer[], revit_id integer, is_scopebox boolean, is_under_slab boolean, is_eqp_in_6 boolean, is_in_8_ft boolean, is_all_sb boolean, rec_specification text, is_vertical boolean, length_3d double precision, vol_3d numeric, angle numeric, prop_data json)
 LANGUAGE plpgsql
 PARALLEL SAFE ROWS 100000
AS $function$
DECLARE
depth_of_slab numeric;
is_eight_ft_above boolean;
slabmaterial text;

jbox_comp integer[] = '{}';
sup_comp integer[] = '{}';
eq_comp integer[] = '{}';
tray_comp_ex_sl integer[] = '{}';
iwd_comp integer[] = '{}';
lf_comp integer[] = '{}';
fd_comp integer[] = '{}';

length_of_rods_parameter integer[];
ug_slab_excep_comp integer[];
vol_comp integer[];

globaloffset_x numeric;
globaloffset_y numeric;
globaloffset_z numeric;

BEGIN

	-- get global offset of files 3D model
	SELECT ROUND(CAST(tf.globaloffset->>'x' AS NUMERIC), 9)*(-1),
			ROUND(CAST(tf.globaloffset->>'y' AS NUMERIC), 9)*(-1),
			ROUND(CAST(tf.globaloffset->>'z' AS NUMERIC), 9)*(-1)
		    into globaloffset_x, globaloffset_y, globaloffset_z
		FROM tbl_files tf WHERE tf.pk_fileid = p_file_id AND fk_file_type_id = 1;

	-- comp ids
	jbox_comp:= (SELECT array_agg(fcm.pk_fam_comp_id) FROM tbl_family_component_master fcm
						  WHERE fcm.revitbased = true AND fcm.fk_family_id = 2 AND pk_fam_comp_id NOT IN (21,30));
	sup_comp:=(SELECT array_agg(fcm.pk_fam_comp_id) FROM tbl_family_component_master fcm
						  WHERE fcm.revitbased = true AND fcm.fk_family_id = 3);
	eq_comp:=(SELECT array_agg(fcm.pk_fam_comp_id) FROM tbl_family_component_master fcm
						  WHERE fcm.revitbased = true AND fcm.fk_family_id = 4);
	tray_comp_ex_sl:=(SELECT array_agg(fcm.pk_fam_comp_id) FROM tbl_family_component_master fcm
						  WHERE fcm.revitbased = true AND fcm.fk_family_id = 5 AND fcm.pk_fam_comp_id NOT IN (43,51,54));
	iwd_comp:=(SELECT array_agg(fcm.pk_fam_comp_id) FROM tbl_family_component_master fcm
						  WHERE fcm.revitbased = true AND fcm.fk_family_id = 6);
	lf_comp:=(SELECT array_agg(fcm.pk_fam_comp_id) FROM tbl_family_component_master fcm
						  WHERE fcm.revitbased = true AND fcm.fk_family_id = 7);
	fd_comp:=(SELECT array_agg(fcm.pk_fam_comp_id) FROM tbl_family_component_master fcm
						  WHERE fcm.revitbased = true AND fcm.fk_family_id = 8);

	length_of_rods_parameter:= ARRAY[26,44,51,246,258,267,276,968,65,66,74,75,83,84,93,94,404,405,417,418];
	-- excluded box, supports, equipments, cabletray, devices, fixtures except cristy box AND floor box
	vol_comp:= (jbox_comp || sup_comp || tray_comp_ex_sl || iwd_comp || lf_comp || fd_comp || eq_comp);
	-- excluded components for inslab AND underground
	ug_slab_excep_comp := (jbox_comp || sup_comp || tray_comp_ex_sl || iwd_comp || lf_comp);
	-- scopebox buffer
	depth_of_slab :=( SELECT t1.depth_of_slab::numeric/12 * -1 FROM tbl_pc_project_details t1
					 WHERE t1.fk_project_id = p_project_id AND t1.depth_of_slab > 0 LIMIT 1 );
	is_eight_ft_above :=(SELECT CASE WHEN flushmount_panel_connection_status = 1 THEN true else false END FROM tbl_pc_project_details t1
					 WHERE t1.fk_project_id = p_project_id LIMIT 1);
	-- slab material
	slabmaterial:=(SELECT material_of_slab FROM tbl_pc_project_details WHERE fk_project_id = p_project_id LIMIT 1);

	RAISE NOTICE 'Initiating Stage (%)', 0;

	CREATE temp table comp_data ON COMMIT DROP AS
		select * from mt_view_faimly
		WHERE rcm_deleted = false AND p_file_id IN (SELECT unnest(fl_fk_fileid))
		AND p_file_id IN (SELECT unnest(fn_fk_fileid))
		AND ol_fk_fileid = p_file_id;

	CREATE index inx_temp_comp_id ON comp_data (objectid, fk_fam_comp_id) WHERE fk_fam_comp_id IS NOT NULL;

	RAISE NOTICE 'Fetched component mapping data (%)', 1;

	CREATE temp table temp_box_intersection ON COMMIT DROP AS
	WITH
	scopebox_data AS (
		SELECT tss.pk_scopebox_id, tss.fk_constid, tss.fk_occupancyid, tss.fk_scopebox_subsubtypeid,
			st_translate(st_geometryfromtext(tss.polyhedralsurface), globaloffset_x, globaloffset_y, globaloffset_z) AS polyhedralsurface
		FROM tbl_sb_scopebox tss
		WHERE tss.fk_projectid = p_project_id AND tss.deleted = false and tss.fk_file_type_id =1
	),
	scopebox_intersection AS (
		SELECT t3.pk_scopebox_id AS oc, t2.pk_scopebox_id AS ct, t1.pk_scopebox_id AS scopebox,
			t1.polyhedralsurface::geometry AS sb_geom, sbsm.conduit_material, sbsm.jbox_nema_rating,
			t1.fk_scopebox_subsubtypeid AS sb_type,
			ST_3DIntersection(
					ST_3DIntersection(ST_MakeSolid(st_astext(t2.polyhedralsurface)), ST_MakeSolid(st_astext(t3.polyhedralsurface))), -- ct-ot intersection
					ST_MakeSolid(st_astext(t1.polyhedralsurface))
				) AS intersected_geometry,
				CASE WHEN t1.fk_scopebox_subsubtypeid = 1 THEN
					ST_Extrude(ST_GeometryN(t1.polyhedralsurface, ST_NumGeometries(t1.polyhedralsurface) - 1),0,0,0.5)
				END AS bufferarea,
			   	CASE WHEN ((t1.fk_scopebox_subsubtypeid = 1) AND (depth_of_slab <> 0)) THEN
					ST_Extrude(ST_GeometryN(t1.polyhedralsurface, ST_NumGeometries(t1.polyhedralsurface) - 1),0,0, depth_of_slab)
				END AS slabarea,
				CASE WHEN ((is_eight_ft_above = true) AND (t1.fk_scopebox_subsubtypeid = 1)) THEN
					ST_Extrude(ST_GeometryN(t1.polyhedralsurface, ST_NumGeometries(t1.polyhedralsurface) - 1),0,0, 8)
				END AS slab_eight_ft_buffer
				FROM scopebox_data t1 --tbl_sb_scopebox t1
				JOIN scopebox_data t2 --tbl_sb_scopebox t2
					ON t1.pk_scopebox_id <> t2.pk_scopebox_id
				JOIN scopebox_data t3 --tbl_sb_scopebox t3
					ON t1.pk_scopebox_id <> t3.pk_scopebox_id
				JOIN tbl_sb_scopebox_sub_subtype_master sbsm
					ON t1.fk_scopebox_subsubtypeid = sbsm.pk_scopebox_sub_subtype_id
			WHERE t1.pk_scopebox_id = p_scopebox_id AND
				t1.pk_scopebox_id <> t2.pk_scopebox_id AND
				t1.pk_scopebox_id <> t3.pk_scopebox_id AND
				t2.pk_scopebox_id <> t3.pk_scopebox_id AND
				(t1.fk_constid = 0 AND t1.fk_occupancyid = 0) AND
				(t2.fk_constid <> 0 AND t2.fk_occupancyid = 0) AND
				(t3.fk_constid = 0 AND t3.fk_occupancyid <> 0) AND
				t1.polyhedralsurface &&& t2.polyhedralsurface AND
				t1.polyhedralsurface &&& t3.polyhedralsurface AND
				t2.polyhedralsurface &&& t3.polyhedralsurface
	),
	intermediate_step AS (
		WITH transformed AS (
			SELECT ST_Force2D(ST_GeometryN(si.sb_geom,(ST_NumGeometries(si.sb_geom)-1))) AS trans, si.scopebox, si.ct, si.oc,
				(st_zmax(sb_geom) - St_Zmin(sb_geom)) AS elev
			FROM scopebox_intersection si
		)
		SELECT (row_number() OVER())::integer AS seq_id, t.geom, t.scopebox, t.ct, t.oc
			FROM (SELECT (ST_Dump(
						ST_Split(
									ST_Split(trans,
										ST_MakeLine(
											ST_MakePoint(ST_X(ST_Centroid(trans)), ST_YMin(trans)),
											ST_MakePoint(ST_X(ST_Centroid(trans)), ST_YMax(trans))
										)
									),
									ST_MakeLine(
											ST_MakePoint(ST_XMin(trans), ST_Y(ST_Centroid(trans))),
											ST_MakePoint(ST_XMax(trans), ST_Y(ST_Centroid(trans)))
									)
								)
					)).geom, tr.scopebox, tr.ct, tr.oc
		FROM transformed tr) t
	),
	object_geometry AS (
		WITH obj_geom AS (
			SELECT bbox.objectid, ST_GeometryFromText(bbox.linestring) AS linestring
			FROM tbl_sb_bounding_box bbox
			WHERE bbox.fileid = p_file_id
		)
		SELECT og.objectid, ims.seq_id, og.linestring,
			si.scopebox, si.ct, si.oc
		FROM obj_geom og
		JOIN scopebox_intersection si
			ON ((si.intersected_geometry &&& og.linestring) or
				(si.bufferarea &&& og.linestring) or
				(si.slabarea &&& og.linestring) or
				(si.slab_eight_ft_buffer &&& og.linestring) )
		JOIN intermediate_step ims
			ON si.scopebox = ims.scopebox AND si.ct = ims.ct AND si.oc = ims.oc
				AND ims.geom && ST_force2d(og.linestring)
	),
	objects_in_sb AS (
		SELECT og.objectid, og.linestring, og.seq_id, cd.fk_fam_comp_id, cd.fk_familytype_id, si.ct, si.oc,
			( ST_3DIntersects(og.linestring, ST_MakeSolid(ST_GeometryN(si.sb_geom,(ST_NumGeometries(si.sb_geom)-1))))
			 OR ST_3DIntersects(og.linestring, ST_MakeSolid(ST_GeometryN(si.sb_geom,ST_NumGeometries(si.sb_geom)))) )AS is_scopebox,
			case
				when (cd.fk_fam_comp_id in (4,43,51,54) and (GeometryType(ST_3DIntersection(ST_AsText(si.intersected_geometry)::geometry,og.linestring)) = 'MULTIPOINT')) then
					ST_3DLength(ST_LineFromMultiPoint(ST_3DIntersection(ST_AsText(si.intersected_geometry)::geometry,og.linestring)))
				when (cd.fk_fam_comp_id in (4,43,51,54) and ST_3DIntersects(si.intersected_geometry,og.linestring)) then
					ST_3DLength(ST_3DIntersection(si.intersected_geometry,og.linestring))
				when ST_DWithin(ST_force2d(og.linestring), st_force2d(si.intersected_geometry), 0.00001) then
					0.0001
				else 0
			end as length_3d,
			case
				WHEN ((cd.fk_fam_comp_id NOT IN (4,7,43,51,54)) AND
					  (((vol_comp @> array[coalesce(cd.fk_fam_comp_id,0)]) AND (si.sb_type = 3))
					  or (fd_comp @> array[coalesce(cd.fk_fam_comp_id,0)]))) THEN
					case
						WHEN st_3dintersects(si.intersected_geometry, Box3D(og.linestring)) THEN
							ROUND(((ST_Volume(ST_3DIntersection(si.intersected_geometry,BOX3D(og.linestring)))/
							ST_Volume(BOX3D(og.linestring))) * 100)::numeric,8)
						else 100
					END
				else 0
			END AS vol_3d,
			CASE WHEN cd.fk_fam_comp_id IN (4,7) THEN si.conduit_material
				 WHEN jbox_comp @> ARRAY[coalesce(cd.fk_fam_comp_id,0)] THEN si.jbox_nema_rating
			END AS rec_specification,
			( (og.linestring &&& si.slabarea) AND --should intersect slab
			  (
				(ST_3DIntersects(si.slabarea, og.linestring) = false) OR --should be completely inside
				(og.linestring &&& ST_GeometryN(si.slabarea, 1) AND cd.fk_fam_comp_id = 7 AND
					og.linestring &&& ST_GeometryN(si.slabarea, 2) = false) OR --should NOT pass FROM top to bottom
				(og.linestring &&& ST_GeometryN(si.slabarea, 1) AND cd.fk_fam_comp_id = 7) OR --only elbow intersecting top of scopebox
				(og.linestring &&& ST_GeometryN(si.slabarea, 1) = false) OR -- should NOT intersect top of scopebox
				(og.linestring &&& ST_GeometryN(si.slabarea, 2) = true AND cd.fk_fam_comp_id = 7)
			  )
			  AND
			  ((og.linestring &&& ST_GeometryN(si.slabarea, 1) AND og.linestring &&& ST_GeometryN(si.slabarea, 2)) = false)
			) AS is_under_slab,
			( og.linestring &&& si.bufferarea ) AS is_in_6,
			( og.linestring &&& si.slab_eight_ft_buffer ) AS is_in_8_ft,
			( ST_3DIntersects(si.intersected_geometry,og.linestring) ) AS is_all_sb,
			( ST_StartPoint(og.linestring) &&& si.slabarea) AS is_not_bottom_slab,
			si.sb_type,
			(( si.sb_type IN (1,2) AND cd.fk_fam_comp_id = 31) or --pvc
			 ( si.sb_type = 3 AND cd.fk_fam_comp_id = 27) --emt
			)AS is_excluded
		FROM scopebox_intersection si
		JOIN object_geometry og ON true
		LEFT JOIN comp_data cd ON cd.objectid = og.objectid
		WHERE CASE WHEN si.sb_type IN (1,2) THEN (ug_slab_excep_comp @> array[coalesce(cd.fk_fam_comp_id,0)]) = false else true END
	)
	SELECT t1.objectid, t1.length_3d, t1.vol_3d, t1.seq_id, t1.fk_fam_comp_id AS comp_id,
		   t1.fk_familytype_id AS familytype_id, t1.sb_type, --t1.rec_specification,
		   case
			  WHEN ((t1.sb_type = 1) AND
					(((t1.is_in_8_ft) AND (coalesce(t1.fk_fam_comp_id,0) IN (4,7)) AND (pl.linestring &&& ST_EndPoint(t1.linestring)))
					 or (t1.is_under_slab))) THEN slabmaterial
			  else t1.rec_specification
			END AS rec_specification,
		   CASE WHEN t1.fk_fam_comp_id IN (4,7,54) THEN usp_checkIsVertical(t1.linestring) else false END AS is_vertical,
		   CASE WHEN t1.fk_fam_comp_id = 54 THEN usp_get_linestring_angle(t1.linestring) END AS angle,
		   CASE WHEN t1.fk_fam_comp_id = 54 THEN -- straight communication tray
				case -- x expansion
					WHEN ((ST_XMax(Box3D(t1.linestring)) - ST_XMin(Box3D(t1.linestring))) >
							(ST_YMax(Box3D(t1.linestring)) - ST_YMin(Box3D(t1.linestring))) AND
						   (ST_XMax(Box3D(t1.linestring)) - ST_XMin(Box3D(t1.linestring))) >
							(ST_ZMax(Box3D(t1.linestring)) - ST_ZMin(Box3D(t1.linestring)))) THEN
						ST_MakeLine(ST_MakePoint(ST_XMin(ST_Expand(Box3D(t1.linestring), 0.34, 0, 0)),ST_YMin(ST_Expand(Box3D(t1.linestring), 0.33, 0, 0)),ST_ZMin(ST_Expand(Box3D(t1.linestring), 0.33, 0, 0))),
									ST_MakePoint(ST_XMax(ST_Expand(Box3D(t1.linestring), 0.34, 0, 0)),ST_YMax(ST_Expand(Box3D(t1.linestring), 0.33, 0, 0)),ST_ZMax(ST_Expand(Box3D(t1.linestring), 0.33, 0, 0))))
					-- y expansion
					WHEN ((ST_YMax(Box3D(t1.linestring)) - ST_YMin(Box3D(t1.linestring))) >
							(ST_XMax(Box3D(t1.linestring)) - ST_XMin(Box3D(t1.linestring))) AND
						   (ST_YMax(Box3D(t1.linestring)) - ST_YMin(Box3D(t1.linestring))) >
							(ST_ZMax(Box3D(t1.linestring)) - ST_ZMin(Box3D(t1.linestring)))) THEN
						ST_MakeLine(ST_MakePoint(ST_XMin(ST_Expand(Box3D(t1.linestring), 0, 0.34, 0)),ST_YMin(ST_Expand(Box3D(t1.linestring), 0, 0.33, 0)),ST_ZMin(ST_Expand(Box3D(t1.linestring), 0, 0.33, 0))),
									ST_MakePoint(ST_XMax(ST_Expand(Box3D(t1.linestring), 0, 0.34, 0)),ST_YMax(ST_Expand(Box3D(t1.linestring), 0, 0.33, 0)),ST_ZMax(ST_Expand(Box3D(t1.linestring), 0, 0.33, 0))))
					else  -- z expansion
						ST_MakeLine(ST_MakePoint(ST_XMin(ST_Expand(Box3D(t1.linestring), 0, 0, 0.34)),ST_YMin(ST_Expand(Box3D(t1.linestring), 0, 0, 0.33)),ST_ZMin(ST_Expand(Box3D(t1.linestring), 0, 0, 0.33))),
									ST_MakePoint(ST_XMax(ST_Expand(Box3D(t1.linestring), 0, 0, 0.34)),ST_YMax(ST_Expand(Box3D(t1.linestring), 0, 0, 0.33)),ST_ZMax(ST_Expand(Box3D(t1.linestring), 0, 0, 0.33))))
				END
				else t1.linestring
			END AS linestring,
		   t1.ct, t1.oc, t1.is_scopebox,
		   coalesce(t1.is_under_slab, false) AS is_under_slab,
		   coalesce(t1.is_in_6, false) AS is_eqp_in_6,
		   coalesce(t1.is_in_8_ft, false) AS is_in_8_ft,
		   coalesce(t1.is_all_sb, false) AS is_all_sb,
		   ((t1.sb_type = 1) AND (((t1.is_in_8_ft) AND (coalesce(t1.fk_fam_comp_id,0) IN (4,7)) AND (pl.linestring &&& ST_EndPoint(t1.linestring)))
			or (t1.is_under_slab))) AS ent_length,
		   (CASE WHEN t1.fk_fam_comp_id = 54 THEN
				(
			 	 jsonb_build_object('z_min', ST_ZMin(t1.linestring::geometry)) ||
			 	 jsonb_build_object('z_max', ST_ZMax(t1.linestring::geometry))
				) else '{}' END
			)::jsonb AS tray_data
		FROM objects_in_sb t1
		LEFT JOIN (
			SELECT p.objectid, p.linestring, p.ct, p.oc FROM objects_in_sb p
				WHERE p.is_in_8_ft = true AND p.fk_fam_comp_id = 37
		) pl
		ON t1.ct = pl.ct AND t1.oc = pl.oc AND pl.linestring &&& t1.linestring
		WHERE t1.length_3d > 0
			AND (
				CASE WHEN t1.is_in_6 = true AND t1.is_scopebox=false THEN t1.fk_fam_comp_id IN (22,23,28,29,34)
				else true END
			);

	RAISE NOTICE 'Initiating Stage (%) Creating gist index', 2;

	CREATE index inx_temp_box_seq_comp_id ON temp_box_intersection using btree (seq_id, comp_id);
	CREATE index inx_temp_box_intersection_linestring ON temp_box_intersection using gist (linestring);

	RAISE NOTICE 'Initiating Stage (%) Generating connections', 3;

	CREATE temp table temp_element_connection ON COMMIT DROP AS
	WITH connected_data AS (
		SELECT distinct t1.objectid, t1.length_3d, t1.vol_3d, t1.comp_id, t1.familytype_id, t1.seq_id AS obj_seq,
			t2.objectid AS connected, t1.ct, t1.oc, t2.seq_id AS conn_seq, t1.is_vertical, t1.angle, t1.sb_type,
			t1.rec_specification, t1.linestring AS line_1, t2.linestring AS line_2,
			ST_3DLength(ST_3DShortestLine(
				St_MakePoint(ST_X(ST_StartPoint(t1.linestring))+0.0001,
							 ST_Y(ST_StartPoint(t1.linestring))-0.0001,
							 ST_Z(ST_StartPoint(t1.linestring)) ),
				t2.linestring)) AS ele_distance,
			t1.is_scopebox, t1.is_under_slab, t1.is_eqp_in_6, t1.is_in_8_ft, t1.is_all_sb, t1.ent_length, t1.tray_data
		FROM temp_box_intersection t1
		LEFT JOIN temp_box_intersection t2
			ON t1.seq_id = t2.seq_id
				AND (
					  ((t1.comp_id = 4 OR t1.comp_id = 7) AND (coalesce(t1.comp_id,0) <> coalesce(t2.comp_id,0))) OR
					  (
						(t1.comp_id IN (54,55,56,57,58) AND t2.comp_id IN (54,55,56,57,58)) OR -- communication trays
						(t1.comp_id IN (43,44,45,46,47,48,49,50) AND t2.comp_id IN (43,44,45,46,47,48,49,50)) OR -- power ladder tray
						(t1.comp_id IN (51,52,53,59,60,61,62,63) AND t2.comp_id IN (51,52,53,59,60,61,62,63)) -- basket tray
					  )
				)
				AND t1.ct = t2.ct AND t1.oc = t2.oc
				AND t1.objectid <> t2.objectid
				AND ((t1.linestring &&& t2.linestring) or ((t1.linestring <#> t2.linestring)<= 0.01))
				AND ((ST_3DIntersects(Box3D(t1.linestring), Box3D(t2.linestring))) OR
					 (ST_3DDistance(Box3D(t1.linestring), Box3D(t2.linestring)) <= 0.01))
			WHERE
			(CASE WHEN t1.sb_type = 3 THEN coalesce(t2.comp_id,0) <> 27 else coalesce(t2.comp_id,0) <> 31 END) AND
			(CASE WHEN ((t1.sb_type = 1) AND (t1.is_in_8_ft = true) AND (t1.is_scopebox = false))
			 	THEN ((t2.linestring &&& ST_EndPoint(t1.linestring)) AND (t1.is_vertical = true) AND
					  (coalesce(t1.comp_id,0) IN (4,7)) AND (t2.comp_id IN (22,23,28,29,34,37))) else true END)
		),
		connected_filtered AS (
			SELECT cd.objectid, cd.length_3d, cd.vol_3d, cd.comp_id, cd.familytype_id, cd.obj_seq, cd.connected, cd.ct, cd.oc,
				cd.conn_seq, cd.ele_distance, cd.is_scopebox, cd.is_under_slab, cd.is_eqp_in_6, cd.is_in_8_ft, cd.is_all_sb,
				cd.ent_length, cd.is_vertical, cd.angle, cd.rec_specification, cd.tray_data,
				min(ele_distance) OVER (PARTITION BY cd.objectid, cd.length_3d, cd.vol_3d, cd.comp_id, cd.ct, cd.oc) AS min_ele_dist,
				max(ele_distance) OVER (PARTITION BY cd.objectid, cd.length_3d, cd.vol_3d, cd.comp_id, cd.ct, cd.oc) AS max_ele_dist
			FROM connected_data cd
			WHERE coalesce(cd.comp_id,0) NOT IN (27,31) -- remove excluded items
		)
		SELECT ed.objectid, ed.length_3d, ed.vol_3d, ed.comp_id, ed.familytype_id, ed.ct, ed.oc, ed.rec_specification,
			ed.is_vertical, ed.angle, ed.is_scopebox, ed.is_under_slab, ed.is_eqp_in_6, ed.is_in_8_ft, ed.is_all_sb,
			ed.ent_length,	ed.at_start[1] AS at_start,
			(array_remove(ed.at_end, ed.at_start[1]))[1] AS at_end,
			mid.in_mid, ed.tray_data
		FROM (
			SELECT fd.objectid, fd.length_3d, fd.vol_3d, fd.comp_id, fd.familytype_id, fd.ct, fd.oc, fd.is_scopebox, fd.is_under_slab,
				fd.is_eqp_in_6, fd.is_in_8_ft, fd.is_all_sb, fd.ent_length, fd.rec_specification, fd.is_vertical, fd.angle,
				array_remove(array_agg(fd.at_start), NULL) AS at_start,
				array_remove(array_agg(fd.at_end), NULL) AS at_end,
				fd.tray_data
			FROM(
				SELECT cf.objectid, cf.length_3d, cf.vol_3d, cf.comp_id, cf.familytype_id, cf.ct, cf.oc, cf.is_scopebox,
					cf.is_under_slab, cf.is_eqp_in_6, cf.is_in_8_ft, cf.is_all_sb, cf.ent_length, cf.rec_specification, cf.is_vertical, cf.angle,
					CASE WHEN cf.ele_distance = cf.min_ele_dist THEN cf.connected END AS at_start,
					CASE WHEN cf.ele_distance = cf.max_ele_dist THEN cf.connected END AS at_end,
					cf.tray_data
				FROM connected_filtered cf
				) fd
			GROUP BY fd.objectid, fd.length_3d, fd.vol_3d, fd.comp_id, fd.familytype_id, fd.ct, fd.oc,	fd.is_scopebox, fd.is_under_slab,
				fd.is_eqp_in_6, fd.is_in_8_ft, fd.is_all_sb, fd.ent_length, fd.rec_specification, fd.is_vertical, fd.angle, fd.tray_data
		) ed
		LEFT JOIN (
			SELECT cf.objectid, cf.length_3d, cf.vol_3d, cf.comp_id, cf.familytype_id, cf.ct, cf.oc,
				array_remove(array_agg(cf.connected), NULL)::integer[] AS in_mid
			FROM connected_filtered cf
			WHERE cf.ele_distance <> cf.min_ele_dist AND cf.ele_distance <> cf.max_ele_dist
			GROUP BY cf.objectid, cf.length_3d, cf.vol_3d, cf.comp_id, cf.familytype_id, cf.ct, cf.oc
		) mid
		ON ed.objectid = mid.objectid AND ed.length_3d = mid.length_3d AND ed.comp_id = mid.comp_id
			AND ed.familytype_id = mid.familytype_id AND ed.ct = mid.ct AND ed.oc = mid.oc;

	RAISE NOTICE 'Initiating Stage (%) Getting propertyjson data', 4;

	CREATE temp table propertyjson ON COMMIT DROP AS
		SELECT dpj.objectid, dpj.filedata::jsonb AS filedata
			FROM tbl_dextract_propertyjson dpj WHERE dpj.file_id = p_file_id;

	RAISE NOTICE 'Initiating Stage (%) indexing propertyjson data', 5;

	CREATE INDEX inx_propertydata_objectid ON propertyjson using btree (objectid);

	RAISE NOTICE 'Initiating Stage (%) Returning Data', 6;

	RETURN QUERY
	WITH stage1 AS(
		SELECT ec.objectid, ec.length_3d, ec.vol_3d, ec.comp_id, ec.ct, ec.oc, ec.at_start, ec.at_end, ec.in_mid, ec.is_scopebox,
			ec.is_under_slab, ec.is_eqp_in_6, ec.is_in_8_ft, ec.is_all_sb, ec.ent_length, ec.rec_specification, ec.is_vertical,
			REPLACE(regexp_replace(btrim(pj.filedata->1 ->>'name'), '^.* \[', ''), ']', '')::integer AS revit_id,
			ip.inspire_parameter_name, ec.tray_data,
			case
				WHEN POSITION('ft' IN pj.filedata->1->'properties'->pl.group_name->>ipm.fk_revit_parameter_name)>0 AND
					((length_of_rods_parameter @> ARRAY[coalesce(ipm.fk_inspire_parameter_id,0)]) = false) AND ec.comp_id NOT IN (4,7,43,51,54,24,26,28,29,34,132,133,134,135,136,137,138,139,140,141,142,143,144,145,179,180,181,182,183,184,185,186,187,205,206,207,208) THEN
					((CAST(ROUND(((SELECT (regexp_matches(pj.filedata->1->'properties'->pl.group_name->>ipm.fk_revit_parameter_name, '[0-9]+\.?[0-9]*'))[1]::numeric) * 12) / 0.25, 2) AS int)) * 0.25 ) || '0 inch'::text
				else
					pj.filedata->1->'properties'->pl.group_name->>ipm.fk_revit_parameter_name
			END AS prop_value,
			--CASE WHEN ec.comp_id = 7 THEN
			--	round((SELECT (regexp_matches(	coalesce(pj.filedata->1->'properties'->'Dimensions'->>'Bend Angle',
			--					 					pj.filedata->1->'properties'->'Dimensions'->>'Angle'),
			--												 '[0-9]+\.?[0-9]*'))[1]::numeric), 1)
			--	else ec.angle END AS angle,
			ec.angle,
			CASE WHEN ec.comp_id = 7 THEN
				coalesce((select (regexp_matches(pj.filedata->1->'properties'->'Dimensions'->>'Bend Angle', '[0-9]+\.?[0-9]*', 'g'))[1]::numeric), 0)
			end as elbow_bend_angle,
			CASE WHEN ec.comp_id = 7 THEN
				coalesce((select (regexp_matches(pj.filedata->1->'properties'->'Dimensions'->>'Angle', '[0-9]+\.?[0-9]*', 'g'))[1]::numeric), 0)
			end as elbow_angle,
			CASE WHEN ec.comp_id = 7 THEN
				cast(regexp_replace(pj.filedata->1->'properties'->'Dimensions'->>'Insp_Length','[A-Za-z]', '', 'g') AS double precision)
			END AS elbow_length
		FROM temp_element_connection ec
		LEFT JOIN tbl_revit_inspire_parameter_mapping ipm
			ON ec.familytype_id = ipm.fk_familytypeid AND ec.comp_id = ipm.fk_fam_comp_id
				AND ipm.fk_revit_parameter_name <> '' AND ipm.deleted = false
		LEFT JOIN  tbl_inspire_parameters ip
			ON ip.pk_inspire_parameter_id = ipm.fk_inspire_parameter_id
		LEFT JOIN tbl_dextract_propertylist pl
			ON pl.fk_familytype_id = ipm.fk_familytypeid AND pl.property_name = ipm.fk_revit_parameter_name
		JOIN propertyjson pj
			ON pj.objectid = ec.objectid
	),
	stage2 AS (
		SELECT s1.objectid, s1.comp_id, s1.ct, s1.oc, s1.at_start, s1.at_end, s1.in_mid, s1.revit_id,
			--s1.angle,
			case when s1.comp_id = 7 then
				case when s1.elbow_bend_angle > s1.elbow_angle then s1.elbow_bend_angle
				else s1.elbow_angle
				end
			else s1.angle
			end as angle,
			s1.is_scopebox,
			s1.is_under_slab, s1.is_eqp_in_6, s1.is_in_8_ft, s1.is_all_sb, s1.ent_length, s1.rec_specification, s1.is_vertical,
			CASE WHEN s1.comp_id = 7 THEN s1.elbow_length else s1.length_3d END AS length,
			s1.vol_3d, s1.tray_data,
			coalesce(json_object_agg(s1.inspire_parameter_name, s1.prop_value)
			FILTER (WHERE s1.inspire_parameter_name IS NOT NULL) , '{}'::JSON)AS prop_data
		FROM stage1 s1
		GROUP BY s1.objectid, s1.length_3d, s1.vol_3d, s1.comp_id, s1.ct, s1.oc, s1.at_start, s1.at_end, s1.in_mid, s1.revit_id,
		s1.angle, s1.elbow_bend_angle, s1.elbow_angle, s1.elbow_length, s1.is_scopebox, s1.is_under_slab, s1.is_eqp_in_6, s1.is_in_8_ft, s1.is_all_sb, s1.ent_length,
		s1.rec_specification, s1.is_vertical, s1.tray_data
	)
	SELECT s2.objectid, s2.comp_id, s2.ct, s2.oc, s2.at_start, s2.at_end, s2.in_mid, s2.revit_id,
			s2.is_scopebox, s2.is_under_slab, s2.is_eqp_in_6, s2.is_in_8_ft, s2.is_all_sb, s2.rec_specification, s2.is_vertical,
			case
				WHEN s2.comp_id = 4 AND s2.is_all_sb = false AND s2.length > 0 THEN
					coalesce((Select (regexp_matches(s2.prop_data->>'Conduit Length', '[0-9]+\.?[0-9]*'))[1]::numeric),s2.length)
				WHEN s2.comp_id = 4 AND (s2.is_in_8_ft or s2.is_under_slab) AND (s2.ent_length) AND s2.length > 0 THEN
					coalesce((Select (regexp_matches(s2.prop_data->>'Conduit Length', '[0-9]+\.?[0-9]*'))[1]::numeric),s2.length)
				WHEN s2.comp_id IN (43,51,54) AND s2.is_all_sb = false AND s2.length > 0 THEN
					coalesce((Select (regexp_matches(s2.prop_data->>'Length', '[0-9]+\.?[0-9]*'))[1]::numeric),s2.length)
				else s2.length
			END AS length,
			s2.vol_3d::numeric AS vol_3d,
			s2.angle, (s2.prop_data::jsonb || coalesce(s2.tray_data, '{}'::jsonb))::json AS prop_data
		FROM stage2 s2;

END;
$function$
;