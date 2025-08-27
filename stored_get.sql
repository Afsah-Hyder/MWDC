DELIMITER $$

CREATE PROCEDURE export_mission_all(IN in_mission_id CHAR(32))
BEGIN
  -- 0) mission (root)
  SELECT * FROM missions WHERE id = in_mission_id;

  -- 1) direct children of missions
  SELECT * FROM areas WHERE missions_id = in_mission_id;
  SELECT * FROM bottomidentification WHERE missions_id = in_mission_id;
  SELECT * FROM environmentaldata WHERE missions_id = in_mission_id;
  SELECT * FROM navigationmark WHERE missions_id = in_mission_id;
  -- operatornotes was commented in your DDL; include if it exists
  SELECT * FROM operatornotes WHERE missions_id = in_mission_id;
  SELECT * FROM specialpoint WHERE missions_id = in_mission_id;
  SELECT * FROM tasks WHERE missions_id = in_mission_id;    -- tasks that reference missions directly
  SELECT * FROM tracks WHERE missions_id = in_mission_id;
  SELECT * FROM underwaterobject WHERE missions_id = in_mission_id; -- direct uo rows if any

  -- 2) grandchildren that depend on areas
  SELECT ac.*
  FROM areacells ac
  WHERE ac.areas_id IN (SELECT id FROM areas WHERE missions_id = in_mission_id);

  SELECT ap.*
  FROM areapoints ap
  WHERE ap.areas_id IN (SELECT id FROM areas WHERE missions_id = in_mission_id);

  -- 2) children of tracks
  SELECT p.*
  FROM paths p
  WHERE p.tracks_id IN (SELECT id FROM tracks WHERE missions_id = in_mission_id);

  SELECT q.*
  FROM qroutes q
  WHERE q.tracks_id IN (SELECT id FROM tracks WHERE missions_id = in_mission_id);

  SELECT tp.*
  FROM trackpoints tp
  WHERE tp.tracks_id IN (SELECT id FROM tracks WHERE missions_id = in_mission_id);

  -- 2) taskexecutions for tasks (direct tasks)
  SELECT te.*
  FROM taskexecutions te
  WHERE te.tasks_id IN (SELECT id FROM tasks WHERE missions_id = in_mission_id);

  -- 3) deeper: tasks reachable via specialpoint or tracks (if not already included)
  SELECT t.*
  FROM tasks t
  WHERE t.specialpoint_id IN (SELECT id FROM specialpoint WHERE missions_id = in_mission_id)
     OR t.tracks_id IN (SELECT id FROM tracks WHERE missions_id = in_mission_id);

  -- 3) taskexecutions for tasks reachable from specialpoint/tracks
  SELECT te2.*
  FROM taskexecutions te2
  WHERE te2.tasks_id IN (
    SELECT id FROM tasks
    WHERE specialpoint_id IN (SELECT id FROM specialpoint WHERE missions_id = in_mission_id)
       OR tracks_id IN (SELECT id FROM tracks WHERE missions_id = in_mission_id)
  );

  -- 4) underwaterobject rows that reference taskexecutions belonging to mission tasks
  SELECT uo_direct.*
  FROM underwaterobject uo_direct
  WHERE uo_direct.taskexecutions_id IN (
    SELECT id FROM taskexecutions
    WHERE tasks_id IN (SELECT id FROM tasks WHERE missions_id = in_mission_id)
  );

  -- 5) paths / qroutes / trackpoints reachable from area->tracks chains
  SELECT p_area.*
  FROM paths p_area
  WHERE p_area.tracks_id IN (
    SELECT id FROM tracks WHERE areas_id IN (SELECT id FROM areas WHERE missions_id = in_mission_id)
  );

  SELECT q_area.*
  FROM qroutes q_area
  WHERE q_area.tracks_id IN (
    SELECT id FROM tracks WHERE areas_id IN (SELECT id FROM areas WHERE missions_id = in_mission_id)
  );

  SELECT tp_area.*
  FROM trackpoints tp_area
  WHERE tp_area.tracks_id IN (
    SELECT id FROM tracks WHERE areas_id IN (SELECT id FROM areas WHERE missions_id = in_mission_id)
  );

  -- 6) any other direct reference tables not listed above (colors, bottomtype lookups are lookup tables; include if they are referencing mission)
  -- e.g. if you have lookup table references that are mission-scoped, you can include them similarly.
END$$

DELIMITER ;
