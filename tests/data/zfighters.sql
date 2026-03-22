DROP TABLE IF EXISTS zfighter_intel;
DROP TABLE IF EXISTS zfighter_registry;
DROP TABLE IF EXISTS zfighters;
DROP TABLE IF EXISTS races;

CREATE TABLE races (
  id        INT PRIMARY KEY,
  race_name TEXT NOT NULL UNIQUE,
  traits    TEXT[] NOT NULL DEFAULT '{}'::text[]
);

CREATE TABLE zfighters (
  id        INT PRIMARY KEY,
  name      TEXT NOT NULL UNIQUE,
  race_id   INT  NOT NULL REFERENCES races(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  height_cm INT  NOT NULL CHECK (height_cm > 0)
);

CREATE INDEX idx_zfighters_race_id ON zfighters(race_id);

CREATE TABLE zfighter_registry (
  fighter_id     INT PRIMARY KEY REFERENCES zfighters(id) ON UPDATE CASCADE ON DELETE CASCADE,
  archival_code  TEXT NOT NULL UNIQUE
);

CREATE TABLE zfighter_intel (
  fighter_id       INT  PRIMARY KEY REFERENCES zfighters(id) ON UPDATE CASCADE ON DELETE CASCADE,
  codename         TEXT NOT NULL,
  scouter_serial   TEXT NOT NULL,
  home_coordinates TEXT NOT NULL
);

ALTER TABLE zfighter_intel
  ADD CONSTRAINT zfighter_intel_fighter_registry_fk
  FOREIGN KEY (fighter_id)
  REFERENCES zfighter_registry(fighter_id)
  ON UPDATE CASCADE
  ON DELETE CASCADE;


-- =========================
-- Seed data
-- =========================

INSERT INTO races (id, race_name, traits) VALUES
  (1, 'Saiyan',   ARRAY['Cool moves','Great Ape potential','Zenkai']),
  (2, 'Human',    ARRAY['Smart','Can cook well']),
  (3, 'Namekian', ARRAY['Regeneration','Stretch limbs','Green']),
  (4, 'Frieza Race', ARRAY['Evolution forms','Space survival']);

INSERT INTO zfighters (id, name, race_id, height_cm)
SELECT v.id, v.name, r.id, v.height_cm
FROM (
  VALUES
    (1, 'Goku',    'Saiyan',   175),
    (2, 'Vegeta',  'Saiyan',   164),
    (3, 'Gohan',   'Saiyan',   176),
    (4, 'Trunks',  'Saiyan',   170),
    (5, 'Broly',   'Saiyan',   220),
    (6, 'Piccolo', 'Namekian', 226),
    (7, 'Frieza',  'Frieza Race', 158),
    (8, 'Yamcha',  'Human',    183)
) AS v(id, name, race_name, height_cm)
JOIN races r ON r.race_name = v.race_name;

INSERT INTO zfighter_registry (fighter_id, archival_code)
SELECT z.id, v.archival_code
FROM (
  VALUES
    ('Goku',   'REG-0001'),
    ('Vegeta', 'REG-0002'),
    ('Gohan',  'REG-0003'),
    ('Trunks', 'REG-0004'),
    ('Broly',  'REG-0005')
) AS v(name, archival_code)
JOIN zfighters z ON z.name = v.name;

INSERT INTO zfighter_intel (fighter_id, codename, scouter_serial, home_coordinates)
SELECT z.id, v.codename, v.scouter_serial, v.home_coordinates
FROM (
  VALUES
    ('Goku',   'Kakarot',   'SCT-9001-A', 'X:120-Y:450'),
    ('Vegeta', 'Prince',    'SCT-9002-B', 'X:120-Y:450'),
    ('Gohan',  'Scholar',   'SCT-9003-C', 'X:119-Y:451'),
    ('Trunks', 'Timeblade', 'SCT-9004-D', 'X:118-Y:452'),
    ('Broly',  'Berserker', 'SCT-9005-E', 'X:200-Y:900')
) AS v(name, codename, scouter_serial, home_coordinates)
JOIN zfighters z ON z.name = v.name;
