DROP TABLE IF EXISTS zfighters;
DROP TABLE IF EXISTS races;

CREATE TABLE races (
  id        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  race_name TEXT NOT NULL UNIQUE,
  traits    TEXT[] NOT NULL DEFAULT '{}'::text[]
);

CREATE TABLE zfighters (
  id        INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name      TEXT NOT NULL UNIQUE,
  race_id   INT  NOT NULL REFERENCES races(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  height_cm INT  NOT NULL CHECK (height_cm > 0)
);

CREATE INDEX idx_zfighters_race_id ON zfighters(race_id);


-- =========================
-- Seed data
-- =========================

INSERT INTO races (race_name, traits) VALUES
  ('Saiyan',   ARRAY['Cool moves','Great Ape potential','Zenkai']),
  ('Human',    ARRAY['Smart','Can cook well']),
  ('Namekian', ARRAY['Regeneration','Stretch limbs','Green']),
  ('Frieza Race', ARRAY['Evolution forms','Space survival']);

INSERT INTO zfighters (name, race_id, height_cm)
SELECT v.name, r.id, v.height_cm
FROM (
  VALUES
    ('Goku',    'Saiyan',   175),
    ('Vegeta',  'Saiyan',   164),
    ('Gohan',   'Saiyan',   176),
    ('Trunks',  'Saiyan',   170),
    ('Broly',   'Saiyan',   220),
    ('Piccolo', 'Namekian', 226),
    ('Frieza',  'Frieza Race', 158),
    ('Yamcha',  'Human',    183)
) AS v(name, race_name, height_cm)
JOIN races r ON r.race_name = v.race_name;

