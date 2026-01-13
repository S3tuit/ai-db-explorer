DROP TABLE IF EXISTS gym_exercise;

CREATE TABLE gym_exercise (
  id              INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name            TEXT NOT NULL UNIQUE,
  noob_weight     INT  NOT NULL CHECK (noob_weight > 0),
  average_weight  INT  NOT NULL CHECK (average_weight > 0),
  pro_weight      INT  NOT NULL CHECK (pro_weight > 0),
  champion_weight INT  NOT NULL CHECK (champion_weight > 0)
);

-- =========================
-- Seed data
-- =========================

INSERT INTO gym_exercise (name, noob_weight, average_weight, pro_weight, champion_weight) VALUES
  ('Bench Press',     40,  80, 120, 180),
  ('Squat',           50, 100, 160, 240),
  ('Deadlift',        60, 120, 180, 280),
  ('Overhead Press',  25,  50,  75, 120),
  ('Barbell Row',     40,  80, 120, 180),
  ('Pull Up (added)', 10,  30,  60, 100),
  ('Dip (added)',    10,  40,  70, 110),
  ('Leg Press',     100, 200, 350, 500),
  ('Bicep Curl',     15,  30,  50,  80),
  ('Tricep Extension',20,  40,  70, 100);

