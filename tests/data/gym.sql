CREATE TABLE gym_exercise
(
    id              INT PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    noob_weight     INT  NOT NULL CHECK (noob_weight > 0),
    average_weight  INT  NOT NULL CHECK (average_weight > 0),
    pro_weight      INT  NOT NULL CHECK (pro_weight > 0),
    champion_weight INT  NOT NULL CHECK (champion_weight > 0)
);

CREATE TABLE gym_bros
(
    id         INT PRIMARY KEY,
    nickname   TEXT UNIQUE,
    real_name  TEXT,
    badge_code TEXT NOT NULL
);

CREATE TABLE lifts
(
    id                INT PRIMARY KEY,
    gym_bro_id        INT  NOT NULL REFERENCES gym_bros (id) ON UPDATE CASCADE ON DELETE CASCADE,
    athlete_real_name TEXT NOT NULL,
    exercise_id       INT  NOT NULL REFERENCES gym_exercise (id) ON UPDATE CASCADE ON DELETE RESTRICT,
    weight_kg         INT  NOT NULL CHECK (weight_kg > 0)
);

CREATE INDEX idx_lifts_athlete_real_name ON lifts (athlete_real_name);

CREATE TABLE orders
(
    code        TEXT PRIMARY KEY,
    description TEXT,
    cents_cost  INT NOT NULL
);

-- =========================
-- Seed data
-- =========================

INSERT INTO gym_exercise (id, name, noob_weight, average_weight, pro_weight, champion_weight)
VALUES (1, 'Bench Press', 40, 80, 120, 180),
       (2, 'Squat', 50, 100, 160, 240),
       (3, 'Deadlift', 60, 120, 180, 280),
       (4, 'Overhead Press', 25, 50, 75, 120),
       (5, 'Barbell Row', 40, 80, 120, 180),
       (6, 'Pull Up (added)', 10, 30, 60, 100),
       (7, 'Dip (added)', 10, 40, 70, 110),
       (8, 'Leg Press', 100, 200, 350, 500),
       (9, 'Bicep Curl', 15, 30, 50, 80),
       (10, 'Tricep Extension', 20, 40, 70, 100);

INSERT INTO gym_bros (id, nickname, real_name, badge_code)
VALUES (1, 'Beard man', 'Christian', '001'),
       (2, 'Black Panther', 'Angelo', '002'),
       (3, 'The beauty', 'Luca', '003'),
       (4, 'The boss', 'Antonio', '004');

INSERT INTO lifts (id, gym_bro_id, athlete_real_name, exercise_id, weight_kg)
VALUES (1, 1, 'Christian', 1, 120),
       (2, 1, 'Christian', 3, 190),
       (3, 2, 'Angelo', 2, 180),
       (4, 2, 'Angelo', 4, 85),
       (5, 3, 'Luca', 1, 100),
       (6, 4, 'Antonio', 8, 320);

INSERT INTO orders (code, description, cents_cost)
VALUES ('001', 'Omega 3', 2500),
       ('002', 'Whey', 5500),
       ('003', 'Magic mush', 7700),
       ('004', 'Red straps', 1500);

CREATE FUNCTION get_weight(a INTEGER, b INTEGER)
    RETURNS INTEGER AS
$$
BEGIN
    RETURN a + b;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION unsafe_get_weight(a INTEGER, b INTEGER)
    RETURNS INTEGER AS
$$
BEGIN
    RETURN a + b + 20;
END;
$$ LANGUAGE plpgsql;
