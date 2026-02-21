DROP TABLE IF EXISTS gym_exercise;

CREATE TABLE gym_exercise
(
    id              INT PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    noob_weight     INT  NOT NULL CHECK (noob_weight > 0),
    average_weight  INT  NOT NULL CHECK (average_weight > 0),
    pro_weight      INT  NOT NULL CHECK (pro_weight > 0),
    champion_weight INT  NOT NULL CHECK (champion_weight > 0)
);

CREATE TABLE name_day (
    name     TEXT PRIMARY KEY,
    name_day DATE,
    saint    TEXT
);

CREATE TABLE gym_bros
(
    id        INT PRIMARY KEY,
    nickname  TEXT UNIQUE,
    real_name TEXT REFERENCES name_day (name)
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

INSERT INTO name_day (name, name_day, saint)
VALUES
    ('Christian', '2024-03-12', 'San Cristoforo / San Cristiano'),
    ('Angelo',    '2024-05-05', 'Sant''Angelo'),
    ('Luca',      '2024-10-18', 'San Luca Evangelista'),
    ('Antonio',   '2024-06-13', 'Sant''Antonio di Padova');

INSERT INTO gym_bros (id, nickname, real_name)
VALUES (1, 'Beard man', 'Christian'),
       (2, 'Black Panther', 'Angelo'),
       (3, 'The beauty', 'Luca'),
       (4, 'The boss', 'Antonio');

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

