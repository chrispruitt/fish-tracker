CREATE TABLE IF NOT EXISTS detections
(
    id SERIAL NOT NULL,
    d_type VARCHAR(10),
    dod date,
    tod time,
    tag_id VARCHAR(35),
    antenna VARCHAR(10),
    species VARCHAR(15),
    marked_length NUMERIC,
    marked_antenna VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS migrations
(
    id SERIAL NOT NULL,
    tag_id VARCHAR(35),
    antenna_origin VARCHAR(10),
    origin_arrival_date date,
    origin_arrival_time time,
    origin_departure_date date,
    origin_departure_time time,
    antenna_destination VARCHAR(10),
    destination_arrival_date date,
    destination_arrival_time time,
    destination_departure_date date,
    destination_departure_time time,
    species VARCHAR(15),
    marked_length NUMERIC
);
