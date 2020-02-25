COPY detections
(
    d_type,
    dod,
    tod,
    tag_id,
    antenna,
    species,
    marked_length,
    marked_antenna
) 
FROM '/Users/chris/workspace/pruitt/fish-tracker/results/cleansed_detection_data.csv' DELIMITER ',' CSV HEADER;

COPY migrations
(
    tag_id,
    antenna_origin,
    origin_arrival_date,
    origin_arrival_time,
    origin_departure_date,
    origin_departure_time,
    antenna_destination,
    destination_arrival_date,
    destination_arrival_time,
    destination_departure_date,
    destination_departure_time,
    species,
    marked_length
) 
FROM '/Users/chris/workspace/pruitt/fish-tracker/results/migrations.csv' DELIMITER ',' CSV HEADER;
