USE playerdata;

CREATE TABLE Players (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    possible_ban BOOLEAN,
    confirmed_ban BOOLEAN,
    confirmed_player BOOLEAN,
    label_id INTEGER,
    label_jagex INTEGER,
    ironman BOOLEAN,
    hardcore_ironman BOOLEAN,
    ultimate_ironman BOOLEAN,
    normalized_name TEXT
);
