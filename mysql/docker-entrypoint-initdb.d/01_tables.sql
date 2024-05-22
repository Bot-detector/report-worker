USE playerdata;

CREATE TABLE Players (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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

-- Insert into
CREATE TABLE `stgReports` (
  `ID` bigint NOT NULL AUTO_INCREMENT,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `reportedID` int NOT NULL,
  `reportingID` int NOT NULL,
  `region_id` int NOT NULL,
  `x_coord` int NOT NULL,
  `y_coord` int NOT NULL,
  `z_coord` int NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `manual_detect` tinyint(1) DEFAULT NULL,
  `on_members_world` int DEFAULT NULL,
  `on_pvp_world` tinyint DEFAULT NULL,
  `world_number` int DEFAULT NULL,
  `equip_head_id` int DEFAULT NULL,
  `equip_amulet_id` int DEFAULT NULL,
  `equip_torso_id` int DEFAULT NULL,
  `equip_legs_id` int DEFAULT NULL,
  `equip_boots_id` int DEFAULT NULL,
  `equip_cape_id` int DEFAULT NULL,
  `equip_hands_id` int DEFAULT NULL,
  `equip_weapon_id` int DEFAULT NULL,
  `equip_shield_id` int DEFAULT NULL,
  `equip_ge_value` bigint DEFAULT NULL,
  PRIMARY KEY (`ID`)
)
;