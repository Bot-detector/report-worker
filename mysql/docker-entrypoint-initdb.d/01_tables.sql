USE playerdata;

CREATE TABLE `Players` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` text NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT NULL,
  `possible_ban` tinyint(1) NOT NULL DEFAULT '0',
  `confirmed_ban` tinyint(1) NOT NULL DEFAULT '0',
  `confirmed_player` tinyint(1) NOT NULL DEFAULT '0',
  `label_id` int NOT NULL DEFAULT '0',
  `label_jagex` int NOT NULL DEFAULT '0',
  `ironman` tinyint DEFAULT NULL,
  `hardcore_ironman` tinyint DEFAULT NULL,
  `ultimate_ironman` tinyint DEFAULT NULL,
  `normalized_name` text,
  PRIMARY KEY (`id`),
  UNIQUE KEY `Unique_name` (`name`(50)),
  KEY `FK_label_id` (`label_id`),
  KEY `confirmed_ban_idx` (`confirmed_ban`),
  KEY `normal_name_index` (`normalized_name`(50)),
  KEY `Players_label_jagex_IDX` (`label_jagex`) USING BTREE,
  KEY `Players_possible_ban_IDX` (`possible_ban`,`confirmed_ban`) USING BTREE
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
);

CREATE TABLE `Reports` (
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
    PRIMARY KEY (`ID`),
    UNIQUE KEY `Unique_Report` (
        `reportedID`,
        `reportingID`,
        `region_id`,
        `manual_detect`
    ),
    KEY `idx_reportingID` (`reportingID`),
    KEY `idx_reportedID_regionDI` (`reportedID`, `region_id`),
    KEY `idx_heatmap` (
        `reportedID`,
        `timestamp`,
        `region_id`
    ),
    CONSTRAINT `FK_Reported_Players_id` FOREIGN KEY (`reportedID`) REFERENCES `Players` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT `FK_Reporting_Players_id` FOREIGN KEY (`reportingID`) REFERENCES `Players` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE report_sighting (
    `report_sighting_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `reporting_id` INT UNSIGNED NOT NULL,
    `reported_id` INT UNSIGNED NOT NULL,
    `manual_detect` TINYINT(1) DEFAULT 0,
    PRIMARY key (`report_sighting_id`),
    UNIQUE KEY unique_sighting (`reporting_id`, `reported_id`, `manual_detect`),
    KEY idx_reported_id (`reported_id`)
);
