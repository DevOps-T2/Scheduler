Table: scheduledcomputation
Create Table: CREATE TABLE `scheduledcomputation` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` varchar(255) NOT NULL,
  `memory_usage` int NOT NULL,
  `vcpu_usage` int NOT NULL,
  `mzn_url` varchar(255) NOT NULL,
  `dzn_url` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_UNIQUE` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=38 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

Table: scheduledcomputation_solver
Create Table: CREATE TABLE `scheduledcomputation_solver` (
  `scheduledcomputation_id` int NOT NULL,
  `solver_id` int NOT NULL,
  PRIMARY KEY (`solver_id`,`scheduledcomputation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci