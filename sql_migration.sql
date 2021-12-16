Table: scheduler
Create Table: CREATE TABLE `scheduler` (
  `id` int NOT NULL AUTO_INCREMENT,
  `user_id` varchar(255) DEFAULT NULL,
  `job_memory` int DEFAULT NULL,
  `job_vcpu` int DEFAULT NULL,
  `mzn_id` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_UNIQUE` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

Table: scheduler_solver
Create Table: CREATE TABLE `scheduler_solver` (
  `scheduler_id` int NOT NULL,
  `solver_id` int NOT NULL,
  PRIMARY KEY (`solver_id`,`scheduler_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci