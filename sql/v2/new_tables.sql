-- Template: __N__ is replaced by physical table suffix 0 or 1 by the generator.
CREATE TABLE `tb_seckill_request___N__` (
  `id` bigint NOT NULL,
  `voucher_id` bigint NOT NULL,
  `user_id` bigint NOT NULL,
  `request_id` varchar(64) COLLATE utf8mb4_bin NOT NULL,
  `epoch` bigint NOT NULL,
  `status` varchar(16) NOT NULL,
  `expires_at` datetime(3) NOT NULL,
  `reason_code` varchar(64) DEFAULT NULL,
  `version` bigint NOT NULL DEFAULT 0,
  `auto_issue` tinyint NOT NULL DEFAULT 0,
  `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_request` (`voucher_id`,`user_id`,`request_id`),
  UNIQUE KEY `uk_order` (`voucher_id`,`id`),
  KEY `idx_expiry` (`status`,`expires_at`,`voucher_id`),
  KEY `idx_user` (`voucher_id`,`user_id`,`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `tb_seckill_active_purchase___N__` (
  `voucher_id` bigint NOT NULL,
  `user_id` bigint NOT NULL,
  `order_id` bigint NOT NULL,
  PRIMARY KEY (`voucher_id`,`user_id`),
  UNIQUE KEY `uk_order` (`voucher_id`,`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `tb_seckill_operation___N__` (
  `voucher_id` bigint NOT NULL,
  `operation_id` varchar(128) NOT NULL,
  `order_id` bigint DEFAULT NULL,
  `kind` varchar(32) NOT NULL,
  `available_delta` int NOT NULL DEFAULT 0,
  `reserved_delta` int NOT NULL DEFAULT 0,
  `sold_delta` int NOT NULL DEFAULT 0,
  `after_available` int NOT NULL,
  `after_reserved` int NOT NULL,
  `after_sold` int NOT NULL,
  `payload` text DEFAULT NULL,
  `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`voucher_id`,`operation_id`),
  KEY `idx_order` (`voucher_id`,`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `tb_seckill_outbox___N__` (
  `event_id` varchar(128) NOT NULL,
  `voucher_id` bigint NOT NULL,
  `aggregate_id` bigint DEFAULT NULL,
  `event_type` varchar(32) NOT NULL,
  `epoch` bigint NOT NULL,
  `sequence_no` bigint DEFAULT NULL,
  `payload` text NOT NULL,
  `status` varchar(16) NOT NULL DEFAULT 'PENDING',
  `attempts` int NOT NULL DEFAULT 0,
  `next_attempt_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `lease_until` datetime(3) DEFAULT NULL,
  `lease_owner` varchar(64) DEFAULT NULL,
  `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`event_id`),
  UNIQUE KEY `uk_projection_seq` (`voucher_id`,`sequence_no`),
  KEY `idx_dispatch` (`status`,`next_attempt_at`,`lease_until`),
  KEY `idx_aggregate` (`voucher_id`,`aggregate_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `tb_seckill_subscription___N__` (
  `voucher_id` bigint NOT NULL,
  `user_id` bigint NOT NULL,
  `subscribed_at` datetime(3) NOT NULL,
  `status` varchar(16) NOT NULL DEFAULT 'WAITING',
  `order_id` bigint DEFAULT NULL,
  `version` bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (`voucher_id`,`user_id`),
  KEY `idx_queue` (`voucher_id`,`status`,`subscribed_at`,`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `tb_seckill_recovery___N__` (
  `voucher_id` bigint NOT NULL,
  `epoch` bigint NOT NULL,
  `status` varchar(16) NOT NULL,
  `lease_owner` varchar(64) DEFAULT NULL,
  `lease_until` datetime(3) DEFAULT NULL,
  `phase` varchar(32) NOT NULL,
  `last_error` text DEFAULT NULL,
  `next_attempt_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`voucher_id`),
  KEY `idx_retry` (`status`,`next_attempt_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `tb_seckill_notification___N__` (
  `event_id` varchar(128) NOT NULL,
  `voucher_id` bigint NOT NULL,
  `user_id` bigint NOT NULL,
  `order_id` bigint DEFAULT NULL,
  `payload` text NOT NULL,
  `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`event_id`),
  KEY `idx_user` (`voucher_id`,`user_id`,`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
