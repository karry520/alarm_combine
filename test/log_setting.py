LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'verbose': {
            'format': '[%(asctime)s] - [%(levelname)s] - %(message)s'
        },
        'simple': {
            'format': '[%(asctime)s] - [%(levelname)s] - %(message)s'
        },
    },
    'filters': {
    },
    'handlers': {
        'null': {
            'level': 'DEBUG',
            'class': 'logging.NullHandler',
        },
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
        'alarm_combine_handler': {
            'level': 'INFO',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': 'log/alarm_combine.log',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 0,
            'formatter': 'verbose'
        },
        'related_fusion_handler': {
            'level': 'INFO',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': 'log/related_fusion.log',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 0,
            'formatter': 'verbose'
        },
        'database_utils_handler': {
            'level': 'INFO',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': 'log/database_utils.log',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 0,
            'formatter': 'verbose'
        },
        'engine_report_handler': {
            'level': 'INFO',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': 'log/engine_report.log',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 0,
            'formatter': 'verbose'
        },
    },
    'loggers': {
        'alarm_combine': {
            'handlers': ['console', 'alarm_combine_handler'],
            'propagate': False,
            'level': 'INFO',
        },
        'related_fusion': {
            'handlers': ['console', 'related_fusion_handler'],
            'propagate': False,
            'level': 'INFO',
        },
        'database_utils': {
            'handlers': ['console', 'database_utils_handler'],
            'propagate': False,
            'level': 'INFO',
        },
        'engine_report': {
            'handlers': ['console', 'engine_report_handler'],
            'propagate': False,
            'level': 'INFO',
        }
    }
}
