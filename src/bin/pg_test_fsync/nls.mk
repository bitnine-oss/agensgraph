# src/bin/pg_test_fsync/nls.mk
CATALOG_NAME     = pg_test_fsync
AVAIL_LANGUAGES  = cs de el es fr ja ko pl ru sv tr uk vi zh_CN
GETTEXT_FILES    = $(FRONTEND_COMMON_GETTEXT_FILES) pg_test_fsync.c
GETTEXT_TRIGGERS = $(FRONTEND_COMMON_GETTEXT_TRIGGERS) die
GETTEXT_FLAGS    = $(FRONTEND_COMMON_GETTEXT_FLAGS)
