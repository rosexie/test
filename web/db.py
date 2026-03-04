from __future__ import annotations

import os


def get_conn():
    import cx_Oracle

    return cx_Oracle.connect(
        user=os.getenv("ORACLE_USER", "BIGDATA_VISION"),
        password=os.getenv("ORACLE_PASSWORD", "BIGDATA_VISION"),
        dsn=os.getenv("ORACLE_DSN", "10.195.227.115:1526/VISION"),
        encoding="UTF-8",
    )
