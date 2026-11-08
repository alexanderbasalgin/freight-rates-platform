import os

import pytest

from etl import config, db


@pytest.fixture(scope="session")
def dwh():
    if not os.getenv("DWH_DB_PASSWORD"):
        pytest.skip("DWH не настроен (нет DWH_DB_*): интеграционные тесты пропущены")
    conn = db.connect(config.dwh_db(), retries=3)
    conn.autocommit = True
    yield conn
    conn.close()


def scalar(conn, sql, params=None):
    return conn.execute(sql, params).fetchone()[0]
