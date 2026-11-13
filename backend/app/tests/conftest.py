import sys
from pathlib import Path

import pytest

# 保证测试无论从哪个目录启动都能 import app
BACKEND_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    """每个测试一个全新的 DATA_DIR + SQLite 库，不碰开发/演示数据。

    app.db.connect() 在调用时读取模块级 DB_PATH，直接 patch 该全局即可让所有
    repository（它们都 ``from app.db import connect``）落到临时库。
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    db_path = data_dir / "app.db"

    from app import db, seed

    monkeypatch.setattr(db, "DB_PATH", db_path)
    seed.init_db()
    yield data_dir
