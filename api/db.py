# api/db.py
import os
from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.pool import NullPool

# 優先用 DATABASE_URL；若無則嘗試 Heroku 的 HEROKU_POSTGRESQL_*_URL
raw_db_url = os.getenv("DATABASE_URL", "")
if not raw_db_url:
    for k, v in os.environ.items():
        if k.startswith("HEROKU_POSTGRESQL_") and k.endswith("_URL") and v:
            raw_db_url = v
            break

if not raw_db_url:
    raise RuntimeError("缺少 DATABASE_URL（或 HEROKU_POSTGRESQL_*_URL）環境變數")

# SQLAlchemy 需要 'postgresql+psycopg2://'，Heroku 常見是 'postgres://'
if raw_db_url.startswith("postgres://"):
    raw_db_url = raw_db_url.replace("postgres://", "postgresql+psycopg2://", 1)
elif raw_db_url.startswith("postgresql://"):
    # 強制指定驅動，避免有些環境沒預設 driver
    raw_db_url = raw_db_url.replace("postgresql://", "postgresql+psycopg2://", 1)

# 若 URL 本身沒有帶 sslmode，則用 connect_args 加上（Heroku 要求）
connect_args = {}
if "sslmode=" not in raw_db_url:
    connect_args = {"sslmode": "require"}

# 使用 NullPool 避免在 Heroku/Serverless 環境的連線回收問題
engine = create_engine(
    raw_db_url,
    poolclass=NullPool,
    future=True,
    connect_args=connect_args,
)

def init_tables():
    """
    建立/補齊本服務所需資料表。
    - line_states：存放 LINE 使用者的對話狀態（簡易狀態機）
    - video_schedules：排程與上傳結果記錄
    """
    with engine.begin() as conn:
        conn.execute(sql_text("""
            CREATE TABLE IF NOT EXISTS line_states (
                line_user_id TEXT PRIMARY KEY,
                stage        TEXT,
                data         JSONB,
                updated_at   TIMESTAMPTZ DEFAULT now()
            );
        """))

        conn.execute(sql_text("""
            CREATE TABLE IF NOT EXISTS video_schedules (
                id              BIGSERIAL PRIMARY KEY,
                line_user_id    TEXT NOT NULL,
                folder_id       TEXT NOT NULL,
                folder_name     TEXT NOT NULL,
                video_type      TEXT CHECK (video_type IN ('long','short')) NOT NULL,
                meta_file_id    TEXT,
                meta_text       TEXT,
                schedule_time   TIMESTAMPTZ NOT NULL,
                status          TEXT DEFAULT 'scheduled',
                created_at      TIMESTAMPTZ DEFAULT now()
            );
        """))

        # 安全補欄位（多次執行不會報錯）
        conn.execute(sql_text("""
            ALTER TABLE video_schedules
            ADD COLUMN IF NOT EXISTS youtube_video_id TEXT
        """))
        conn.execute(sql_text("""
            ALTER TABLE video_schedules
            ADD COLUMN IF NOT EXISTS last_error TEXT
        """))
