import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    if "token" not in cfg or not cfg["token"]:
        raise RuntimeError("Please put your bot token into config.json under the 'token' key.")
    cfg.setdefault("chat_id", 0)
    cfg.setdefault("inactivity_days", 7)
    cfg.setdefault("check_interval_hours", 12)
    cfg.setdefault("soft_kick", True)
    cfg.setdefault("channel_id", 0)
    cfg.setdefault("tz", "Europe/Moscow")
    cfg.setdefault("schedule_jitter_min", 15)
    cfg.setdefault("allowed_user_ids", [])
    cfg.setdefault("rules_tz", "Europe/Moscow")
    cfg.setdefault("rules_time", "06:00")
    cfg.setdefault("rules_message_file", "rules.txt")
    cfg.setdefault(
        "rules_message",
        (
            "Это официальная беседа rothko's kimono (он же роткочат)\n\n"
            "Правила роткочата:\n\n"
            "1. Мы стремимся к адекватной и дружелюбной атмосфере общения.\n\n"
            "2. Мы не баним за слова и мнения, даже если они нам не нравятся. \n\n"
            "3. Черный юмор, сарказм и умеренное ракование уместны и приемлемы, если они не "
            "оскорбляют участников дискуссии и других участников чата. Это является основным "
            "критерием — не стесняйтесь об этом сообщать.\n\n"
            "4. Мы не приемлем переход на личности"
        ),
    )
    cfg.setdefault("metrics_owner_ids", [])
    cfg.setdefault("metrics_dump_path", "private_metrics.ndjson")
    return cfg

CONFIG = load_config()