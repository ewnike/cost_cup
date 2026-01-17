# schema_utils.py
from constants import SCHEMA


def fq(schema_key: str, obj_name: str) -> str:
    """
    Fully qualify an identifier like schema.table_or_view.

    Example:
      fq("raw", "game") -> "raw.game"
      fq("derived", f"game_plays_{season}_from_raw_pbp") -> "derived.game_plays_20192020_from_raw_pbp"

    """
    schema = SCHEMA[schema_key]
    return f"{schema}.{obj_name}"
