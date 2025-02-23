from sqlalchemy import Column, Float, Integer, String, Table


def define_game_player_info_test(metadata):
    """Define the player_info_test table schema."""
    return Table(
        "player_info_test",
        metadata,
        Column("player_id", Integer, primary_key=True),
        Column("firstName", String(50)),
        Column("lastName", String(50)),
        Column("nationality", String(10)),
        Column("birthCity", String(50)),
        Column("primaryPosition", String(10)),
        Column("birthDate", String(20)),
        Column("birthStateProvince", String(50), nullable=True),
        Column("height_cm", Float),  # ✅ Use float for converted height in cm
        Column("weight", Integer),
        Column("shootsCatches", String(5)),
    )
