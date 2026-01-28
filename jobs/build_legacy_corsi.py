"""Legacy Corsi build job."""

from config_helpers import build_all_configs


def run_legacy_corsi() -> None:
    """Run_legacy_corsi."""
    configs = build_all_configs()
    # TODO: replace with your actual function that processes configs
    # process_all_configs(configs)
    print(f"Legacy Corsi job would run {len(configs)} configs.")
