from src.config import load_config
from src.universe import build_universe_once, print_universe_summary, save_universe_json


def main():
    cfg = load_config()
    universe = build_universe_once(cfg)
    print_universe_summary(universe)
    save_universe_json(universe, "data/universe.json")
    print("\nSaved to: data/universe.json")


if __name__ == "__main__":
    main()
