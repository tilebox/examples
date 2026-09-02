import dotenv
import shapely
from shapely import MultiPolygon
from tilebox.workflows import Client

from s2_stats import S2Stats

# aois defines the areas of interest, for which statistics are calculated
# Polygons created using Tilebox Console:
# - [Switzerland](https://console.tilebox.com/datasets/explorer/34d7b08b-8a27-4b40-819b-b11c6189695a?collectionId=5dea4d16-30f3-4f14-b713-8995fec173e8&view=explorer&polygons=AQETqQKXEjYCCxJMAv0RkQIaEsYC2BEpA%2BQRQwMWEoQD3BGuAxYSDAQCEg4EIxIiBCoSKARhEvQDXBLQA24S4AOMElQDuhI8A6cSqQKXEg==)
# - [USA](https://console.tilebox.com/datasets/explorer/34d7b08b-8a27-4b40-819b-b11c6189695a?collectionId=5dea4d16-30f3-4f14-b713-8995fec173e8&view=explorer&polygons=AwETwc4aE8/OjQ%2Bc0VoMOtb/C2HakAl82qMKyt5eC0zgbAkg4XgJtuC7C3riUA1c44kOu%2BYsEaTl9xK54w0SqeBlESveCBOk2pMTwc4aEwEHG8SrB9bClwh%2BwfQIwMBZCPbBBggFwyAHG8SrBwEJksnUFnbJmhuRwh8cv70QG0G%2BNBfGwDsWb7mEFCm6ZBOSydQW)
AREAS: dict[str, shapely.Geometry] = {
    "Switzerland": MultiPolygon(
        [
            (
                (
                    (6.81, 47.59),
                    (5.66, 46.19),
                    (5.88, 46.05),
                    (6.57, 46.34),
                    (7.10, 45.68),
                    (8.09, 45.80),
                    (8.35, 46.30),
                    (9.00, 45.72),
                    (9.42, 46.30),
                    (10.36, 46.10),
                    (10.38, 46.43),
                    (10.58, 46.50),
                    (10.64, 47.05),
                    (10.12, 47.00),
                    (9.76, 47.18),
                    (9.92, 47.48),
                    (8.52, 47.94),
                    (8.28, 47.75),
                    (6.81, 47.59),
                ),
            ),
        ]
    ),
    "USA": MultiPolygon(
        [
            (
                (
                    (-126.07, 48.90),
                    (-125.93, 39.81),
                    (-118.76, 31.62),
                    (-106.94, 30.71),
                    (-96.31, 24.48),
                    (-96.04, 27.23),
                    (-85.02, 29.10),
                    (-81.16, 24.12),
                    (-79.04, 24.24),
                    (-80.10, 30.03),
                    (-75.58, 34.08),
                    (-73.32, 37.21),
                    (-64.69, 43.96),
                    (-67.48, 48.55),
                    (-72.39, 46.21),
                    (-80.23, 44.53),
                    (-86.61, 48.72),
                    (-95.64, 50.11),
                    (-126.07, 48.90),
                ),
            ),
            (
                (
                    (-153.33, 19.63),
                    (-156.58, 21.99),
                    (-160.02, 22.92),
                    (-161.92, 21.37),
                    (-158.82, 20.54),
                    (-156.11, 18.24),
                    (-153.33, 19.63),
                ),
            ),
            (
                (
                    (-139.34, 58.44),
                    (-139.62, 70.66),
                    (-157.27, 71.99),
                    (-169.61, 69.28),
                    (-168.31, 59.40),
                    (-161.86, 56.91),
                    (-180.65, 52.52),
                    (-178.79, 49.64),
                    (-139.34, 58.44),
                ),
            ),
        ]
    ),
}


def main() -> None:
    dotenv.load_dotenv()
    automations = Client().automations()
    name = "s2-stats-automation"
    if any(automation.name == name for automation in automations.all()):
        print("Cron automation already exists")
        return

    automation = automations.create_cron_automation(
        name,
        S2Stats(areas=AREAS),
        "* * * * *",
        max_retries=3,
    )
    print(f"Created cron automation {automation.name}")


if __name__ == "__main__":
    main()
