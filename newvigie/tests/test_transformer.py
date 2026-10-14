import unittest

import pandas as pd

from pipeline.transformer import normalize_column_name, transform_reports


class TransformerTests(unittest.TestCase):
    def test_column_names_are_ascii_snake_case(self) -> None:
        self.assertEqual(normalize_column_name("Type de demande / État"), "type_de_demande_etat")

    def test_union_numeric_cleanup_and_lineage(self) -> None:
        reports = [
            (
                pd.DataFrame(
                    {
                        "Organisme": ["Ministère A"],
                        "Montant total": ["1 234,50 €"],
                        "Indicateur": ["A"],
                    }
                ),
                "gs://bucket/rapport-a.csv",
            ),
            (
                pd.DataFrame(
                    {
                        "Organisation": ["Ministère B"],
                        "Montant total": ["-"],
                        "Commentaire": ["Ajout"],
                    }
                ),
                "gs://bucket/dossier/rapport-b.csv",
            ),
        ]

        result = transform_reports(reports)

        self.assertEqual(len(result), 2)
        self.assertEqual(
            result.loc[result["organisme"] == "Ministère A", "montant_total"].iloc[0],
            1234.5,
        )
        self.assertEqual(
            result.loc[result["organisme"] == "Ministère B", "montant_total"].iloc[0],
            0,
        )
        self.assertTrue(pd.isna(result.loc[result["organisme"] == "Ministère B", "indicateur"].iloc[0]))
        self.assertIn("gs://bucket/dossier/rapport-b.csv", result["source_file"].tolist())
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result["ingested_at"]))

    def test_strict_and_business_duplicates_keep_latest_version(self) -> None:
        reports = [
            (
                pd.DataFrame(
                    {
                        "Date": ["2025-01-01", "2025-01-01"],
                        "Organisme": ["Ville A", "Ville A"],
                        "Type de demande": ["Accès", "Accès"],
                        "Montant": ["10", "10"],
                    }
                ),
                "gs://bucket/ancien.csv",
            ),
            (
                pd.DataFrame(
                    {
                        "date rapport": ["2025-01-01"],
                        "organisation": ["Ville A"],
                        "type_demande": ["Accès"],
                        "montant": ["12"],
                    }
                ),
                "gs://bucket/recent.csv",
            ),
        ]

        result = transform_reports(reports)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["montant"], 12)
        self.assertEqual(result.iloc[0]["source_file"], "gs://bucket/recent.csv")

    def test_dirty_numeric_values_and_thousand_separators(self) -> None:
        reports = [
            (
                pd.DataFrame(
                    {
                        "Montant": ["1,234,567.89", "1.234.567,89", "erreur"],
                    }
                ),
                "gs://bucket/montants.csv",
            )
        ]

        result = transform_reports(reports)

        self.assertEqual(result["montant"].iloc[0], 1234567.89)
        self.assertEqual(result["montant"].iloc[1], 1234567.89)
        self.assertTrue(pd.isna(result["montant"].iloc[2]))

    def test_numeric_looking_entity_codes_remain_strings(self) -> None:
        reports = [
            (
                pd.DataFrame({"Organisme": ["001", "002"]}),
                "gs://bucket/organismes.csv",
            )
        ]

        result = transform_reports(reports)

        self.assertEqual(result["organisme"].tolist(), ["001", "002"])


if __name__ == "__main__":
    unittest.main()
