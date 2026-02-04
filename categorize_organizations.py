"""
Categorize organizations in donation data CSV files.

This module provides functions to automatically categorize organizations based on
their names and classifications into predefined categories.
"""

import polars as pl
from typing import Optional

CATEGORIES = [
    "Religious Organizations",
    "Educational",
    "Healthcare/Medical",
    "Disability Services",
    "Children/Youth",
    "Elderly Care",
    "Environmental/Conservation",
    "Cultural/Arts",
    "Sports/Recreation",
    "Corporate Foundations",
    "Emergency/Disaster Relief",
    "Research/Academic",
    "Welfare/Social Services",
    "Animals",
    "Others",
]


def categorize_organization(org_name: str, classification: Optional[str] = None) -> str:
    """
    Categorize organization based on name and classification.

    Args:
        org_name: Name of the organization
        classification: Optional classification field from the data

    Returns:
        Category name as string
    """
    org_upper = org_name.upper()

    # Religious Organizations (including religious welfare/charity)
    religious_keywords = [
        "MASJID",
        "GEREJA",
        "CHURCH",
        "TEMPLE",
        "KUIL",
        "TOKONG",
        "SURAU",
        "WAT",
        "BUDDHA",
        "BUDDHIST",
        "ISLAM",
        "ISLAMIC",
        "CHRISTIAN",
        "CATHOLIC",
        "TABUNG PEMBINAAN RUMAH IBADAT",
        "TABUNG PENGURUSAN RUMAH IBADAT",
        "TPBRI",
        "TPRI",
        "RELIGIOUS",
        "FAITH",
        "MISSIONARY",
        "MISSION",
        "YAYASAN ISLAM",
        "ISLAMIC RELIEF",
        "MUSLIM AID",
        "CATHOLIC WELFARE",
        "AGAMA ISLAM",
        "MAJLIS AGAMA",
        "WANITA ISLAM",
        "WAKAF",
        "WAQAF",
        "WORSHIP",
        "TABERNACLE",
        "GOSPEL",
    ]
    if any(keyword in org_upper for keyword in religious_keywords):
        return "Religious Organizations"

    # Healthcare/Medical (check before children/youth)
    healthcare_keywords = [
        "HOSPITAL",
        "HOSPIS",
        "HOSPICE",
        "DIALISIS",
        "DIALYSIS",
        "HEMODIALISIS",
        "HAEMODIALYSIS",
        "MEDICAL",
        "CANCER",
        "DIABETES",
        "ALZHEIMER",
        "PSIKIATRI",
        "PSYCHIATRY",
        "RENAL",
        "KIDNEY",
        "CLINIC",
        "KLINIK",
        "MATERNITY",
        "PAEDIATRIC",
        "MEDICAL CENTRE",
        "MEDICAL FOUNDATION",
        "ARTHRITIS",
        "BREAST CANCER",
        "KANSER",
        "JANTUNG",
        "HAEMODIALISIS",
        # Disease/condition specific keywords
        "THALASSAEMIA",
        "SPINAL",
        "BARAH",
        "AIDS",
        "HIV",
        # Health-related (specific contexts)
        "KESIHATAN REPRODUKTIF",
        "KESIHATAN JIWA",
        "KESIHATAN MENTAL",
        "KESIHATAN KELUARGA",
        "KESIHATAN MATA",
        "MENTAL HEALTH",
        "REPRODUCTIVE HEALTH",
        "FAMILY PLANNING",
    ]
    if any(keyword in org_upper for keyword in healthcare_keywords):
        return "Healthcare/Medical"

    # Educational
    education_keywords = [
        "SEKOLAH",
        "SCHOOL",
        "PELAJARAN",
        "EDUCATION",
        "AKADEMI",
        "ACADEMY",
        "KOLEJ",
        "COLLEGE",
        "UNIVERSITY",
        "UNIVERSITI",
        "SCHOLARSHIP",
        "BIASISWA",
        "TABUNG PEMBINAAN SEKOLAH",
        "TPBS",
        "YAYASAN PENDIDIKAN",
        "EDUCATION FUND",
        "EDUCATIONAL",
        "SJK",
        "SJKC",
        "TADIKA",
        "ENDOWMEN",
        "ENDOWMENT",
    ]
    if any(keyword in org_upper for keyword in education_keywords):
        return "Educational"

    # Disability Services
    disability_keywords = [
        "CACAT",
        "DISABLED",
        "SPASTIK",
        "CEREBRAL PALSY",
        "PEKAK",
        "DEAF",
        "ISTIMEWA",
        "DOWN SYNDROME",
        "AUTISM",
        "BLIND",
        "BUTA",
        "PEMULIHAN DALAM KOMUNITI",
        "PDK",
        "TAMAN SINAR HARAPAN",
        "TERENCAT",
        "KURANG UPAYA",
        "OKU",
    ]
    if any(keyword in org_upper for keyword in disability_keywords):
        return "Disability Services"

    # Children/Youth
    children_keywords = [
        "KANAK-KANAK",
        "CHILDREN",
        "YOUTH",
        "BELIA",
        "ANAK YATIM",
        "CHILD",
        "YATIM",
        "ORPHAN",
        "PELAWAT RUMAH KANAK-KANAK",
        "CHILD WELFARE",
        "KANAK-KANAK YATIM",
        "MONTFORT YOUTH",
    ]
    if any(keyword in org_upper for keyword in children_keywords):
        return "Children/Youth"

    # Elderly Care
    elderly_keywords = [
        "RUMAH SERI KENANGAN",
        "WARGA TUA",
        "ELDERLY",
        "SENIOR CITIZEN",
        "WARGA EMAS",
        "SERI KENANGAN",
        "RUMAH WARGA EMAS",
        "RUMAH TUA",
    ]
    if any(keyword in org_upper for keyword in elderly_keywords):
        return "Elderly Care"

    # Environmental/Conservation
    environmental_keywords = [
        "ALAM",
        "ENVIRONMENT",
        "CONSERVATION",
        "NATURAL HERITAGE",
        "WETLAND",
        "ZOOLOGICAL",
        "WILDLIFE",
        "ECOLOGY",
        "BORNEO CONSERVATION",
        "NATURAL HERITAGE",
        "PEKA",
        "ELEPHANT CONSERVATION",
    ]
    if any(keyword in org_upper for keyword in environmental_keywords):
        return "Environmental/Conservation"

    # Research/Academic
    research_keywords = [
        "RESEARCH",
        "INSTITUTE",
        "STUDIES",
        "SCIENCE AWARD",
        "ACADEMIC",
        "PENANG INSTITUTE",
        "PENYELIDIKAN",
        "BLUE OCEAN STRATEGY",
        "SOCIAL RESEARCH",
    ]
    if any(keyword in org_upper for keyword in research_keywords):
        return "Research/Academic"

    # Cultural/Arts
    cultural_keywords = [
        "MUZIUM",
        "MUSEUM",
        "SENI",
        "ART",
        "CULTURAL",
        "FILHARMONIK",
        "PHILHARMONIC",
        "ORCHESTRA",
        "GALLERY",
        "BALAI SENI",
        "INSTITUTE OF ART",
    ]
    if any(keyword in org_upper for keyword in cultural_keywords):
        return "Cultural/Arts"

    # Sports/Recreation
    sports_keywords = [
        "SUKAN",
        "SPORTS",
        "RUGBY",
        "ATHLETICS",
        "OLYMPIC",
        "PARALYMPIC",
        "GOLF",
        "STADIUM",
        "ATHLETE",
        "TABUNG SUKAN",
        "OLIMPIK",
    ]
    if any(keyword in org_upper for keyword in sports_keywords):
        return "Sports/Recreation"

    # Emergency/Disaster Relief
    emergency_keywords = [
        "BENCANA",
        "DISASTER",
        "RELIEF",
        "EMERGENCY",
        "FOOD AID",
        "HUMANITARIAN",
        "MUHIBBAH FOOD BANK",
        "FOOD BANK",
    ]
    if any(keyword in org_upper for keyword in emergency_keywords):
        return "Emergency/Disaster Relief"

    # Animals (check before Welfare/Social Services to catch animal welfare orgs)
    animal_keywords = [
        "HAIWAN",
        "ANIMAL",
        "BINATANG",
        "SPCA",
        "PENYELAMAT HAIWAN",
        "KEBAJIKAN HAIWAN",
        "PERLINDUNGAN HAIWAN",
        "MENCEGAH PENYEKSAAN BINATANG",
        "MENCEGAH KEZALIMAN TERHADAP HAIWAN",
        "PREVENTION OF CRUELTY TO ANIMALS",
        "ANIMAL WELFARE",
        "ANIMAL RESCUE",
    ]
    if any(keyword in org_upper for keyword in animal_keywords):
        return "Animals"

    # Welfare/Social Services (only explicit welfare keywords)
    welfare_keywords = [
        "KEBAJIKAN",
        "WELFARE",
        "YATIM",
        "MISKIN",
        "ASNAF",
        "AMAL",
        "CHARITY",
    ]
    if any(keyword in org_upper for keyword in welfare_keywords):
        return "Welfare/Social Services"

    # Default to Others for manual review
    return "Others"


def add_category_column(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add category column to a DataFrame.

    Args:
        df: DataFrame with 'organization' column (and optionally 'classification')

    Returns:
        DataFrame with 'category' column added
    """
    has_classification = "classification" in df.columns

    if has_classification:
        df = df.with_columns(
            [
                pl.struct(["organization", "classification"])
                .map_elements(
                    lambda x: categorize_organization(x["organization"], x["classification"]), return_dtype=pl.Utf8
                )
                .alias("category")
            ]
        )
    else:
        df = df.with_columns(
            [
                pl.col("organization")
                .map_elements(lambda x: categorize_organization(x, None), return_dtype=pl.Utf8)
                .alias("category")
            ]
        )

    return df


def categorize_csv_file(csv_path: str, output_path: Optional[str] = None, verbose: bool = True) -> pl.DataFrame:
    """
    Categorize organizations in a CSV file and add category column.

    Args:
        csv_path: Path to input CSV file
        output_path: Path to output CSV file (if None, overwrites input)
        verbose: Whether to print summary information

    Returns:
        DataFrame with category column added
    """
    # Read CSV
    df = pl.read_csv(csv_path)

    # Verify required columns
    if "organization" not in df.columns:
        raise ValueError("CSV must contain 'organization' column")

    # Add category column
    df = add_category_column(df)

    # Save results
    output = output_path if output_path else csv_path
    df.write_csv(output)

    # Print summary
    if verbose:
        print(f"Processed {len(df)} rows from {csv_path}")
        print(f"Categories: {df['category'].value_counts().sort('count', descending=True)}")
        print(f"Saved to {output}")

    return df


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python categorize_organizations.py <csv_file> [output_file]")
        print("  csv_file: Path to input CSV file")
        print("  output_file: Optional path to output CSV file (default: overwrites input)")
        sys.exit(1)

    csv_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None

    categorize_csv_file(csv_path, output_path)
