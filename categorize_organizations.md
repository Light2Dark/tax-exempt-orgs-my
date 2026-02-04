# Categorize Organizations in Donations Data

This skill can be applied to any CSV file containing donation/organization data with `organization` and `classification` columns.

## Categories

1. **Religious Organizations** - Mosques, churches, temples, religious building funds, wakaf/waqaf funds, and religious welfare/charity organizations
2. **Educational** - Schools, scholarships, educational foundations, academies, colleges, universities, endowments
3. **Healthcare/Medical** - Hospitals, hospices, dialysis centers, medical foundations, disease-specific organizations
4. **Disability Services** - Organizations for persons with disabilities
5. **Children/Youth** - Organizations focused on children and youth development
6. **Elderly Care** - Homes and services for the elderly
7. **Environmental/Conservation** - Environmental protection, conservation, natural heritage
8. **Cultural/Arts** - Museums, art galleries, cultural organizations, heritage
9. **Sports/Recreation** - Sports organizations, recreational groups
10. **Corporate Foundations** - Corporate foundations and trusts (separate from general welfare)
11. **Emergency/Disaster Relief** - Disaster relief, emergency assistance, food banks
12. **Research/Academic** - Research institutes, academic foundations
13. **Welfare/Social Services** - General welfare, orphanages, poverty relief, social assistance (default category)

## Classification Reasoning

### Order of Checks
The order matters because some organizations could match multiple categories. Check in this order:

1. **Religious Organizations** - Check first to catch all religious-related organizations including welfare
2. **Healthcare/Medical** - Check before Children/Youth to catch medical organizations for children (e.g., "PERSATUAN JANTUNG KANAK-KANAK")
3. **Educational** - Check before other categories to catch educational foundations
4. **Disability Services** - Check before Children/Youth to catch disability services for children
5. **Children/Youth** - General children/youth organizations
6. **Elderly Care** - Specific elderly care keywords
7. **Environmental/Conservation** - Environmental keywords
8. **Research/Academic** - Check before Cultural/Arts to catch research institutes
9. **Cultural/Arts** - Museums, galleries, arts
10. **Sports/Recreation** - Sports-related
11. **Corporate Foundations** - Check for known corporate patterns, but exclude if clearly educational/healthcare
12. **Emergency/Disaster Relief** - Disaster and food aid
13. **Welfare/Social Services** - Default catch-all

### Key Patterns

- **Religious**: Include both places of worship (MASJID, GEREJA, CHURCH, TEMPLE, WORSHIP, TABERNACLE) AND religious welfare (ISLAMIC RELIEF, CATHOLIC WELFARE, MAJLIS AGAMA, WAKAF, WAQAF)
- **Educational**: Includes universities (UNIVERSITI, UNIVERSITY), endowments (ENDOWMEN, ENDOWMENT), and all educational institutions
- **Corporate Foundations**: Must exclude educational/healthcare organizations even if they match corporate keywords
- **Healthcare**: Includes disease-specific orgs (CANCER, DIABETES, ARTHRITIS) and medical facilities (KESIHATAN = health in Malay)
- **Disability**: Includes both Malay (CACAT, ISTIMEWA, PEKAK) and English (DISABLED, DEAF, BLIND) terms

## Python Steps

### 1. Analyze Data Structure

```python
import polars as pl

# Read CSV (replace with your file path)
csv_path = 'public/generated/subsection_44_6/subsection_44_6.csv'
df = pl.read_csv(csv_path)

# Basic stats
print(f"Total rows: {len(df)}")
print(f"Unique organizations: {df['organization'].n_unique()}")
print(f"Columns: {df.columns}")

# Verify required columns exist
required_cols = ['organization', 'classification']
assert all(col in df.columns for col in required_cols), f"Missing required columns. Need: {required_cols}"

# Sample organizations
print(df['organization'].unique().head(50).to_list())
```

### 2. Implement Categorization Function

```python
def categorize_organization(org_name, classification):
    """Categorize organization based on name and classification"""
    org_upper = org_name.upper()
    
    # Religious Organizations (including religious welfare/charity)
    religious_keywords = [
        'MASJID', 'GEREJA', 'CHURCH', 'TEMPLE', 'KUIL', 'TOKONG', 'SURAU', 'WAT',
        'BUDDHA', 'BUDDHIST', 'ISLAM', 'ISLAMIC', 'CHRISTIAN', 'CATHOLIC', 
        'TABUNG PEMBINAAN RUMAH IBADAT', 'TABUNG PENGURUSAN RUMAH IBADAT',
        'TPBRI', 'TPRI', 'RELIGIOUS', 'FAITH', 'MISSIONARY', 'MISSION',
        'YAYASAN ISLAM', 'ISLAMIC RELIEF', 'MUSLIM AID', 'CATHOLIC WELFARE',
        'AGAMA ISLAM', 'MAJLIS AGAMA', 'WANITA ISLAM', 'WAKAF', 'WAQAF',
        'WORSHIP', 'TABERNACLE', 'GOSPEL'
    ]
    if any(keyword in org_upper for keyword in religious_keywords):
        return 'Religious Organizations'
    
    # Healthcare/Medical (check before children/youth)
    healthcare_keywords = [
        'HOSPITAL', 'HOSPIS', 'HOSPICE', 'DIALISIS', 'DIALYSIS',
        'HEMODIALISIS', 'HAEMODIALYSIS', 'MEDICAL', 'CANCER', 'DIABETES',
        'ALZHEIMER', 'PSIKIATRI', 'PSYCHIATRY', 'RENAL', 'KIDNEY',
        'HEALTH', 'CLINIC', 'KLINIK', 'MATERNITY', 'PAEDIATRIC',
        'MEDICAL CENTRE', 'MEDICAL FOUNDATION', 'ARTHRITIS',
        'BREAST CANCER', 'KANSER', 'JANTUNG', 'HEART', 'HAEMODIALISIS',
        'AIA HAVE-A-HEART', 'KESIHATAN'
    ]
    if any(keyword in org_upper for keyword in healthcare_keywords):
        return 'Healthcare/Medical'
    
    # Educational
    education_keywords = [
        'SEKOLAH', 'SCHOOL', 'PELAJARAN', 'EDUCATION', 'AKADEMI', 'ACADEMY',
        'KOLEJ', 'COLLEGE', 'UNIVERSITY', 'UNIVERSITI', 'SCHOLARSHIP', 'BIASISWA',
        'TABUNG PEMBINAAN SEKOLAH', 'TPBS', 'STUDENT', 'PELAJAR',
        'YAYASAN PENDIDIKAN', 'EDUCATION FUND', 'EDUCATIONAL',
        'SJK', 'SJKC', 'TADIKA', 'ENDOWMEN', 'ENDOWMENT'
    ]
    if any(keyword in org_upper for keyword in education_keywords):
        return 'Educational'
    
    # Disability Services
    disability_keywords = [
        'CACAT', 'DISABLED', 'SPASTIK', 'CEREBRAL PALSY', 'PEKAK', 'DEAF',
        'ISTIMEWA', 'SPECIAL', 'DOWN SYNDROME', 'AUTISM', 'BLIND', 'BUTA',
        'PEMULIHAN DALAM KOMUNITI', 'PDK', 'TAMAN SINAR HARAPAN',
        'PEMULIHAN', 'REHABILITATION', 'TERENCAT', 'KURANG UPAYA', 'OKU'
    ]
    if any(keyword in org_upper for keyword in disability_keywords):
        return 'Disability Services'
    
    # Children/Youth
    children_keywords = [
        'KANAK-KANAK', 'CHILDREN', 'YOUTH', 'BELIA', 'ANAK YATIM', 'CHILD',
        'YATIM', 'ORPHAN', 'PELAWAT RUMAH KANAK-KANAK', 'CHILD WELFARE',
        'KANAK-KANAK YATIM', 'MONTFORT YOUTH'
    ]
    if any(keyword in org_upper for keyword in children_keywords):
        return 'Children/Youth'
    
    # Elderly Care
    elderly_keywords = [
        'RUMAH SERI KENANGAN', 'WARGA TUA', 'ELDERLY', 'SENIOR',
        'SENIOR CITIZEN', 'WARGA EMAS', 'SERI KENANGAN',
        'RUMAH WARGA EMAS', 'RUMAH TUA'
    ]
    if any(keyword in org_upper for keyword in elderly_keywords):
        return 'Elderly Care'
    
    # Environmental/Conservation
    environmental_keywords = [
        'ALAM', 'ENVIRONMENT', 'CONSERVATION', 'NATURAL HERITAGE',
        'WETLAND', 'ZOOLOGICAL', 'WILDLIFE', 'GREEN', 'ECOLOGY',
        'BORNEO CONSERVATION', 'NATURAL HERITAGE', 'PEKA',
        'ELEPHANT CONSERVATION'
    ]
    if any(keyword in org_upper for keyword in environmental_keywords):
        return 'Environmental/Conservation'
    
    # Research/Academic
    research_keywords = [
        'RESEARCH', 'INSTITUTE', 'STUDIES', 'SCIENCE AWARD',
        'TECHNOLOGY', 'ACADEMIC', 'PENANG INSTITUTE', 'PENYELIDIKAN',
        'BLUE OCEAN STRATEGY', 'SOCIAL RESEARCH'
    ]
    if any(keyword in org_upper for keyword in research_keywords):
        return 'Research/Academic'
    
    # Cultural/Arts
    cultural_keywords = [
        'MUZIUM', 'MUSEUM', 'SENI', 'ART', 'CULTURAL', 'HERITAGE',
        'FILHARMONIK', 'PHILHARMONIC', 'ORCHESTRA', 'GALLERY',
        'BALAI SENI', 'INSTITUTE OF ART'
    ]
    if any(keyword in org_upper for keyword in cultural_keywords):
        return 'Cultural/Arts'
    
    # Sports/Recreation
    sports_keywords = [
        'SUKAN', 'SPORTS', 'RUGBY', 'ATHLETICS', 'OLYMPIC', 'PARALYMPIC',
        'GOLF', 'STADIUM', 'ATHLETE', 'TABUNG SUKAN', 'OLIMPIK'
    ]
    if any(keyword in org_upper for keyword in sports_keywords):
        return 'Sports/Recreation'
    
    # Corporate Foundations
    corporate_keywords = [
        'AXIATA FOUNDATION', 'CIMB FOUNDATION', 'IOI FOUNDATION',
        'BERJAYA', 'MAH SING', 'ECM LIBRA', 'ELKEN', 'ALLIANCE',
        'BRITISH AMERICAN TOBACCO', 'EXXONMOBIL', 'JEFFREY CHEAH',
        'CCEP FOUNDATION', 'ALPRO FOUNDATION',
        'CIMB', 'AXIATA', 'IOI', 'BERJAYA CARES', 'ECM LIBRA',
        'ELKEN FOUNDATION', 'ALLIANCE FOUNDATION'
    ]
    if any(keyword in org_upper for keyword in corporate_keywords):
        if not any(edu in org_upper for edu in ['SCHOOL', 'SEKOLAH', 'EDUCATION', 'PELAJARAN', 'MEDICAL', 'HEALTH']):
            return 'Corporate Foundations'
    
    # Emergency/Disaster Relief
    emergency_keywords = [
        'BENCANA', 'DISASTER', 'RELIEF', 'EMERGENCY', 'FOOD AID',
        'HUMANITARIAN', 'MUHIBBAH FOOD BANK', 'FOOD BANK'
    ]
    if any(keyword in org_upper for keyword in emergency_keywords):
        return 'Emergency/Disaster Relief'
    
    # Default to Welfare/Social Services
    return 'Welfare/Social Services'
```

### 3. Apply Categorization

```python
# Add category column
df = df.with_columns([
    pl.struct(['organization', 'classification'])
    .map_elements(
        lambda x: categorize_organization(x['organization'], x['classification']),
        return_dtype=pl.Utf8
    )
    .alias('category')
])

# If classification column doesn't exist, use None
# df = df.with_columns([
#     pl.col('organization')
#     .map_elements(
#         lambda x: categorize_organization(x, None),
#         return_dtype=pl.Utf8
#     )
#     .alias('category')
# ])
```

### 4. Verify Results

```python
# Category distribution
print("Category distribution:")
print(df['category'].value_counts().sort('count', descending=True))

# Check for nulls
print(f"\nOrganizations without category: {df.filter(pl.col('category').is_null()).height}")

# Sample organizations by category
for category in df['category'].unique().sort().to_list():
    sample = df.filter(pl.col('category') == category)['organization'].unique().head(3).to_list()
    print(f"\n{category}:")
    for org in sample:
        print(f"  - {org}")
```

### 5. Save Results

```python
# Save to CSV (overwrite original or save to new file)
df.write_csv(csv_path)  # Overwrite original
# df.write_csv('output_with_categories.csv')  # Or save to new file
```

### 6. Reusable Function for Any Dataset

```python
def categorize_csv_file(csv_path, output_path=None):
    """
    Categorize organizations in a CSV file and add category column.
    
    Args:
        csv_path: Path to input CSV file
        output_path: Path to output CSV file (if None, overwrites input)
    
    Returns:
        DataFrame with category column added
    """
    import polars as pl
    
    # Read CSV
    df = pl.read_csv(csv_path)
    
    # Verify required columns
    if 'organization' not in df.columns:
        raise ValueError("CSV must contain 'organization' column")
    
    # Get classification column if it exists
    has_classification = 'classification' in df.columns
    
    # Add category column
    if has_classification:
        df = df.with_columns([
            pl.struct(['organization', 'classification'])
            .map_elements(
                lambda x: categorize_organization(x['organization'], x['classification']),
                return_dtype=pl.Utf8
            )
            .alias('category')
        ])
    else:
        df = df.with_columns([
            pl.col('organization')
            .map_elements(
                lambda x: categorize_organization(x, None),
                return_dtype=pl.Utf8
            )
            .alias('category')
        ])
    
    # Save results
    output = output_path if output_path else csv_path
    df.write_csv(output)
    
    # Print summary
    print(f"Processed {len(df)} rows")
    print(f"Categories: {df['category'].value_counts().sort('count', descending=True)}")
    
    return df

# Usage example:
# categorize_csv_file('public/generated/subsection_11D/subsection_11D.csv')
# categorize_csv_file('public/generated/subsection_PUA/subsection_pua.csv')
```

## Manual Review Process

After running the automatic categorization, organizations in the "Others" category need manual review. Use the following approach:

### 1. Review "Others" Category

```python
import polars as pl

df = pl.read_csv('your_file.csv')
others = df.filter(pl.col('category') == 'Others')

# Get unique organizations
unique_orgs = others['organization'].unique().sort().to_list()
print(f"Organizations to review: {len(unique_orgs)}")
```

### 2. Manual Categorization Patterns

When reviewing, look for:

- **Corporate Foundations**: Known company names (MAGNUM, CIMB, AXIATA, IOI, BERJAYA, MAYBANK, YTL, PETRONAS, etc.)
- **Educational**: Missed education keywords (TABUNG PENDIDIKAN, AMANAH PENDIDIKAN, etc.)
- **Healthcare/Medical**: Disease-specific terms (PARKINSON, THALASSAEMIA, etc.)
- **Religious**: Religious keywords in YAYASAN names (YAYASAN.*ISLAM, YAYASAN.*QURAN, etc.)
- **Other categories**: Apply same pattern matching logic as automatic categorization

### 3. Update Categories

```python
# Create manual mapping
manual_categories = {
    'YAYASAN MAGNUM': 'Corporate Foundations',
    'CIMB FOUNDATION': 'Corporate Foundations',
    # ... add more mappings
}

# Update dataframe
df = df.with_columns(
    pl.when(pl.col('organization').is_in(list(manual_categories.keys())))
    .then(pl.col('organization').map_elements(lambda x: manual_categories[x], return_dtype=pl.Utf8))
    .otherwise(pl.col('category'))
    .alias('category')
)

# Save
df.write_csv('your_file.csv')
```

### 4. Iterative Review

- Start with obvious cases (corporate foundations, clear category indicators)
- Review remaining "Others" for patterns that can be added to automatic categorization
- Some organizations may legitimately remain in "Others" if they don't fit any category

## Verification Checklist

- [ ] All rows have a category assigned (no nulls)
- [ ] Category distribution makes sense
- [ ] Sample organizations in each category look correct
- [ ] Religious organizations include both places of worship and religious welfare
- [ ] Corporate foundations are correctly identified
- [ ] Medical organizations for children are categorized as Healthcare/Medical, not Children/Youth
- [ ] "Others" category has been manually reviewed