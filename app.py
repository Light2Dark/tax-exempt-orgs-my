import marimo

__generated_with = "0.19.7"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl
    from categorize_organizations import CATEGORIES

    cols_to_drop = [
        "reference_num",
        "start_date",
        "end_date",
        "remarks",
        "status",
        "classification",
    ]


@app.cell(hide_code=True)
def _(inputs):
    mo.md(rf"""
    # Tax-exempted Organizations in Malaysia

    Looking to make a tax-deductible donation in Malaysia? This directory helps you find approved organizations where your donations qualify for tax deductions. All organizations listed here are taken from Inland Revenue Board of Malaysia (LHDN) [website](https://www.hasil.gov.my/en/quick-links/services/donation-approval/).

    {inputs}

    /// admonition | 💡 Quick tip

    You can claim tax deductions of up to 10% of your total income when you donate to approved organizations.
    ///
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Charitable & Non-Profit Organizations 🫂

    **What you'll find here**: Charities, educational foundations, healthcare organizations, community service groups, and other non-profit institutions that have been operating for at least two years.

    **Tax benefit**: Donations to these organizations are tax-deductible (up to 10% of your total income when combined with other eligible donations).
    """)
    return


@app.cell
def _(filter_dataset):
    subsection_446 = pl.read_csv(
        f"{mo.notebook_location()}/public/generated/subsection_44_6/subsection_44_6.csv"
    ).pipe(filter_dataset)
    mo.ui.table(
        subsection_446,
        page_size=10,
        selection=None,
        wrapped_columns=["organization"],
    )
    return (subsection_446,)


@app.cell
def _():
    mo.md(r"""
    ## Religious Authorities & Public Universities 🍃

    Donations here can be made as wakaf (Islamic endowments) or general endowments.

    **What you'll find here**:
    - Religious authorities and bodies approved for wakaf donations
    - Public universities that accept endowment contributions

    **Tax benefit**: Donations to these institutions are tax-deductible (up to 10% of your total income when combined with other eligible donations). For endowments, the principal amount is typically preserved, with only the returns being used.
    """)
    return


@app.cell
def _(filter_dataset):
    subsection_11D = pl.read_csv(
        f"{mo.notebook_location()}/public/generated/subsection_11D/subsection_11D.csv"
    ).pipe(filter_dataset)
    mo.ui.table(
        subsection_11D,
        page_size=10,
        selection=None,
        wrapped_columns=["organization"],
    )
    return (subsection_11D,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Tax-Exempt Religious Institutions 🛐

    Religious institutions and organizations that are registered as companies limited by guarantee. These institutions receive full tax exemption on their income.

    **Note**: While these institutions are tax-exempt, donations to them may not automatically qualify for donor tax deductions unless they're also approved under other provisions. Check with the organization or your tax advisor for details.
    """)
    return


@app.cell
def _(filter_dataset):
    subsection_pua = pl.read_csv(
        f"{mo.notebook_location()}/public/generated/subsection_PUA/subsection_pua.csv"
    ).pipe(filter_dataset)
    mo.ui.table(subsection_pua, selection=None, wrapped_columns=["organization"])
    return (subsection_pua,)


@app.cell
def _(dropdown, search_input):
    def filter_dataset(df: pl.DataFrame) -> pl.DataFrame:
        df = df.filter(pl.col("status") == "approved").drop(cols_to_drop)
        df = df.with_columns(pl.col("*").str.to_titlecase())
        search = search_input.value.lower()
        conditions = [
            pl.col(col).str.to_lowercase().str.contains(search)
            for col in df.columns
        ]
        df = df.filter(pl.any_horizontal(conditions))

        if dropdown.value != len(CATEGORIES):
            return df.filter(pl.col("category").is_in(dropdown.value))

        return df
    return (filter_dataset,)


@app.cell
def _():
    search_input = mo.ui.text(label="Search", debounce=False)
    dropdown = mo.ui.multiselect(
        options=CATEGORIES, value=CATEGORIES, label="Categories"
    )
    dropdown_wrap = dropdown.style({"margin-bottom": "8px"})
    return dropdown, dropdown_wrap, search_input


@app.cell
def _(
    dropdown_wrap,
    search_input,
    subsection_11D,
    subsection_446,
    subsection_pua,
):
    _total = len(subsection_446) + len(subsection_11D) + len(subsection_pua)
    num_orgs = mo.Html(
        f"<span style='color: green; font-weight: bold;'>{_total}</span>"
    )
    inputs = mo.vstack([mo.md(f"{search_input} {num_orgs}"), dropdown_wrap])
    return (inputs,)


if __name__ == "__main__":
    app.run()
