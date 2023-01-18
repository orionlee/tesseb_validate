import pandas as pd

import tesseb_tce_meta

#
# Fetch the necessary TCEs for the correspond TICs in TESSEB catalog


def create_tesseb_validate_catalog():
    out_path = "./tesseb_validate_catalog.csv"

    # basic join the two tables
    df_tesseb = tesseb_tce_meta.load_tesseb_catalog()
    df_tce = tesseb_tce_meta.load_tce_meta_table()

    df = pd.merge(
        df_tesseb,
        df_tce,
        how="left",
        left_on="tess_id",
        right_on="tic_id",
        validate="many_to_one",
    )
    # drop  tic_id, retain the tess_id column from TESS EB catalog
    df.drop("tic_id", axis=1, inplace=True)

    # additional logic done at load time to keep things flexible

    df.to_csv(out_path, index=False)
    return df


def load_tesseb_validate_catalog():
    csv_path = "./tesseb_validate_catalog.csv"

    #  columns that are inherently nullable ( a TIC may not have TCE(s))
    nullable_col_suffixes = [
        "obsID",
        "sector_range_start",
        "sector_range_stop",
        "sector_range_span",
        "tce_num",
        "pipeline_run",
        "planetNumber",
    ]
    nullable_cols = [f"TCE1_{c}" for c in nullable_col_suffixes]
    nullable_cols += [f"TCE2_{c}" for c in nullable_col_suffixes]

    # make the following nullable,
    # so that when the TCE meta is joined with TESSEB and there is no match for a given TIC,
    # the column does not become float
    nullable_cols += ["num_tces", "tic_Id"]

    nullable_cols_dtype = {c: "Int64" for c in nullable_cols}
    df = pd.read_csv(
        csv_path,
        dtype=nullable_cols_dtype,
    )

    # a column for display purpose (to show TESS EB URL)
    df["TESSEB_URL"] = df["tess_id"]

    _add_matching_meta(df)
    return df


# TODO: Consider to relax match criteria to increase the yield of matching TCEs
# - accept those with period diff (in days) is very small
#    - the +/- 1% for epoch phase is too stringent for short period (EW type),
#    - e.g., for a EW type with 0.3 day period, 1% would be 0.003 day, or 4.3 minutes
# - if the difference in epoch phase is such that the TCE epoch still falls
#    within an eclipse(maybe 50% of the duration), accept it as well, as the TCE epoch
#    would still catch the change of brightness
#    - basically, instead of using +/- 1%, we make the cutoff dependent of the
#      width of the eclipse
#    - for the width to be used, use the smaller of the (polynomial, 2-gaussian) values
#      to be conservative.
#


def _is_period_ratio_match(ratio):
    if pd.isna(ratio):
        return "-"
    # threshold is +/- 1%, and scaled to half period and double period
    if (0.99 < ratio < 1.01) or (0.495 < ratio < 0.505) or (1.98 < ratio < 2.02):
        return "Y"
    else:
        return "N"


def _is_epoch_diff_phase_match(diff_phase):
    # The diff_phase is normalized to [0, 1)
    if pd.isna(diff_phase):
        return "-"
    # threshold is +/- 1%, and scaled to case half period (double period has no effect on epoch)
    if (diff_phase < 0.01) or (0.99 < diff_phase < 1) or (0.495 < diff_phase < 0.505):
        return "Y"
    else:
        return "N"


def _add_matching_meta(df):
    # Given the TESSEB + TCE table, produce additional columns to facilitate matching
    for tce in ["TCE1", "TCE2"]:
        col_period_ratio = df[f"{tce}_orbitalPeriodDays"] / df["period"]
        df[f"Match_{tce}_period_ratio"] = [
            _is_period_ratio_match(r) for r in col_period_ratio
        ]
        df[f"Diff_{tce}_period_ratio"] = col_period_ratio

        col_epoch_diff_phase = (
            (df["bjd0"] - df[f"{tce}_transitEpochBtjd"]) / df["period"]
        ) % 1
        df[f"Match_{tce}_epoch_phase"] = [
            _is_epoch_diff_phase_match(p) for p in col_epoch_diff_phase
        ]
        df[f"Diff_{tce}_epoch_phase"] = col_epoch_diff_phase
        df[f"Diff_{tce}_epoch_days"] = col_epoch_diff_phase * df["period"]


if __name__ == "__main__":
    print("Create TESS EB validation catalog (combining TESS EB and matching TCES)...")
    create_tesseb_validate_catalog()
