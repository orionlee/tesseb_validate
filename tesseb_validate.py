import os
import warnings

import pandas as pd

import tess_dv

TMP_DATA_DIR = "data"


def load_tesseb_catalog():
    # downloaded from:
    # https://archive.stsci.edu/hlsps/tess-ebs/hlsp_tess-ebs_tess_lcf-ffi_s0001-s0026_tess_v1.0_cat.csv
    # see: https://archive.stsci.edu/hlsp/tess-ebs
    df = pd.read_csv(
        f"{TMP_DATA_DIR}/hlsp_tess-ebs_tess_lcf-ffi_s0001-s0026_tess_v1.0_cat.csv"
    )
    return df


def _df_to_csv(df, out_path, mode="a"):
    if (
        (not mode.startswith("w"))
        and (os.path.exists(out_path))
        and (os.path.getsize(out_path) > 0)
    ):
        header = False
    else:
        header = True
    return df.to_csv(out_path, index=False, mode=mode, header=header)


def get_tce_meta(tic):
    def to_pandas(tce_info, prefix):
        tce_info = tce_info.copy()
        tce_info.update(tce_info.pop("planet", {}))
        df = pd.DataFrame([tce_info])
        # prefix the columns
        column_map = {col: f"{prefix}_{col}" for col in df.columns}
        df = df.rename(columns=column_map)

        return df

    tce_infos, dvr_xml_tab = tess_dv.get_tce_minimal_infos_of_tic(
        tic, also_return_dvr_xml_table=True
    )
    num_tces = len(tce_infos)
    tce_infos = tess_dv.filter_top_2_tces_for_eb(tce_infos)
    tess_dv.add_info_from_tce_xml(tce_infos, dvr_xml_tab, download_dir=TMP_DATA_DIR)

    # convert the result to a DataFrame
    df = pd.DataFrame([dict(tic_id=tic, num_tces=num_tces)])

    if len(tce_infos) > 0:
        df_tce1 = to_pandas(tce_infos[0], "TCE1")
        df_tce1.drop("TCE1_tic_id", axis=1, inplace=True)
        df = pd.concat([df, df_tce1], axis=1)

    if len(tce_infos) > 1:
        df_tce2 = to_pandas(tce_infos[0], "TCE2")
        df_tce2.drop("TCE2_tic_id", axis=1, inplace=True)
        df = pd.concat([df, df_tce2], axis=1)

    return df


def get_n_save_tce_meta_of_tics(tics=None, max_row=None):
    out_path = f"{TMP_DATA_DIR}/tic_tce_meta.csv"


    if tics is None:
        tics = load_tesseb_catalog()["tess_id"].to_numpy()

    if max_row is not None:
        tics = tics[:max_row]

    def get_tics_saved():
        saved_path = f"{TMP_DATA_DIR}/tic_tce_meta.csv"
        if os.path.exists(saved_path) and os.path.getsize(saved_path) > 0:
            return load_tce_meta_table()["tic_id"].to_numpy()
        else:
            return []


    # those that have been downloaded (and can be skipped)
    tics_saved = get_tics_saved()

    num_downloaded = 0
    for tic in tics:
        if tic not in tics_saved:
            print(f"TIC {tic}")  # to show progress
            with warnings.catch_warnings():
                # to filter UserWarning: get_tce_minimal_infos_of_tic(): Multiple DVS for s0011-s0011:TCE1. Discard pipeline_run 214.
                warnings.filterwarnings(
                    "ignore", category=UserWarning, message=".*Multiple DVS for.*"
                )
                df_tce_meta = get_tce_meta(tic)
            _df_to_csv(df_tce_meta, out_path, mode="a")
            num_downloaded += 1
        else:
            print(f"TIC {tic} : already done")
    return num_downloaded


def load_tce_meta_table():
    file_path = f"{TMP_DATA_DIR}/tic_tce_meta.csv"
    df = pd.read_csv(file_path)
    return df