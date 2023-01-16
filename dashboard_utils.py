import re
import urllib
import pandas as pd


def style(df_catalog):
    def make_clickable(val, url_template, target, quote_val=False, link_text_func=None):
        if pd.isna(val):
            return val
        val_in_url = str(val)
        if quote_val:
            val_in_url = urllib.parse.quote_plus(val)
        if "%s" not in url_template:
            url = f"{url_template}{val_in_url}"
        else:
            url = url_template.replace("%s", val_in_url)
        link_text = val
        if link_text_func is not None:
            link_text = link_text_func(val)
        return f'<a target="{target}" href="{url}">{link_text}</a>'

    def make_tic_id_clickable(val):
        return make_clickable(
            val, "https://exofop.ipac.caltech.edu/tess/target.php?id=", "_exofop"
        )

    def make_tesseb_link(val):
        # TESSEB_URL column is populated with tic id so as to construct the links
        # as follows
        # use the search URL as the search result contains a flag column (which seems to suggest ambiguous result)
        # that is not in the detailed report (or the dump in csv / Vizier)
        return make_clickable(
            val,
            "http://tessebs.villanova.edu/search_results?tic=",
            "_tesseb",
            link_text_func=lambda val: "details",
        )

    def make_exomast_link(val):
        return make_clickable(
            val,
            "https://exo.mast.stsci.edu/exomast_planet.html?planet=",
            "_exomast",
            # shorten the link text to save space
            link_text_func=lambda val: re.sub("^TIC\d+", "", val),
        )

    def make_dv_file_link(val):
        match = re.search("_(dv.)[.]pdf$", val)
        if match:
            file_type = match[1]
        else:
            file_type = "report"  # a fallback that should not happen

        return make_clickable(
            val,
            "https://exo.mast.stsci.edu/api/v0.1/Download/file?uri=",
            f"_{file_type}",
            link_text_func=lambda val: file_type,
        )

    format_spec = {
        "tess_id": make_tic_id_clickable,
        "tic_id": make_tic_id_clickable,
        "TESSEB_URL": make_tesseb_link,
        "TCE1_tce_id": make_exomast_link,
        "TCE1_dvs_dataURI": make_dv_file_link,
        "TCE1_dvm_dataURI": make_dv_file_link,
        "TCE1_dvr_dataURI": make_dv_file_link,
        "TCE2_tce_id": make_exomast_link,
        "TCE2_dvs_dataURI": make_dv_file_link,
        "TCE2_dvm_dataURI": make_dv_file_link,
        "TCE2_dvr_dataURI": make_dv_file_link,
    }

    return df_catalog.style.format(format_spec).hide(axis="index")
