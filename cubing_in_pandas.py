#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A quick and dirty way to support CUBE and ROLLUP in Python pandas.

Released under MIT License.
"""

import pandas as pd
import numpy as np
from itertools import combinations


def _cols_normalize(cols=None):
    if cols is None:
        return []
    if isinstance(cols, str):
        return [cols]
    return cols


def _check_no_interleaving_cols(normal_cols=None,
                            cube_cols=None,
                            rollup_cols=None):
    normal_cols = set(_cols_normalize(normal_cols))
    cube_cols = set(_cols_normalize(cube_cols))
    rollup_cols = set(_cols_normalize(rollup_cols))

    all_cols = (normal_cols | cube_cols | rollup_cols)

    all_lengths = (len(normal_cols) + len(cube_cols) + len(rollup_cols))

    return len(all_cols) == all_lengths


def _get_cube_combinations(x):
    res = []
    for comb_len in range(len(x) + 1):
        for comb in combinations(x, comb_len):
            res.append(comb)
    return res


def _get_rollup_combinations(x):
    res = []
    for i in range(len(x) + 1):
        comb = tuple(x[:i])
        res.append(comb)
    return res


def _get_grouping_filling(col_name, filling=None):
    if filling is None:
        return None
    try:
        return filling.get(col_name, None)
    except:
        return filling


def cubinggroupby(df,
                  normal_cols=None,
                  cube_cols=None,
                  rollup_cols=None,
                  agg=None,
                  fill_grouping=None,
                  as_index=True):
    """
    Group by with CUBE and ROLLUP, then aggregate, in pandas.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame.
    normal_cols: list
        The columns be used in simple good old group by.
    cube_cols: list
        The columns that are going to be grouped by with CUBE.
    rollup_cols: list
        The columns that are going to be grouped by with ROLLUP.
    agg: Union[str, dict]
        The aggregation functions.
        It can be a str like 'mean', 'min', 'max', 'sum',
        and it can also be a dict from column name to aggregation functions.
        Refer func parameter in pandas.core.groupby.DataFrameGroupBy.agg.
    fill_grouping: dict
        A dict from column name to value.
        This parameter specifies the special values to fill in folded dimensions.
        For example, you may want to assign the value 'TOTAL' to the column,
        then you can set this parameter as {col_name: 'TOTAL'}.
    as_index: bool, True by default
        Whether the dimensions be the index of the result DataFrame.


    Returns
    -------
    pd.DataFrame
        A DataFrame object after group by and aggregate

    See Also
    --------
    pandas.core.groupby.DataFrameGroupBy.agg
    """
    normal_cols = _cols_normalize(normal_cols)
    cube_cols = _cols_normalize(cube_cols)
    rollup_cols = _cols_normalize(rollup_cols)

    # three different group by columns
    # should not be interleaved with each other
    if not _check_no_interleaving_cols(normal_cols, cube_cols, rollup_cols):
        raise ValueError('the columns to be grouped by should be different')

    all_groupby_cols = normal_cols + cube_cols + rollup_cols

    # we only limit a few supported agg
    if isinstance(agg, str):
        value_cols = list(set(df.columns) - set(all_groupby_cols))
        grouping_func = agg
        agg = {x: grouping_func
               for x in value_cols}  # agg param becomes a dict
    elif isinstance(agg, dict):
        value_cols = list(agg.keys())
    else:
        raise ValueError('the agg parameter should be only str or dict')

    # the columns that would be in the final result
    result_cols = all_groupby_cols + value_cols  # for sortting the column index later

    cube_combs = _get_cube_combinations(cube_cols)
    rollup_combs = _get_rollup_combinations(rollup_cols)

    comb_result_dfs = []

    for cube_single_comb in cube_combs:
        for rollup_single_comb in rollup_combs:
            groupby_cols = normal_cols + list(cube_single_comb) + list(
                rollup_single_comb)
            remaining_cols = list(set(all_groupby_cols) - set(groupby_cols))

            #print(groupby_cols)
            #print(agg)
            if groupby_cols == []:
                # special treatment if all dimensions are folded
                single_result = df.agg(agg).to_frame().T
            else:
                single_result = df.groupby(groupby_cols,
                                           as_index=False).agg(agg)

            # for all groupby cols,
            # if they are not in this grouping set,
            # we should add the columns filled with NULL
            for single_remaining_col in remaining_cols:
                dim_value_to_fill = _get_grouping_filling(
                    single_remaining_col, fill_grouping)
                #print(dim_value_to_fill)
                single_result[single_remaining_col] = dim_value_to_fill

                original_dtype = df[single_remaining_col].dtype

                if pd.api.types.is_integer_dtype(
                        original_dtype) and dim_value_to_fill is None:
                    # for integer and None, we need special treatment
                    single_result[single_remaining_col] = pd.to_numeric(
                        single_result[single_remaining_col])
                else:
                    # directly use astype if not integer and not None
                    single_result[single_remaining_col] = single_result[
                        single_remaining_col].astype(
                            df[single_remaining_col].dtype)

            single_result = single_result[result_cols]

            comb_result_dfs.append(single_result)

    # "UNION ALL"
    unioned_result = pd.concat(
        comb_result_dfs,
        ignore_index=True).sort_values(all_groupby_cols).reset_index(drop=True)

    # make it more like the regular pandas groupby
    if as_index:
        unioned_result = unioned_result.set_index(
            all_groupby_cols).sort_index()

    return unioned_result
