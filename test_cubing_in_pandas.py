#!/usr/bin/env python
# -*- coding: utf-8 -*-

from io import StringIO

import pandas as pd
import numpy as np

from cubing_in_pandas import (cubinggroupby, _cols_normalize,
                              _check_no_interleaving_cols, _get_cube_combinations,
                              _get_rollup_combinations)


def test__cols_normalize():
    assert _cols_normalize(None) == []
    assert _cols_normalize('a') == ['a']
    assert _cols_normalize(['a', 'b']) == ['a', 'b']


def test__check_no_interleaving_cols():
    assert _check_no_interleaving_cols([], None, [])
    assert _check_no_interleaving_cols([1, 2], [3, 4])
    assert _check_no_interleaving_cols([1, 2], None, [5, 6])
    assert not _check_no_interleaving_cols([1, 2], [1, 4])
    assert not _check_no_interleaving_cols([1, 2], None, [2, 4])
    assert _check_no_interleaving_cols([1, 2], [3, 4], [5, 6])


def test__get_cube_combinations():
    assert _get_cube_combinations([]) == [tuple()]
    assert _get_cube_combinations([1]) == [tuple(), (1, )]
    assert _get_cube_combinations([1, 2, 3, 4]) == [
        tuple(), (1, ), (2, ), (3, ), (4, ), (1, 2), (1, 3), (1, 4), (2, 3),
        (2, 4), (3, 4), (1, 2, 3), (1, 2, 4), (1, 3, 4), (2, 3, 4),
        (1, 2, 3, 4)
    ]


def test__get_rollup_combinations():
    assert _get_rollup_combinations([]) == [tuple()]
    assert _get_rollup_combinations([1]) == [tuple(), (1, )]
    assert _get_rollup_combinations(
        [1, 2, 3, 4]) == [tuple(), (1, ), (1, 2), (1, 2, 3), (1, 2, 3, 4)]


def test_cubinggroupby():
    input_text = """category,area,year,product,price
cat1,area1,1,prod1,10
cat1,area1,2,prod1,11
cat1,area2,3,prod2,12
cat2,area1,1,prod1,13
cat2,area2,3,prod3,14
cat2,area1,2,prod2,15
,area1,4,prod3,16
,area2,2,prod1,17
cat2,,3,prod4,18
"""
    df = pd.read_csv(StringIO(input_text), sep=',')

    df_normal_groupby = df.groupby(['category', 'area', 'year'],
                                   as_index=False).agg({
                                       'product': pd.Series.nunique,
                                       'price': 'mean'
                                   })
    assert df_normal_groupby.shape == (6, 5)

    df_cubing_groupby = cubinggroupby(df,
                                      cube_cols=['category', 'area'],
                                      rollup_cols=['year'],
                                      agg={
                                          'product': pd.Series.nunique,
                                          'price': 'mean'
                                      },
                                      fill_grouping={
                                          'category': 'TOTAL',
                                          'area': np.nan
                                      },
                                      as_index=False)

    output_text = """category,area,year,product,price
TOTAL,area1,1.0,1,23
TOTAL,area1,2.0,2,26
TOTAL,area1,4.0,1,16
TOTAL,area1,,3,65
TOTAL,area2,2.0,1,17
TOTAL,area2,3.0,2,26
TOTAL,area2,,3,43
TOTAL,,1.0,1,23
TOTAL,,2.0,2,43
TOTAL,,3.0,3,44
TOTAL,,4.0,1,16
TOTAL,,,4,126
cat1,area1,1.0,1,10
cat1,area1,2.0,1,11
cat1,area1,,1,21
cat1,area2,3.0,1,12
cat1,area2,,1,12
cat1,,1.0,1,10
cat1,,2.0,1,11
cat1,,3.0,1,12
cat1,,,2,33
cat2,area1,1.0,1,13
cat2,area1,2.0,1,15
cat2,area1,,2,28
cat2,area2,3.0,1,14
cat2,area2,,1,14
cat2,,1.0,1,13
cat2,,2.0,1,15
cat2,,3.0,2,32
cat2,,,4,60
"""
    df_cubing_groupby_expected = pd.read_csv(StringIO(output_text), sep=',')

    # because the dataframes contain NaN, we cannot compare their elements easily
    assert df_cubing_groupby.shape == df_cubing_groupby_expected.shape
    # total should be equal to total
    assert df.agg({
        'product': pd.Series.nunique,
        'price': 'sum'
    }).to_frame().T.equals(
        df_cubing_groupby_expected.loc[(
            (df_cubing_groupby_expected['category'] == 'TOTAL')
            & pd.isnull(df_cubing_groupby_expected['area'])
            & pd.isnull(df_cubing_groupby_expected['year'])),
                                       ['product', 'price']].reset_index(
                                           drop=True))
