import json
import os

import pytest

from apps.cache.cache import PublicationProductTreeCache, ConfigCache
from apps.elastic.modules.publication import get_cds_publication_count_by_mission


@pytest.fixture
def from_dt():
    return "2023-07-10T00:00:00"


@pytest.fixture
def to_dt():
    return "2023-07-25T00:00:00"

def read_config_file(path, filename):
    filepath = os.path.join(path, filename)
    with open(filepath, 'r') as f:
        array = json.load(f)
    return array

@pytest.fixture
def test_config_path():
    return "config"

@pytest.fixture
def config_file():
    return 'config-test.json'

@pytest.fixture
def product_tree_config_file():
    return 'publicationProductTree.json'

@pytest.fixture
def product_config(test_config_path, config_file):
    config = read_config_file(test_config_path,
                              config_file)['config']
    for key in config.keys():
        ConfigCache.store_object(key, config[key])

@pytest.fixture
def product_tree_config(test_config_path, product_tree_config_file):
    config = read_config_file(test_config_path,
                              product_tree_config_file)
    for key in config.keys():
        PublicationProductTreeCache.store_object(key, config[key])

    # config = read_config_file(ConfigCache.load_object('fileMissionTimeliness'))
    # for key in config.keys():
    #     MissionTimelinessCache.store_object(key, config[key])


def test_pub_stats_July(product_config, product_tree_config, from_dt, to_dt):
    mission = "S1"
    stats_data = get_cds_publication_count_by_mission(from_dt, to_dt, mission)
    # Extract count, sum for all product types
    total = sum([int(rec['count']) for rec in stats_data])
    # verify that count is under threahold value
    assert total < 30000
    # verify list of product types
    # Glist of product types for publication for S1
    productLevel_list = PublicationProductTreeCache.load_object(mission).get('levels')
    config_prodtypes = [typ for prodtypelist in productLevel_list.values() for typ in prodtypelist]
    prodtypes = [rec['productType'] for rec in stats_data]
    assert list(sorted(prodtypes)) == config_prodtypes

def test_pub_trend_month_July(product_config):
    mission = "S1"
    from_date = "2023-07-01T00:00:00"
    to_date = "2023-07-31T00:00:00"
    trend_data = get_cds_publication_count_by_mission(mission, from_date, to_date)
    # extract period for mission
    # check that period total for each subperiod does not differ
    # from Periods mean by 10%


