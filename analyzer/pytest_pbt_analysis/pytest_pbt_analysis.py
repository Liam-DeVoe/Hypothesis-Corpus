def pytest_addoption(parser):
    group = parser.getgroup("pbt-analysis", "PBT analysis control options")
    group.addoption(
        "--experiment-nodeid",
        action="store",
        dest="experiment_nodeid",
        default=None,
        help="Filter tests to only run the specified nodeid",
    )


def pytest_collection_modifyitems(session, config, items):
    nodeid = config.getoption("experiment_nodeid")
    if nodeid is None:
        return

    n = len(items)
    for i, item in enumerate(reversed(items)):
        if item.nodeid != nodeid:
            del items[n - i - 1]

    assert len(items) == 1, items
