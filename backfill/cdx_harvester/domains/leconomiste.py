from cdx_harvester.cdx_common import DomainConfig

CONFIG = DomainConfig(
    name="leconomiste",
    cdx_url_pattern="leconomiste.com/*",
    date_from="20210101",
    include_regex=r"/article/",
    exclude_regex=r"/(tag|category|page|search|feed)(/|$|\?)|\?page=",
)
